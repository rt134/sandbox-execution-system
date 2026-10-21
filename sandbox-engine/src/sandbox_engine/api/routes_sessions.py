from __future__ import annotations

import asyncio
import os
import time
import uuid
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

from ..config import settings
from ..core.http_client import request_with_byte_cap, stream_response_bytes
from ..core.lease_manager import LeaseManager
from ..core.metrics import ACTIVE_SESSIONS, WORKSPACE_LOCKS_ACTIVE
from ..core.security import (
    generate_daemon_token,
    validate_session_id,
    validate_workspace_id,
    verify_api_key,
)
from ..drivers.base import ContainerDriver
from ..models import (
    HeartbeatRequest,
    HeartbeatResponse,
    SandboxSpec,
    SessionAcquireRequest,
    SessionAcquireResponse,
)

router = APIRouter(
    prefix="/v1/sessions",
    tags=["Stateful Sessions"],
    dependencies=[Depends(verify_api_key)],
)


def get_driver(request: Request) -> ContainerDriver:
    return request.app.state.driver


def get_lease_manager(request: Request) -> LeaseManager:
    return request.app.state.lease_manager


@router.post("/acquire", response_model=SessionAcquireResponse)
async def acquire_session(
    payload: SessionAcquireRequest,
    request: Request,
    driver: ContainerDriver = Depends(get_driver),
    lease_mgr: LeaseManager = Depends(get_lease_manager),
) -> SessionAcquireResponse:
    """
    Acquire or resume a stateful agent session with a mounted workspace volume.
    Enforces strict path validation against directory traversal and atomic concurrency locking.
    """
    if payload.session_id:
        validate_session_id(payload.session_id)
    session_id = payload.session_id or f"sess_{uuid.uuid4().hex[:12]}"
    workspace_id = payload.workspace_id
    from ..core.ttl import clamp_new_session_ttl

    ttl = clamp_new_session_ttl(payload.ttl_seconds)

    tenant_id = getattr(request.state, "tenant_id", "default")

    if not await lease_mgr.tenant_has_workspace(tenant_id, workspace_id):
        ws_count = await lease_mgr.count_tenant_workspaces(tenant_id)
        if ws_count >= settings.max_workspaces_per_tenant:
            raise HTTPException(
                status_code=429,
                detail=f"Tenant workspace quota exceeded ({settings.max_workspaces_per_tenant} distinct workspaces max)",
            )

    # Multi-Tenant Ownership Guard: Verify that workspace_id is owned by caller's tenant
    is_owner, current_owner = await lease_mgr.claim_workspace_ownership(workspace_id, tenant_id=tenant_id)
    if not is_owner:
        raise HTTPException(
            status_code=403,
            detail=f"Access denied: workspace '{workspace_id}' is owned by another tenant",
        )

    # Tenant Quota Guard: Check concurrent active session limit per tenant atomically
    existing = await lease_mgr.get(session_id)
    reserved_provisional = False
    if not (existing and existing.status == "active"):
        reserved = await lease_mgr.reserve_session_slot(
            tenant_id=tenant_id,
            max_concurrent=settings.max_concurrent_sessions_per_tenant,
            session_id=session_id,
            workspace_id=workspace_id,
        )
        if not reserved:
            raise HTTPException(
                status_code=429,
                detail=f"Tenant concurrent session quota exceeded ({settings.max_concurrent_sessions_per_tenant} active sessions max)",
            )
        reserved_provisional = True

    is_k8s = settings.driver.lower() == "k8s"

    # Security: Strict validation against directory traversal strictly scoped to tenant directory
    # Skip host directory creation on K8s to comply with readOnlyRootFilesystem
    workspace_dir = validate_workspace_id(
        workspace_id, settings.workspaces_root, tenant_id=tenant_id, create_host_dir=not is_k8s
    )

    global_active = len(await lease_mgr.list_active())
    if global_active >= settings.max_global_sessions:
        if reserved_provisional:
            await lease_mgr.release(session_id)
        raise HTTPException(
            status_code=503,
            detail="Global session capacity reached",
            headers={"Retry-After": "30"},
        )

    lock_held = False
    lease_committed = False
    spawned_sandbox: str | None = None
    try:
        acquired, locked_by, expires_at = await lease_mgr.acquire_workspace_lock(
            workspace_id=workspace_id,
            session_id=session_id,
            ttl_seconds=ttl,
            tenant_id=tenant_id,
        )
        if not acquired:
            if reserved_provisional:
                await lease_mgr.release(session_id)
            remaining = max(0.0, expires_at - time.time()) if expires_at else 0.0
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Workspace is currently locked by another active session",
                    "workspace_id": workspace_id,
                    "locked_by": locked_by,
                    "expires_at": expires_at,
                    "ttl_remaining_sec": round(remaining, 1),
                },
            )
        lock_held = True

        # Check for existing active lease to reattach
        if existing and existing.status == "active":
            if existing.tenant_id and existing.tenant_id != tenant_id:
                raise HTTPException(status_code=403, detail="Access denied: session belongs to another tenant")
            # Check if container is still responsive
            if await driver.healthcheck(existing.endpoint, auth_token=existing.auth_token):
                await lease_mgr.heartbeat(session_id, ttl)
                await lease_mgr.refresh_workspace_lock(workspace_id, session_id, ttl, tenant_id=tenant_id)
                lease_committed = True
                return SessionAcquireResponse(
                    session_id=existing.session_id,
                    workspace_id=existing.workspace_id,
                    sandbox_id=existing.sandbox_id,
                    endpoint=existing.endpoint,
                    expires_at=existing.expires_at,
                    status="active",
                )

        if not is_k8s:
            os.makedirs(workspace_dir, exist_ok=True)
        auth_token = generate_daemon_token()

        spec = SandboxSpec(
            spec_id=session_id,
            sandbox_type="stateful",
            image=settings.image,
            workspace_path=workspace_dir,
            mem_limit=settings.default_mem_limit,
            cpu_limit=settings.default_cpu_limit,
            labels={
                "sandbox.session_id": session_id,
                "sandbox.workspace_id": workspace_id,
                "sandbox.tenant_id": tenant_id,
                **({"sandbox.workspace_host_path": workspace_dir} if not is_k8s else {}),
            },
            pids_limit=settings.pids_limit,
            disk_quota_mb=settings.default_workspace_disk_quota_mb,
            auth_token=auth_token,
        )

        instance = await driver.create_sandbox(spec)
        spawned_sandbox = instance.sandbox_id
        ready = await driver.healthcheck(instance.endpoint, auth_token=auth_token)
        if not ready:
            raise HTTPException(status_code=502, detail="Stateful sandbox failed healthcheck")

        lease = await lease_mgr.create_or_renew(
            session_id=session_id,
            workspace_id=workspace_id,
            sandbox_id=instance.sandbox_id,
            endpoint=instance.endpoint,
            ttl_seconds=ttl,
            auth_token=auth_token,
            tenant_id=tenant_id,
        )

        # Update metrics
        active_leases = await lease_mgr.list_active()
        ACTIVE_SESSIONS.set(len(active_leases))
        active_locks = await lease_mgr.count_active_locks()
        WORKSPACE_LOCKS_ACTIVE.set(active_locks)

        spawned_sandbox = None
        lease_committed = True
        return SessionAcquireResponse(
            session_id=lease.session_id,
            workspace_id=lease.workspace_id,
            sandbox_id=lease.sandbox_id,
            endpoint=lease.endpoint,
            expires_at=lease.expires_at,
            status="active",
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to acquire session") from e
    finally:
        if not lease_committed:
            if lock_held:
                await lease_mgr.release_workspace_lock(workspace_id, session_id, tenant_id=tenant_id)
            if reserved_provisional:
                await lease_mgr.release(session_id)
            if spawned_sandbox:
                remove_ws = settings.driver == "docker"
                await driver.destroy_sandbox(spawned_sandbox, remove_workspace=remove_ws)


@router.post("/{session_id}/heartbeat", response_model=HeartbeatResponse)
async def session_heartbeat(
    session_id: str,
    request: Request,
    payload: HeartbeatRequest = None,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
) -> HeartbeatResponse:
    """Send sliding heartbeat to keep the session alive and refresh workspace lock."""
    tenant_id = getattr(request.state, "tenant_id", "default")
    existing = await lease_mgr.get(session_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    if existing.tenant_id and existing.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied: session belongs to another tenant")

    ttl = payload.ttl_seconds if payload else None
    lease = await lease_mgr.heartbeat(session_id, ttl)
    if not lease:
        raise HTTPException(status_code=404, detail="Session not found or expired")

    if lease.workspace_id:
        await lease_mgr.refresh_workspace_lock(lease.workspace_id, session_id, ttl, tenant_id=tenant_id)

    return HeartbeatResponse(
        session_id=lease.session_id,
        expires_at=lease.expires_at,
        status=lease.status,
    )


@router.post("/{session_id}/release")
async def release_session(
    session_id: str,
    request: Request,
    driver: ContainerDriver = Depends(get_driver),
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    """Explicitly release and terminate a session while releasing the workspace concurrency lock."""
    tenant_id = getattr(request.state, "tenant_id", "default")
    lease = await lease_mgr.get(session_id)
    if not lease:
        raise HTTPException(status_code=404, detail="Session not found")
    if lease.tenant_id and lease.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied: session belongs to another tenant")

    await lease_mgr.release(session_id)
    if lease.workspace_id:
        await lease_mgr.release_workspace_lock(lease.workspace_id, session_id, tenant_id=tenant_id)

    if lease.sandbox_id:
        await driver.destroy_sandbox(lease.sandbox_id)

    # Update metrics
    active_leases = await lease_mgr.list_active()
    ACTIVE_SESSIONS.set(len(active_leases))
    active_locks = await lease_mgr.count_active_locks()
    WORKSPACE_LOCKS_ACTIVE.set(active_locks)

    return {"status": "released", "session_id": session_id}


# =========================================================================
# Proxied Session APIs (Auto-Heartbeat & Keepalive on each interaction)
# =========================================================================

async def _get_active_session(session_id: str, lease_mgr: LeaseManager, tenant_id: str = "default") -> tuple[str, Optional[str]]:
    validate_session_id(session_id)
    lease = await lease_mgr.get(session_id)
    if not lease or lease.status != "active":
        raise HTTPException(status_code=404, detail="Session not found or expired")
    if lease.tenant_id and lease.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied: session belongs to another tenant")
    await lease_mgr.heartbeat(session_id)  # Auto-extend lease
    if lease.workspace_id:
        await lease_mgr.refresh_workspace_lock(lease.workspace_id, session_id)
    return lease.endpoint, lease.auth_token


@router.post("/{session_id}/exec")
async def proxy_exec(
    session_id: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    validate_session_id(session_id)
    tenant_id = getattr(request.state, "tenant_id", "default")
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr, tenant_id=tenant_id)
    body = await request.json()
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}

    # Audit log the command invocation
    raw_cmd = body.get("command") or body.get("cmd") or body.get("script") or ""
    cmd_summary = str(raw_cmd)[:500]
    await lease_mgr.log_audit_event(
        session_id=session_id,
        event_type="command_exec",
        details={
            "command": cmd_summary,
            "stream": body.get("stream", False),
            "tenant_id": tenant_id,
        },
    )

    if body.get("stream", False):
        client = httpx.AsyncClient(timeout=None)
        stop_keepalive = asyncio.Event()

        # Background keepalive task to keep long-running streaming commands alive
        async def keepalive_loop():
            while not stop_keepalive.is_set():
                try:
                    await asyncio.wait_for(stop_keepalive.wait(), timeout=15.0)
                    break
                except asyncio.TimeoutError:
                    await lease_mgr.heartbeat(session_id)

        keepalive_task = asyncio.create_task(keepalive_loop())

        async def stream_generator():
            total_bytes = 0
            try:
                async with client.stream("POST", f"{endpoint}/exec", json=body, headers=headers) as r:
                    async for chunk in r.aiter_bytes():
                        total_bytes += len(chunk)
                        if total_bytes > settings.max_stream_response_bytes:
                            break
                        yield chunk
            finally:
                stop_keepalive.set()
                keepalive_task.cancel()
                await client.aclose()

        return StreamingResponse(stream_generator(), media_type="application/x-ndjson")

    try:
        timeout_val = float(body.get("timeout", 300.0))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid exec timeout value")
    timeout_val = min(max(timeout_val, 0.1), 1800.0)
    stop_keepalive = asyncio.Event()

    async def buffered_keepalive_loop():
        while not stop_keepalive.is_set():
            try:
                await asyncio.wait_for(stop_keepalive.wait(), timeout=15.0)
                break
            except asyncio.TimeoutError:
                await lease_mgr.heartbeat(session_id)

    keepalive_task = asyncio.create_task(buffered_keepalive_loop())
    try:
        content, status_code, resp_headers = await request_with_byte_cap(
            "POST",
            f"{endpoint}/exec",
            json=body,
            headers=headers,
            timeout_seconds=timeout_val + 10.0,
        )
        return Response(
            content=content,
            status_code=status_code,
            media_type=resp_headers.get("content-type", "application/json"),
        )
    finally:
        stop_keepalive.set()
        keepalive_task.cancel()


@router.post("/{session_id}/fs/write")
async def proxy_fs_write(
    session_id: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    validate_session_id(session_id)
    tenant_id = getattr(request.state, "tenant_id", "default")
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr, tenant_id=tenant_id)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    body = await request.json()

    # Audit log file write
    await lease_mgr.log_audit_event(
        session_id=session_id,
        event_type="file_write",
        details={"path": body.get("path", ""), "tenant_id": tenant_id},
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{endpoint}/fs/write", json=body, headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.get("/{session_id}/fs/read")
async def proxy_fs_read(
    session_id: str,
    path: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    validate_session_id(session_id)
    tenant_id = getattr(request.state, "tenant_id", "default")
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr, tenant_id=tenant_id)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    async def file_stream():
        async for chunk in stream_response_bytes(
            "GET",
            f"{endpoint}/fs/read",
            params={"path": path},
            headers=headers,
            max_bytes=settings.max_response_bytes,
        ):
            yield chunk

    return StreamingResponse(file_stream(), media_type="application/octet-stream")


@router.get("/{session_id}/fs/list")
async def proxy_fs_list(
    session_id: str,
    request: Request,
    path: str = ".",
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    tenant_id = getattr(request.state, "tenant_id", "default")
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr, tenant_id=tenant_id)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{endpoint}/fs/list", params={"path": path}, headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.post("/{session_id}/fs/delete")
async def proxy_fs_delete(
    session_id: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    tenant_id = getattr(request.state, "tenant_id", "default")
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr, tenant_id=tenant_id)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    body = await request.json()

    # Audit log file delete
    await lease_mgr.log_audit_event(
        session_id=session_id,
        event_type="file_delete",
        details={"path": body.get("path", ""), "tenant_id": tenant_id},
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{endpoint}/fs/delete", json=body, headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.get("/{session_id}/fs/quota")
async def proxy_fs_quota(
    session_id: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    """Retrieve workspace disk quota metrics (used, quota, usage percentage)."""
    tenant_id = getattr(request.state, "tenant_id", "default")
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr, tenant_id=tenant_id)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{endpoint}/fs/quota", headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))
