from __future__ import annotations

import asyncio
import os
import time
import uuid
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

from ..config import settings
from ..core.lease_manager import LeaseManager
from ..core.metrics import ACTIVE_SESSIONS, WORKSPACE_LOCKS_ACTIVE
from ..core.security import generate_daemon_token, validate_workspace_id, verify_api_key
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
    driver: ContainerDriver = Depends(get_driver),
    lease_mgr: LeaseManager = Depends(get_lease_manager),
) -> SessionAcquireResponse:
    """
    Acquire or resume a stateful agent session with a mounted workspace volume.
    Enforces strict path validation against directory traversal and atomic concurrency locking.
    """
    session_id = payload.session_id or f"sess_{uuid.uuid4().hex[:12]}"
    workspace_id = payload.workspace_id
    ttl = payload.ttl_seconds or settings.default_session_ttl

    # Security: Strict validation against directory traversal (e.g. '../..', '/')
    workspace_dir = validate_workspace_id(workspace_id, settings.workspaces_root)

    # Concurrency Lock: Ensure workspace is not held by another active session
    acquired, locked_by, expires_at = await lease_mgr.acquire_workspace_lock(
        workspace_id=workspace_id,
        session_id=session_id,
        ttl_seconds=ttl,
    )
    if not acquired:
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

    try:
        # Check for existing active lease to reattach
        existing = await lease_mgr.get(session_id)
        if existing and existing.status == "active":
            # Check if container is still responsive
            if await driver.healthcheck(existing.endpoint, auth_token=existing.auth_token):
                await lease_mgr.heartbeat(session_id, ttl)
                await lease_mgr.refresh_workspace_lock(workspace_id, session_id, ttl)
                return SessionAcquireResponse(
                    session_id=existing.session_id,
                    workspace_id=existing.workspace_id,
                    sandbox_id=existing.sandbox_id,
                    endpoint=existing.endpoint,
                    expires_at=existing.expires_at,
                    status="active",
                )

        os.makedirs(workspace_dir, exist_ok=True)
        auth_token = generate_daemon_token()

        # Spawn new stateful sandbox container with cgroup pids & disk quota enforcement
        spec = SandboxSpec(
            spec_id=session_id,
            sandbox_type="stateful",
            image=settings.image,
            workspace_path=workspace_dir,
            labels={"sandbox.session_id": session_id, "sandbox.workspace_id": workspace_id},
            pids_limit=getattr(payload, "pids_limit", None) or settings.pids_limit,
            disk_quota_mb=getattr(payload, "disk_quota_mb", None) or settings.default_workspace_disk_quota_mb,
            auth_token=auth_token,
        )

        instance = await driver.create_sandbox(spec)
        ready = await driver.healthcheck(instance.endpoint, auth_token=auth_token)
        if not ready:
            await driver.destroy_sandbox(instance.sandbox_id)
            raise HTTPException(status_code=502, detail="Stateful sandbox failed healthcheck")

        lease = await lease_mgr.create_or_renew(
            session_id=session_id,
            workspace_id=workspace_id,
            sandbox_id=instance.sandbox_id,
            endpoint=instance.endpoint,
            ttl_seconds=ttl,
            auth_token=auth_token,
        )


        # Update metrics
        active_leases = await lease_mgr.list_active()
        ACTIVE_SESSIONS.set(len(active_leases))
        active_locks = await lease_mgr.count_active_locks()
        WORKSPACE_LOCKS_ACTIVE.set(active_locks)

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
        await lease_mgr.release_workspace_lock(workspace_id, session_id)
        raise HTTPException(status_code=500, detail=f"Failed to acquire session: {e}")


@router.post("/{session_id}/heartbeat", response_model=HeartbeatResponse)
async def session_heartbeat(
    session_id: str,
    payload: HeartbeatRequest = None,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
) -> HeartbeatResponse:
    """Send sliding heartbeat to keep the session alive and refresh workspace lock."""
    ttl = payload.ttl_seconds if payload else None
    lease = await lease_mgr.heartbeat(session_id, ttl)
    if not lease:
        raise HTTPException(status_code=404, detail="Session not found or expired")

    if lease.workspace_id:
        await lease_mgr.refresh_workspace_lock(lease.workspace_id, session_id, ttl)

    return HeartbeatResponse(
        session_id=lease.session_id,
        expires_at=lease.expires_at,
        status=lease.status,
    )


@router.post("/{session_id}/release")
async def release_session(
    session_id: str,
    driver: ContainerDriver = Depends(get_driver),
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    """Explicitly release and terminate a session while releasing the workspace concurrency lock."""
    lease = await lease_mgr.get(session_id)
    if not lease:
        raise HTTPException(status_code=404, detail="Session not found")

    await lease_mgr.release(session_id)
    if lease.workspace_id:
        await lease_mgr.release_workspace_lock(lease.workspace_id, session_id)

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

async def _get_active_session(session_id: str, lease_mgr: LeaseManager) -> tuple[str, Optional[str]]:
    lease = await lease_mgr.get(session_id)
    if not lease or lease.status != "active":
        raise HTTPException(status_code=404, detail="Session not found or expired")
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
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr)
    body = await request.json()
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}

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
            try:
                async with client.stream("POST", f"{endpoint}/exec", json=body, headers=headers) as r:
                    async for chunk in r.aiter_bytes():
                        yield chunk
            finally:
                stop_keepalive.set()
                keepalive_task.cancel()
                await client.aclose()

        return StreamingResponse(stream_generator(), media_type="application/x-ndjson")

    # Buffered execution with auto-keepalive
    timeout_val = float(body.get("timeout", 300.0))
    async with httpx.AsyncClient(timeout=timeout_val + 10.0) as client:
        resp = await client.post(f"{endpoint}/exec", json=body, headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.post("/{session_id}/fs/write")
async def proxy_fs_write(
    session_id: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    body = await request.json()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{endpoint}/fs/write", json=body, headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.get("/{session_id}/fs/read")
async def proxy_fs_read(
    session_id: str,
    path: str,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{endpoint}/fs/read", params={"path": path}, headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.get("/{session_id}/fs/list")
async def proxy_fs_list(
    session_id: str,
    path: str = ".",
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr)
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
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    body = await request.json()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{endpoint}/fs/delete", json=body, headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.get("/{session_id}/fs/quota")
async def proxy_fs_quota(
    session_id: str,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    """Retrieve workspace disk quota metrics (used, quota, usage percentage)."""
    endpoint, auth_token = await _get_active_session(session_id, lease_mgr)
    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{endpoint}/fs/quota", headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))
