from __future__ import annotations

import os
import uuid
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

from ..config import settings
from ..core.lease_manager import LeaseManager
from ..drivers.base import ContainerDriver
from ..models import (
    HeartbeatRequest,
    HeartbeatResponse,
    SandboxSpec,
    SessionAcquireRequest,
    SessionAcquireResponse,
)

router = APIRouter(prefix="/v1/sessions", tags=["Stateful Sessions"])


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
    """
    session_id = payload.session_id or f"sess_{uuid.uuid4().hex[:12]}"
    workspace_id = payload.workspace_id
    ttl = payload.ttl_seconds or settings.default_session_ttl

    # Check for existing active lease to reattach
    existing = await lease_mgr.get(session_id)
    if existing and existing.status == "active":
        # Check if container is still responsive
        if await driver.healthcheck(existing.endpoint):
            await lease_mgr.heartbeat(session_id, ttl)
            return SessionAcquireResponse(
                session_id=existing.session_id,
                workspace_id=existing.workspace_id,
                sandbox_id=existing.sandbox_id,
                endpoint=existing.endpoint,
                expires_at=existing.expires_at,
                status="active",
            )

    # Prepare persistent workspace directory on host
    workspace_dir = os.path.join(settings.workspaces_root, workspace_id)
    os.makedirs(workspace_dir, exist_ok=True)

    # Spawn new stateful sandbox container
    spec = SandboxSpec(
        spec_id=session_id,
        sandbox_type="stateful",
        image=settings.image,
        workspace_path=workspace_dir,
        labels={"sandbox.session_id": session_id, "sandbox.workspace_id": workspace_id},
    )

    instance = await driver.create_sandbox(spec)
    ready = await driver.healthcheck(instance.endpoint)
    if not ready:
        await driver.destroy_sandbox(instance.sandbox_id)
        raise HTTPException(status_code=502, detail="Stateful sandbox failed healthcheck")

    lease = await lease_mgr.create_or_renew(
        session_id=session_id,
        workspace_id=workspace_id,
        sandbox_id=instance.sandbox_id,
        endpoint=instance.endpoint,
        ttl_seconds=ttl,
    )

    return SessionAcquireResponse(
        session_id=lease.session_id,
        workspace_id=lease.workspace_id,
        sandbox_id=lease.sandbox_id,
        endpoint=lease.endpoint,
        expires_at=lease.expires_at,
        status="active",
    )


@router.post("/{session_id}/heartbeat", response_model=HeartbeatResponse)
async def session_heartbeat(
    session_id: str,
    payload: HeartbeatRequest = None,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
) -> HeartbeatResponse:
    """Send sliding heartbeat to keep the session alive."""
    ttl = payload.ttl_seconds if payload else None
    lease = await lease_mgr.heartbeat(session_id, ttl)
    if not lease:
        raise HTTPException(status_code=404, detail="Session not found or expired")
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
    """Explicitly release and terminate a session while keeping the workspace intact."""
    lease = await lease_mgr.get(session_id)
    if not lease:
        raise HTTPException(status_code=404, detail="Session not found")

    await lease_mgr.release(session_id)
    if lease.sandbox_id:
        await driver.destroy_sandbox(lease.sandbox_id)

    return {"status": "released", "session_id": session_id}


# =========================================================================
# Proxied Session APIs (Auto-Heartbeat on each interaction)
# =========================================================================

async def _get_active_session(session_id: str, lease_mgr: LeaseManager) -> str:
    lease = await lease_mgr.get(session_id)
    if not lease or lease.status != "active":
        raise HTTPException(status_code=404, detail="Session not found or expired")
    await lease_mgr.heartbeat(session_id)  # Auto-extend lease
    return lease.endpoint


@router.post("/{session_id}/exec")
async def proxy_exec(
    session_id: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    endpoint = await _get_active_session(session_id, lease_mgr)
    body = await request.json()

    if body.get("stream", False):
        client = httpx.AsyncClient(timeout=None)

        async def stream_generator():
            try:
                async with client.stream("POST", f"{endpoint}/exec", json=body) as r:
                    async for chunk in r.aiter_bytes():
                        yield chunk
            finally:
                await client.aclose()

        return StreamingResponse(stream_generator(), media_type="application/x-ndjson")

    async with httpx.AsyncClient(timeout=300.0) as client:
        resp = await client.post(f"{endpoint}/exec", json=body)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.post("/{session_id}/fs/write")
async def proxy_fs_write(
    session_id: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    endpoint = await _get_active_session(session_id, lease_mgr)
    body = await request.json()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{endpoint}/fs/write", json=body)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.get("/{session_id}/fs/read")
async def proxy_fs_read(
    session_id: str,
    path: str,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    endpoint = await _get_active_session(session_id, lease_mgr)
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{endpoint}/fs/read", params={"path": path})
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.get("/{session_id}/fs/list")
async def proxy_fs_list(
    session_id: str,
    path: str = ".",
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    endpoint = await _get_active_session(session_id, lease_mgr)
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{endpoint}/fs/list", params={"path": path})
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))


@router.post("/{session_id}/fs/delete")
async def proxy_fs_delete(
    session_id: str,
    request: Request,
    lease_mgr: LeaseManager = Depends(get_lease_manager),
):
    endpoint = await _get_active_session(session_id, lease_mgr)
    body = await request.json()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{endpoint}/fs/delete", json=body)
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))
