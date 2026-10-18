from __future__ import annotations

import asyncio
import logging
import secrets
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import websockets

from ..config import settings
from ..core.lease_manager import LeaseManager

logger = logging.getLogger("sandbox.routes_ws")
router = APIRouter(prefix="/v1/sessions", tags=["WebSocket PTY"])


@router.websocket("/{session_id}/ws/pty")
async def websocket_pty_proxy(websocket: WebSocket, session_id: str):
    is_prod = settings.environment.lower() in ("production", "prod")

    # 1. Verify caller API key and credentials
    if settings.api_key:
        if is_prod and "token" in websocket.query_params:
            await websocket.close(code=1008, reason="API token in URL query parameter is forbidden in production")
            return

        token = websocket.headers.get("x-api-key")
        auth_hdr = websocket.headers.get("authorization", "")
        if auth_hdr.startswith("Bearer "):
            token = auth_hdr.replace("Bearer ", "").strip()
        elif not is_prod and "token" in websocket.query_params:
            token = websocket.query_params.get("token")

        if not token or not secrets.compare_digest(token, settings.api_key):
            await websocket.close(code=1008, reason="Unauthorized: invalid API key")
            return

    # 2. Extract and validate Tenant Identity
    raw_tenant = websocket.headers.get("x-tenant-id", "").strip()
    if is_prod:
        if not raw_tenant or raw_tenant.lower() == "default":
            await websocket.close(code=1008, reason="Header 'X-Tenant-ID' is required in production and cannot be 'default'")
            return
        tenant_id = raw_tenant
    else:
        tenant_id = raw_tenant or websocket.query_params.get("tenant_id") or "default"

    lease_mgr: LeaseManager = websocket.app.state.lease_manager

    # 3. Verify session existence and tenant ownership
    lease = await lease_mgr.get(session_id)
    if not lease or lease.status != "active":
        await websocket.close(code=1008, reason="Session not found or expired")
        return

    if lease.tenant_id and lease.tenant_id != tenant_id:
        await websocket.close(code=4403, reason="Access denied: session belongs to another tenant")
        return

    await websocket.accept()

    # Convert http:// endpoint to ws:// with internal daemon auth token
    container_ws_url = lease.endpoint.replace("http://", "ws://") + "/ws/pty"
    if lease.auth_token:
        container_ws_url += f"?token={lease.auth_token}"

    try:
        async with websockets.connect(container_ws_url, ping_interval=20, ping_timeout=20) as backend_ws:

            async def client_to_backend():
                try:
                    while True:
                        msg = await websocket.receive()
                        # Enforce WebSocket message payload limits
                        if "text" in msg:
                            if len(msg["text"]) > settings.max_ws_message_size_bytes:
                                await websocket.close(code=1009, reason="Message exceeds size limit")
                                break
                            await backend_ws.send(msg["text"])
                        elif "bytes" in msg:
                            if len(msg["bytes"]) > settings.max_ws_message_size_bytes:
                                await websocket.close(code=1009, reason="Message exceeds size limit")
                                break
                            await backend_ws.send(msg["bytes"])
                        # Auto-extend lease on user activity
                        await lease_mgr.heartbeat(session_id)
                except (WebSocketDisconnect, asyncio.CancelledError):
                    pass

            async def backend_to_client():
                try:
                    async for data in backend_ws:
                        if isinstance(data, str):
                            await websocket.send_text(data)
                        else:
                            await websocket.send_bytes(data)
                except (WebSocketDisconnect, asyncio.CancelledError):
                    pass

            # Run bi-directional proxying; exit as soon as EITHER direction finishes to avoid hanging
            t1 = asyncio.create_task(client_to_backend())
            t2 = asyncio.create_task(backend_to_client())

            done, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()

    except Exception as e:
        logger.error(f"WebSocket proxy error for session {session_id}: {e}")
        try:
            await websocket.close(code=1011, reason=str(e))
        except Exception:
            pass
