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
    # 1. Verify caller API key if configured
    if settings.api_key:
        token = websocket.query_params.get("token") or websocket.headers.get("x-api-key")
        auth_hdr = websocket.headers.get("authorization", "")
        if auth_hdr.startswith("Bearer "):
            token = auth_hdr.replace("Bearer ", "").strip()
        if not token or not secrets.compare_digest(token, settings.api_key):
            await websocket.close(code=1008, reason="Unauthorized: invalid API key")
            return

    await websocket.accept()
    lease_mgr: LeaseManager = websocket.app.state.lease_manager

    lease = await lease_mgr.get(session_id)
    if not lease or lease.status != "active":
        await websocket.close(code=1008, reason="Session not found or expired")
        return

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
                        if "text" in msg:
                            await backend_ws.send(msg["text"])
                        elif "bytes" in msg:
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
