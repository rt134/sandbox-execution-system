from __future__ import annotations

import asyncio
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import websockets

from ..core.lease_manager import LeaseManager

logger = logging.getLogger("sandbox.routes_ws")
router = APIRouter(prefix="/v1/sessions", tags=["WebSocket PTY"])


@router.websocket("/{session_id}/ws/pty")
async def websocket_pty_proxy(websocket: WebSocket, session_id: str):
    await websocket.accept()
    lease_mgr: LeaseManager = websocket.app.state.lease_manager

    lease = await lease_mgr.get(session_id)
    if not lease or lease.status != "active":
        await websocket.close(code=1008, reason="Session not found or expired")
        return

    # Convert http:// endpoint to ws://
    container_ws_url = lease.endpoint.replace("http://", "ws://") + "/ws/pty"

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

            # Run bi-directional proxying
            await asyncio.gather(client_to_backend(), backend_to_client(), return_exceptions=True)

    except Exception as e:
        logger.error(f"WebSocket proxy error for session {session_id}: {e}")
        try:
            await websocket.close(code=1011, reason=str(e))
        except Exception:
            pass
