from __future__ import annotations

import asyncio
import json
from typing import Optional, Union
import websockets

from .models import SandboxError


class PTYChannel:
    """Represents an active interactive terminal session over WebSocket."""

    def __init__(self, ws: websockets.WebSocketClientProtocol):
        self._ws = ws
        self._closed = False

    async def send(self, data: Union[str, bytes]) -> None:
        """Send raw keystrokes or text to the terminal stdin."""
        if self._closed:
            raise SandboxError("PTY channel is closed")
        if isinstance(data, str):
            await self._ws.send(data.encode("utf-8"))
        else:
            await self._ws.send(data)

    async def resize(self, cols: int, rows: int) -> None:
        """Send a SIGWINCH window resize event."""
        if self._closed:
            raise SandboxError("PTY channel is closed")
        payload = json.dumps({"type": "resize", "cols": cols, "rows": rows})
        await self._ws.send(payload)

    async def recv(self, timeout: Optional[float] = None) -> str:
        """Receive terminal output text from the remote PTY."""
        if self._closed:
            raise SandboxError("PTY channel is closed")
        try:
            if timeout:
                frame = await asyncio.wait_for(self._ws.recv(), timeout=timeout)
            else:
                frame = await self._ws.recv()

            if isinstance(frame, bytes):
                return frame.decode("utf-8", errors="replace")
            return str(frame)
        except asyncio.TimeoutError:
            return ""

    async def close(self) -> None:
        """Close the interactive PTY channel."""
        if not self._closed:
            self._closed = True
            await self._ws.close()


class TerminalClient:
    """Client for opening interactive PTY terminal channels."""

    def __init__(self, base_url: str, session_id: str):
        # Convert http/https to ws/wss
        ws_base = base_url.replace("http://", "ws://").replace("https://", "wss://").rstrip("/")
        self.ws_url = f"{ws_base}/v1/sessions/{session_id}/ws/pty"

    async def open(self, cols: int = 80, rows: int = 24) -> PTYChannel:
        """Open an interactive PTY shell."""
        try:
            ws = await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20)
            channel = PTYChannel(ws)
            if cols != 80 or rows != 24:
                await channel.resize(cols, rows)
            return channel
        except Exception as e:
            raise SandboxError(f"Failed to open PTY terminal: {e}")
