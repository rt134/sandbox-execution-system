from __future__ import annotations

import json
from typing import AsyncIterator, Dict, Optional
import httpx

from .fs import FileSystemClient
from .models import ExecChunk, ExecResult, SandboxError, SessionInfo
from .terminal import TerminalClient


class SandboxSession:
    """
    Represents an active, stateful sandbox session for an autonomous agent.
    Provides filesystem access, process execution, and interactive PTY terminals.
    """

    def __init__(self, base_url: str, info: SessionInfo, http_client: httpx.AsyncClient):
        self.base_url = base_url.rstrip("/")
        self.info = info
        self._http = http_client

        # Sub-clients
        self.fs = FileSystemClient(base_url, info.session_id, http_client)
        self.terminal = TerminalClient(base_url, info.session_id)

    @property
    def id(self) -> str:
        return self.info.session_id

    @property
    def workspace_id(self) -> str:
        return self.info.workspace_id

    async def exec(
        self,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = 300.0,
    ) -> ExecResult:
        """Run a command and wait for its complete buffered output."""
        resp = await self._http.post(
            f"{self.base_url}/v1/sessions/{self.id}/exec",
            json={
                "command": command,
                "cwd": cwd,
                "env": env or {},
                "timeout": timeout,
                "stream": False,
            },
            timeout=timeout + 10.0 if timeout else 310.0,
        )
        if resp.status_code != 200:
            raise SandboxError(f"Exec command failed: {resp.text}")

        data = resp.json()
        return ExecResult(
            stdout=data.get("stdout", ""),
            stderr=data.get("stderr", ""),
            exit_code=data.get("exit_code", 0),
            duration_ms=data.get("duration_ms", 0.0),
            error=data.get("error"),
        )

    async def exec_stream(
        self,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = 300.0,
    ) -> AsyncIterator[ExecChunk]:
        """Run a command and stream stdout/stderr chunks in real-time."""
        req_payload = {
            "command": command,
            "cwd": cwd,
            "env": env or {},
            "timeout": timeout,
            "stream": True,
        }

        async with self._http.stream(
            "POST",
            f"{self.base_url}/v1/sessions/{self.id}/exec",
            json=req_payload,
            timeout=None,
        ) as response:
            if response.status_code != 200:
                body = await response.aread()
                raise SandboxError(f"Exec stream failed with status {response.status_code}: {body.decode()}")

            async for line in response.aiter_lines():
                if not line.strip():
                    continue
                try:
                    chunk_data = json.loads(line)
                    yield ExecChunk(
                        stream=chunk_data.get("stream"),
                        text=chunk_data.get("text", ""),
                        exit_code=chunk_data.get("exit_code"),
                        error=chunk_data.get("error"),
                    )
                except Exception:
                    pass

    async def heartbeat(self, ttl_seconds: Optional[int] = None) -> None:
        """Send a heartbeat to extend the session lease."""
        resp = await self._http.post(
            f"{self.base_url}/v1/sessions/{self.id}/heartbeat",
            json={"ttl_seconds": ttl_seconds},
        )
        if resp.status_code != 200:
            raise SandboxError(f"Heartbeat failed: {resp.text}")

    async def release(self) -> None:
        """Release the session and shut down the container."""
        await self._http.post(f"{self.base_url}/v1/sessions/{self.id}/release")

    async def __aenter__(self) -> SandboxSession:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.release()
        except Exception:
            pass
