from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional
import httpx

from .models import CodeResult, SandboxError, SessionInfo, WorkspaceConflictError
from .session import SandboxSession


class SandboxClient:
    """
    Unified client for the Sandbox Execution System.
    Connects to sandbox-engine to run stateless code and manage stateful agent sessions.
    """

    def __init__(self, base_url: str = "http://localhost:8080", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def run_code(
        self,
        language: str = "python",
        code: str = "",
        inputs: Optional[Dict[str, Any]] = None,
        timeout: float = 10.0,
    ) -> CodeResult:
        """
        Execute stateless code with sub-50ms latency using the engine's warm pool.
        Ideal for code execution, code interpreters, and tool calls.
        """
        resp = await self._http.post(
            "/v1/stateless/run",
            json={
                "language": language,
                "code": code,
                "inputs": inputs or {},
                "timeout": timeout,
            },
            timeout=timeout + 5.0,
        )
        if resp.status_code != 200:
            raise SandboxError(f"Stateless run failed ({resp.status_code}): {resp.text}")

        data = resp.json()
        return CodeResult(
            output=data.get("output"),
            logs=data.get("logs", ""),
            error=data.get("error"),
            exit_code=data.get("exit_code", 0),
            duration_ms=data.get("duration_ms", 0.0),
            sandbox_id=data.get("sandbox_id"),
        )

    @asynccontextmanager
    async def acquire_session(
        self,
        workspace_id: str,
        session_id: Optional[str] = None,
        ttl_seconds: Optional[int] = 300,
    ) -> AsyncIterator[SandboxSession]:
        """
        Acquire a stateful sandbox session as an async context manager.
        Automatically releases the session when exiting the `async with` block.
        """
        session = await self.create_session(
            workspace_id=workspace_id,
            session_id=session_id,
            ttl_seconds=ttl_seconds,
        )
        try:
            yield session
        finally:
            await session.release()

    async def create_session(
        self,
        workspace_id: str,
        session_id: Optional[str] = None,
        ttl_seconds: Optional[int] = 300,
    ) -> SandboxSession:
        """Create a stateful session without context management."""
        resp = await self._http.post(
            "/v1/sessions/acquire",
            json={
                "session_id": session_id,
                "workspace_id": workspace_id,
                "ttl_seconds": ttl_seconds,
            },
        )
        if resp.status_code == 409:
            raise WorkspaceConflictError(f"Workspace concurrency conflict (409): {resp.text}")
        if resp.status_code != 200:
            raise SandboxError(f"Failed to acquire session: {resp.text}")

        info = SessionInfo.model_validate(resp.json())
        return SandboxSession(self.base_url, info, self._http)


    async def aclose(self):
        """Close the underlying HTTP client."""
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
