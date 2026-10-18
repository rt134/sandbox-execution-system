from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CodeResult(BaseModel):
    """Result of a stateless code execution."""
    output: Any = None
    logs: str = ""
    error: Optional[str] = None
    exit_code: int = 0
    duration_ms: float = 0.0
    sandbox_id: Optional[str] = None


class ExecResult(BaseModel):
    """Result of a buffered shell execution."""
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    duration_ms: float = 0.0
    error: Optional[str] = None


class ExecChunk(BaseModel):
    """Chunk of streaming process execution."""
    stream: Optional[str] = None  # "stdout" or "stderr"
    text: str = ""
    exit_code: Optional[int] = None
    error: Optional[str] = None


class FileInfo(BaseModel):
    """Metadata for a file or directory in /workspace."""
    name: str
    path: str
    size: int
    is_dir: bool
    mode: str = ""
    modified_at: str = ""


class SessionInfo(BaseModel):
    """Metadata for an active stateful agent session."""
    session_id: str
    workspace_id: str
    sandbox_id: str
    endpoint: str
    expires_at: float
    status: str = "active"


class SandboxError(Exception):
    """Base exception for Sandbox Client errors."""
    pass


class ExecutionTimeoutError(SandboxError):
    """Raised when code execution times out."""
    pass
