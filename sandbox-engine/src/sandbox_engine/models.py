from __future__ import annotations

import time
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class SandboxSpec(BaseModel):
    spec_id: str
    sandbox_type: Literal["stateless", "stateful"] = "stateless"
    image: str = "sandbox-base:latest"
    env: Dict[str, str] = Field(default_factory=dict)
    labels: Dict[str, str] = Field(default_factory=dict)
    workspace_path: Optional[str] = None
    cpu_limit: float = 1.0
    mem_limit: str = "512m"
    pids_limit: int = 256
    disk_quota_mb: int = 1024
    auth_token: Optional[str] = None


class SandboxInstance(BaseModel):
    sandbox_id: str
    driver: Literal["docker", "k8s"]
    endpoint: str  # Direct HTTP endpoint to shellctl, e.g. "http://127.0.0.1:15001" or "http://10.244.1.25:5004"
    created_at: float = Field(default_factory=time.time)
    status: str = "running"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    auth_token: Optional[str] = None


class StatelessRunRequest(BaseModel):
    language: str = "python"
    code: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    timeout: float = 10.0  # seconds


class StatelessRunResponse(BaseModel):
    output: Any = None
    logs: str = ""
    error: Optional[str] = None
    exit_code: int = 0
    duration_ms: float = 0.0
    sandbox_id: Optional[str] = None


class LeaseRecord(BaseModel):
    session_id: str
    workspace_id: str
    sandbox_id: str
    endpoint: str
    created_at: float
    expires_at: float
    status: Literal["active", "expired", "released"] = "active"
    auth_token: Optional[str] = None



class SessionAcquireRequest(BaseModel):
    session_id: Optional[str] = None
    workspace_id: str
    ttl_seconds: Optional[int] = None


class SessionAcquireResponse(BaseModel):
    session_id: str
    workspace_id: str
    sandbox_id: str
    endpoint: str
    expires_at: float
    status: str = "active"


class HeartbeatRequest(BaseModel):
    ttl_seconds: Optional[int] = None


class HeartbeatResponse(BaseModel):
    session_id: str
    expires_at: float
    status: str
