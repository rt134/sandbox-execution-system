from __future__ import annotations

import os
import re
import secrets
from typing import Optional
from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ..config import settings

# Strict workspace ID regex: alphanumeric, underscores, hyphens, max 64 chars
WORKSPACE_ID_REGEX = re.compile(r"^[a-zA-Z0-9_\-]{1,64}$")

security_bearer = HTTPBearer(auto_error=False)


def validate_workspace_id(workspace_id: str, workspaces_root: Optional[str] = None) -> str:
    """
    Validates workspace_id against strict regex and guarantees path canonicalization
    within the workspaces_root directory to prevent path traversal attacks.
    """
    if not workspace_id or not isinstance(workspace_id, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="workspace_id must be a non-empty string",
        )

    # 1. Enforce strict character whitelist
    if not WORKSPACE_ID_REGEX.match(workspace_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid workspace_id '{workspace_id}'. Must match pattern '^[a-zA-Z0-9_\\-]{{1,64}}$'",
        )

    # 2. Path Traversal Defense: canonical path resolution
    root = os.path.realpath(os.path.abspath(workspaces_root or settings.workspaces_root))
    target_path = os.path.realpath(os.path.abspath(os.path.join(root, workspace_id)))

    # Ensure target path is strictly a sub-path of root
    expected_prefix = root if root.endswith(os.sep) else root + os.sep
    if not target_path.startswith(expected_prefix):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Security violation: workspace path escapes allowed root directory",
        )

    return target_path


def generate_daemon_token() -> str:
    """Generates a cryptographically strong internal token for shellctl daemon auth."""
    if settings.daemon_auth_token:
        return settings.daemon_auth_token
    return secrets.token_hex(24)


async def verify_api_key(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security_bearer),
) -> Optional[str]:
    """
    Validates caller API Key against SANDBOX_API_KEY if configured.
    Supports Authorization: Bearer <key>, X-API-Key header, or ?token= query parameter.
    """
    expected_key = settings.api_key
    if not expected_key:
        return None  # No API key configured -> open for local dev

    provided_key: Optional[str] = None

    if credentials and credentials.credentials:
        provided_key = credentials.credentials
    elif "x-api-key" in request.headers:
        provided_key = request.headers["x-api-key"]
    elif "token" in request.query_params:
        provided_key = request.query_params["token"]

    if not provided_key or not secrets.compare_digest(provided_key, expected_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: invalid or missing Sandbox API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return provided_key
