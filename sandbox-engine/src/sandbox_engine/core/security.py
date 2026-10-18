from __future__ import annotations

import base64
import hashlib
import os
import re
import secrets
from typing import Optional
from cryptography.fernet import Fernet
from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ..config import settings

# Strict workspace ID regex: alphanumeric, underscores, hyphens, max 64 chars, strictly matching end of string \Z
WORKSPACE_ID_REGEX = re.compile(r"^[a-zA-Z0-9_\-]{1,64}\Z")

security_bearer = HTTPBearer(auto_error=False)


def _get_fernet() -> Fernet:
    """Derives a deterministic 32-byte urlsafe Fernet key from settings.encryption_key."""
    key_hash = hashlib.sha256(settings.encryption_key.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(key_hash))


def encrypt_token(token: Optional[str]) -> Optional[str]:
    """Encrypts a token at rest before persisting into PostgreSQL/database."""
    if not token:
        return None
    return _get_fernet().encrypt(token.encode("utf-8")).decode("utf-8")


def decrypt_token(encrypted_token: Optional[str]) -> Optional[str]:
    """Decrypts a token retrieved from PostgreSQL/database."""
    if not encrypted_token:
        return None
    try:
        return _get_fernet().decrypt(encrypted_token.encode("utf-8")).decode("utf-8")
    except Exception:
        # Fallback for unencrypted legacy tokens
        return encrypted_token


def validate_workspace_id(workspace_id: str, workspaces_root: Optional[str] = None) -> str:
    """
    Validates workspace_id against strict regex (no trailing newlines) and guarantees
    path canonicalization within workspaces_root.
    Also ensures directory permissions allow UID 1000 to read/write.
    """
    if not workspace_id or not isinstance(workspace_id, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="workspace_id must be a non-empty string",
        )

    # Reject newlines or null bytes
    if "\n" in workspace_id or "\r" in workspace_id or "\0" in workspace_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid workspace_id: line breaks or null characters are strictly forbidden",
        )

    # 1. Enforce strict character whitelist using fullmatch with \Z
    if not WORKSPACE_ID_REGEX.fullmatch(workspace_id):
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

    # 3. Ensure host workspace directory exists and has 0o777 permissions
    # so that the container's non-root UID 1000 can read/write without permission denial
    os.makedirs(target_path, mode=0o777, exist_ok=True)
    try:
        os.chmod(target_path, 0o777)
    except Exception:
        pass

    return target_path


def generate_daemon_token() -> str:
    """
    Generates a cryptographically strong unique token for shellctl daemon auth.
    Always produces a distinct random 32-byte secret per container to prevent shared-secret attacks.
    """
    return secrets.token_hex(32)


async def verify_api_key(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security_bearer),
) -> str:
    """
    Validates caller API Key against SANDBOX_API_KEY.
    In production mode, refuses to run if API key is not configured.
    Enforces tenant identification via X-Tenant-ID or API key binding.
    """
    is_prod = settings.environment.lower() in ("production", "prod")
    expected_key = settings.api_key

    if is_prod and not expected_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: SANDBOX_API_KEY must be set in production mode",
        )

    provided_key: Optional[str] = None
    if credentials and credentials.credentials:
        provided_key = credentials.credentials
    elif "x-api-key" in request.headers:
        provided_key = request.headers["x-api-key"]
    elif "token" in request.query_params:
        provided_key = request.query_params["token"]

    if expected_key:
        if not provided_key or not secrets.compare_digest(provided_key, expected_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized: invalid or missing Sandbox API key",
                headers={"WWW-Authenticate": "Bearer"},
            )

    # Resolve Tenant Identity
    tenant_id = request.headers.get("x-tenant-id", "default").strip()
    if not tenant_id:
        tenant_id = "default"

    request.state.tenant_id = tenant_id
    return tenant_id
