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


import json


def validate_workspace_id(
    workspace_id: str,
    workspaces_root: Optional[str] = None,
    tenant_id: str = "default",
) -> str:
    """
    Validates workspace_id against strict regex (no trailing newlines) and guarantees
    path canonicalization strictly within a tenant-scoped directory.
    Eliminates cross-tenant workspace file sharing and directory traversal.
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

    # 2. Path Traversal & Multi-Tenant Defense: tenant-scoped directory isolation
    tenant_slug = hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()[:16]
    root = os.path.realpath(os.path.abspath(workspaces_root or settings.workspaces_root))
    tenant_root = os.path.realpath(os.path.abspath(os.path.join(root, f"tenant_{tenant_slug}")))
    target_path = os.path.realpath(os.path.abspath(os.path.join(tenant_root, workspace_id)))

    # Ensure target path is strictly inside tenant_root
    expected_prefix = tenant_root if tenant_root.endswith(os.sep) else tenant_root + os.sep
    if not target_path.startswith(expected_prefix):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Security violation: workspace path escapes allowed tenant root directory",
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
    Validates caller API Key against SANDBOX_API_KEY or per-tenant keys.
    In production mode:
      - Refuses to run if API key is not configured.
      - Strictly forbids passing credentials via query parameters.
      - Mandates X-Tenant-ID header (rejects 'default').
      - Validates per-tenant key binding if configured.
    """
    is_prod = settings.environment.lower() in ("production", "prod")
    expected_key = settings.api_key

    if is_prod and not expected_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: SANDBOX_API_KEY must be set in production mode",
        )

    # In production, query parameter token is strictly forbidden (prevents URL leakage)
    if is_prod and "token" in request.query_params:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Security violation: API token in URL query parameter is forbidden in production mode",
        )

    provided_key: Optional[str] = None
    if credentials and credentials.credentials:
        provided_key = credentials.credentials
    elif "x-api-key" in request.headers:
        provided_key = request.headers["x-api-key"]
    elif not is_prod and "token" in request.query_params:
        provided_key = request.query_params["token"]

    # Resolve and validate Tenant Identity
    raw_tenant = request.headers.get("x-tenant-id", "").strip()
    if is_prod:
        if not raw_tenant or raw_tenant.lower() == "default":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Header 'X-Tenant-ID' is required in production and cannot be 'default'",
            )
        tenant_id = raw_tenant
    else:
        tenant_id = raw_tenant if raw_tenant else "default"

    # Per-tenant key validation if tenant_keys_json is configured
    if settings.tenant_keys_json:
        try:
            tenant_key_map = json.loads(settings.tenant_keys_json)
            if tenant_id in tenant_key_map:
                expected_key = tenant_key_map[tenant_id]
            else:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Tenant '{tenant_id}' is not authorized or key mapping is missing",
                )
        except json.JSONDecodeError:
            pass

    if expected_key:
        if not provided_key or not secrets.compare_digest(provided_key, expected_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized: invalid or missing Sandbox API key",
                headers={"WWW-Authenticate": "Bearer"},
            )

    request.state.tenant_id = tenant_id
    return tenant_id
