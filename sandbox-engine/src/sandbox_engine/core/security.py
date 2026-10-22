from __future__ import annotations

import base64
import hashlib
import os
import re
import secrets
from typing import Optional
from cryptography.fernet import Fernet, MultiFernet
from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ..config import settings

# Strict workspace ID regex: alphanumeric, underscores, hyphens, max 63 chars (capped for K8s label compliance), strictly matching \Z
WORKSPACE_ID_REGEX = re.compile(r"^[a-zA-Z0-9_\-]{1,63}\Z")

# Strict tenant ID regex: alphanumeric, underscores, hyphens, dots, max 63 chars (compatible with K8s label standard)
TENANT_ID_REGEX = re.compile(r"^[a-zA-Z0-9_\-\.]{1,63}\Z")

# Strict session ID regex: alphanumeric, underscores, hyphens, dots, max 63 chars
SESSION_ID_REGEX = re.compile(r"^[a-zA-Z0-9_\-\.]{1,63}\Z")

security_bearer = HTTPBearer(auto_error=False)


def safe_compare_digest(a: Optional[str], b: Optional[str]) -> bool:
    """
    Constant-time string comparison that safely handles non-ASCII characters
    by encoding to UTF-8 bytes first. Prevents TypeError (500 Internal Server Error)
    when non-ASCII characters are supplied in API keys.
    """
    if a is None or b is None:
        return False
    try:
        return secrets.compare_digest(a.encode("utf-8"), b.encode("utf-8"))
    except Exception:
        return False


def validate_session_id(session_id: str) -> str:
    """Validates session_id against strict regex to prevent 500s from DB truncation or K8s label errors."""
    if not session_id or not isinstance(session_id, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="session_id must be a non-empty string",
        )
    if not SESSION_ID_REGEX.fullmatch(session_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid session_id '{session_id}'. Must match pattern '^[a-zA-Z0-9_\\-\\.]+$' (max 63 chars)",
        )
    return session_id


def _fernet_from_secret(secret: str) -> Fernet:
    key_hash = hashlib.sha256(secret.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(key_hash))


def _get_fernet() -> MultiFernet | Fernet:
    keys = [settings.encryption_key]
    extra = os.environ.get("SANDBOX_ENCRYPTION_KEYS", "")
    if extra.strip():
        keys.extend(k.strip() for k in extra.split(",") if k.strip())
    fernets = [_fernet_from_secret(k) for k in keys if k]
    if len(fernets) == 1:
        return fernets[0]
    return MultiFernet(fernets)


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
    except Exception as e:
        is_prod = settings.environment.lower() in ("production", "prod")
        if is_prod:
            raise ValueError("Failed to decrypt stored auth token") from e
        return encrypted_token


import json
from .rate_limiter import rate_limiter


def validate_workspace_id(
    workspace_id: str,
    workspaces_root: Optional[str] = None,
    tenant_id: str = "default",
    create_host_dir: bool = True,
) -> str:
    """
    Validates workspace_id against strict regex (no trailing newlines) and guarantees
    path canonicalization strictly within a tenant-scoped directory.
    Eliminates cross-tenant workspace file sharing and directory traversal.
    Skips host directory creation when create_host_dir=False (e.g., on K8s with read-only rootfs).
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

    # 3. Ensure host workspace directory exists and has 0o777 permissions (Docker driver only)
    if create_host_dir:
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


async def authenticate_tenant_credentials(
    provided_key: Optional[str],
    raw_tenant: Optional[str],
    has_query_token: bool = False,
    enforce_rate_limit: bool = True,
) -> str:
    """
    Unified authentication and tenant identity resolution shared between REST and WebSocket routes.
    Guarantees:
      - Rejects query parameter tokens in production.
      - Enforces strict tenant_id regex validation (compatible with K8s labels).
      - Mandates X-Tenant-ID header in production (rejects 'default').
      - Validates per-tenant keys if configured via SANDBOX_TENANT_KEYS_JSON.
      - Enforces tenant rate limits strictly AFTER successful authentication.
    """
    is_prod = settings.environment.lower() in ("production", "prod")
    expected_key = settings.api_key

    if is_prod and not expected_key and not settings.tenant_keys_json:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: SANDBOX_API_KEY or SANDBOX_TENANT_KEYS_JSON must be set in production mode",
        )

    # In production, query parameter token is strictly forbidden (prevents URL leakage in logs/traces)
    if is_prod and has_query_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Security violation: API token in URL query parameter is forbidden in production mode",
        )

    tenant_str = (raw_tenant or "").strip()
    tenant_key_map: dict = {}
    if settings.tenant_keys_json:
        try:
            tenant_key_map = json.loads(settings.tenant_keys_json)
            if not isinstance(tenant_key_map, dict):
                raise ValueError("tenant_keys_json must be a JSON object")
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Configuration error in SANDBOX_TENANT_KEYS_JSON: {e}",
            )

    if tenant_key_map:
        matched_tenant = None
        for tid, key in tenant_key_map.items():
            if provided_key and safe_compare_digest(provided_key, str(key)):
                matched_tenant = str(tid)
                break
        if not matched_tenant:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unauthorized: invalid or missing Sandbox API key",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if tenant_str and tenant_str != matched_tenant:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="X-Tenant-ID does not match the authenticated tenant key",
            )
        tenant_id = matched_tenant
    else:
        # Resolves N2: Support api-key-only authentication when SANDBOX_TENANT_KEYS_JSON is not configured
        if expected_key:
            if not provided_key or not safe_compare_digest(provided_key, expected_key):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Unauthorized: invalid or missing Sandbox API key",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            tenant_id = tenant_str if tenant_str else "default"
        else:
            if is_prod:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Server configuration error: SANDBOX_API_KEY or SANDBOX_TENANT_KEYS_JSON required in production",
                )
            tenant_id = tenant_str if tenant_str else "default"

    if not TENANT_ID_REGEX.fullmatch(tenant_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid tenant_id '{tenant_id}'. Must match pattern '^[a-zA-Z0-9_\\-\\.]+$' (max 63 chars)",
        )

    # Post-Auth Rate Limiting: executed ONLY after credentials have been verified
    if enforce_rate_limit:
        await rate_limiter.check_rate_limit(tenant_id)

    return tenant_id


async def verify_api_key(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security_bearer),
) -> str:
    """
    Validates caller API Key against SANDBOX_API_KEY or per-tenant keys for REST endpoints.
    """
    is_prod = settings.environment.lower() in ("production", "prod")

    provided_key: Optional[str] = None
    if credentials and credentials.credentials:
        provided_key = credentials.credentials
    elif "x-api-key" in request.headers:
        provided_key = request.headers["x-api-key"]
    elif not is_prod and "token" in request.query_params:
        provided_key = request.query_params["token"]

    raw_tenant = request.headers.get("x-tenant-id")
    has_query_token = "token" in request.query_params

    # Enforce authentication, format checks, and post-auth rate limiting
    tenant_id = await authenticate_tenant_credentials(
        provided_key=provided_key,
        raw_tenant=raw_tenant,
        has_query_token=has_query_token,
        enforce_rate_limit=True,
    )

    request.state.tenant_id = tenant_id
    return tenant_id
