from __future__ import annotations

import json
import os
import re
import secrets
from typing import List, Literal, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EngineSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SANDBOX_",
        env_file=".env",
        extra="ignore",
    )

    # Core Execution Driver
    driver: Literal["docker", "k8s"] = "docker"

    # Engine Replica Instance ID (uses stable HOSTNAME/POD_NAME if present, otherwise random)
    engine_instance_id: str = Field(
        default_factory=lambda: os.environ.get("HOSTNAME") or os.environ.get("POD_NAME") or secrets.token_hex(4)
    )

    # Container Images
    image: str = "sandbox-base:latest"

    # Authentication & Security
    api_key: Optional[str] = None  # Caller -> Engine Bearer token / X-API-Key
    tenant_keys_json: Optional[str] = os.environ.get("SANDBOX_TENANT_KEYS_JSON")  # Optional tenant-specific keys
    encryption_key: str = os.environ.get("SANDBOX_ENCRYPTION_KEY", "sandbox-default-secret-key-change-in-prod-32bytes")
    cors_allowed_origins: List[str] = ["*"]
    max_request_size_bytes: int = 25 * 1024 * 1024  # 25MB request body cap
    max_stream_response_bytes: int = 50 * 1024 * 1024  # 50MB max streaming response
    max_ws_message_size_bytes: int = 1024 * 1024  # 1MB max WebSocket message

    # Per-Tenant Quotas & Limits
    max_session_ttl: int = 7200  # Max 2 hours session duration
    max_stateless_timeout: float = 60.0  # Max 60 seconds execution timeout
    max_concurrent_sessions_per_tenant: int = 10  # Max active sessions per tenant
    rate_limit_requests_per_minute: int = 120  # Rate limit per tenant

    # Pool Configurations (Stateless)
    warm_pool_size: int = 2
    warm_pool_acquire_timeout: float = 5.0
    max_concurrent_spawns: int = 10  # Cap concurrent container launches

    # Environment & Persistence (PostgreSQL)
    environment: str = os.environ.get("ENV", os.environ.get("ENVIRONMENT", "development"))
    postgres_url: Optional[str] = (
        os.environ.get("SANDBOX_POSTGRES_URL")
        or os.environ.get("POSTGRES_URL")
        or os.environ.get("DATABASE_URL")
    )
    default_local_postgres_url: str = "postgresql://sandbox:sandbox@localhost:5432/sandbox"

    # Stateful Session Configurations
    default_session_ttl: int = 300
    sweeper_interval_sec: float = 5.0
    orphan_grace_period_sec: float = 60.0  # Prevent sweeper race on freshly created containers
    workspaces_root: str = os.environ.get("SANDBOX_WORKSPACES_ROOT", os.path.abspath(".workspaces"))
    redis_url: Optional[str] = None  # Optional secondary cache/coordination
    sqlite_path: str = ".sandbox_ledger.db"
    require_redis: bool = False  # If True, refuse silent fallback

    def get_effective_postgres_url(self) -> Optional[str]:
        """
        Returns the PostgreSQL connection string.
        In production, requires the connection URL to be provided via ENV.
        In development, defaults to the local Docker PostgreSQL container.
        """
        if self.postgres_url:
            return self.postgres_url

        if self.environment.lower() in ("production", "prod"):
            raise ValueError(
                "Production environment requires PostgreSQL database URL! "
                "Please set SANDBOX_POSTGRES_URL, POSTGRES_URL, or DATABASE_URL in your environment."
            )

        return self.default_local_postgres_url

    def validate_tenant_keys_json(self):
        """
        Validates the format and syntax of SANDBOX_TENANT_KEYS_JSON.
        Fails fast on malformed JSON or invalid tenant_id to prevent silent auth bypass.
        """
        if self.tenant_keys_json:
            try:
                mapping = json.loads(self.tenant_keys_json)
            except Exception as e:
                raise ValueError(
                    f"Configuration error: SANDBOX_TENANT_KEYS_JSON contains malformed JSON: {e}"
                )
            if not isinstance(mapping, dict):
                raise ValueError(
                    "Configuration error: SANDBOX_TENANT_KEYS_JSON must be a JSON dictionary mapping tenant_id -> api_key"
                )
            for tid, key in mapping.items():
                if not re.match(r"^[a-zA-Z0-9_\-\.]{1,63}\Z", str(tid)):
                    raise ValueError(
                        f"Configuration error: Invalid tenant_id '{tid}' in SANDBOX_TENANT_KEYS_JSON. "
                        "Must match '^[a-zA-Z0-9_\\-\\.]+$' (max 63 chars)."
                    )
                if not isinstance(key, str) or not key.strip():
                    raise ValueError(
                        f"Configuration error: Invalid or empty API key for tenant '{tid}' in SANDBOX_TENANT_KEYS_JSON"
                    )

    def validate_production_settings(self):
        """
        Validates that production environment has mandatory security configurations.
        Fails fast if auth, persistence, or encryption keys are missing, weak, or default placeholders.
        """
        self.validate_tenant_keys_json()
        if self.environment.lower() in ("production", "prod"):
            # 1. API Key Validation
            if not self.api_key:
                raise ValueError(
                    "Production environment requires SANDBOX_API_KEY to be set! "
                    "Refusing to start engine in fail-open unauthenticated mode."
                )
            if len(self.api_key) < 32:
                raise ValueError(
                    f"Production SANDBOX_API_KEY is too short ({len(self.api_key)} chars). Must be at least 32 characters."
                )
            weak_patterns = [
                "change-in-prod",
                "replace-me",
                "replace-in-prod",
                "default-secret",
                "secret-key",
                "sandbox-default",
                "default-sandbox",
                "sandbox-secret",
                "12345",
                "password",
                "admin",
            ]
            for wp in weak_patterns:
                if wp in self.api_key.lower():
                    raise ValueError(
                        f"Production SANDBOX_API_KEY contains known insecure placeholder '{wp}'. Must set a real secret."
                    )

            # 2. Encryption Key Validation
            if not self.encryption_key or len(self.encryption_key) < 32:
                raise ValueError(
                    "Production requires a strong SANDBOX_ENCRYPTION_KEY of at least 32 characters for token encryption at rest."
                )
            for wp in weak_patterns:
                if wp in self.encryption_key.lower():
                    raise ValueError(
                        f"Production SANDBOX_ENCRYPTION_KEY contains known insecure placeholder '{wp}'. Must set a real secret."
                    )

            # 3. Database URL Validation
            if not self.postgres_url:
                raise ValueError(
                    "Production environment requires PostgreSQL database URL! "
                    "Please set SANDBOX_POSTGRES_URL, POSTGRES_URL, or DATABASE_URL in your environment."
                )

            # 4. Tenant Keys Validation (Fail-fast on malformed JSON or weak keys)
            if self.tenant_keys_json:
                try:
                    mapping = json.loads(self.tenant_keys_json)
                    if not isinstance(mapping, dict):
                        raise ValueError(
                            "Production configuration error: SANDBOX_TENANT_KEYS_JSON must be a JSON dictionary mapping tenant_id -> api_key"
                        )
                    for tid, key in mapping.items():
                        if not re.match(r"^[a-zA-Z0-9_\-\.]{1,63}\Z", str(tid)):
                            raise ValueError(f"Invalid tenant_id '{tid}' in SANDBOX_TENANT_KEYS_JSON")
                        if not isinstance(key, str) or len(key) < 32:
                            raise ValueError(
                                f"API key for tenant '{tid}' in SANDBOX_TENANT_KEYS_JSON is too short. Must be at least 32 characters."
                            )
                except json.JSONDecodeError as e:
                    raise ValueError(f"Production configuration error: SANDBOX_TENANT_KEYS_JSON contains malformed JSON: {e}")

            # 5. Driver & Runtime Isolation Validation
            if self.driver == "docker" and not os.environ.get("ALLOW_INSECURE_DOCKER_PRODUCTION"):
                raise ValueError(
                    "Production environment requires SANDBOX_DRIVER=k8s with strong container isolation (gVisor/Kata). "
                    "Docker driver is supported for local development and testing only. "
                    "Set SANDBOX_DRIVER=k8s or set ALLOW_INSECURE_DOCKER_PRODUCTION=1 to explicitly override."
                )

    # Egress Proxy Configurations (Squid)
    http_proxy: Optional[str] = None
    https_proxy: Optional[str] = None
    no_proxy: str = "localhost,127.0.0.1"

    # Docker Specific Settings
    docker_runtime: str = "runc"  # 'runc' for local dev, 'runsc' for gVisor production
    allow_runtime_fallback: bool = False  # Fail hard if requested secure runtime is unavailable!
    docker_network: str = "bridge"
    docker_network_internal: bool = False  # Set to True for internal network with no direct host egress
    docker_host_port_start: int = 15000

    # Kubernetes Specific Settings
    k8s_namespace: str = "sandboxes"
    k8s_runtime_class: str = "gvisor"
    k8s_node_selector_key: str = "sandbox"
    k8s_node_selector_value: str = "true"
    k8s_use_pvc: bool = False
    k8s_storage_class: Optional[str] = None

    # Engine Server Settings
    host: str = "0.0.0.0"
    port: int = 8080

    # Resource & Security Limits
    pids_limit: int = 256  # Fork-bomb prevention: cgroup pids.max
    default_mem_limit: str = "512m"
    default_cpu_limit: float = 1.0
    default_workspace_disk_quota_mb: int = 1024  # 1GB disk quota
    enable_prometheus_metrics: bool = True


settings = EngineSettings()
