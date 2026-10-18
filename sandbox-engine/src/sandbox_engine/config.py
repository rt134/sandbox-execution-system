from __future__ import annotations

import os
from typing import List, Literal, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class EngineSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SANDBOX_",
        env_file=".env",
        extra="ignore",
    )

    # Core Execution Driver
    driver: Literal["docker", "k8s"] = "docker"

    # Container Images
    image: str = "sandbox-base:latest"

    # Authentication & Security
    api_key: Optional[str] = None  # Caller -> Engine Bearer token / X-API-Key
    daemon_auth_token: Optional[str] = None  # Engine -> Daemon internal shared secret
    cors_allowed_origins: List[str] = ["*"]

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
    workspaces_root: str = os.path.abspath(".workspaces")
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

    # Egress Proxy Configurations (Squid)
    http_proxy: Optional[str] = None
    https_proxy: Optional[str] = None
    no_proxy: str = "localhost,127.0.0.1"

    # Docker Specific Settings
    docker_runtime: str = "runc"  # 'runc' for local dev, 'runsc' for gVisor production
    allow_runtime_fallback: bool = False  # Fail hard if requested secure runtime is unavailable!
    docker_network: str = "bridge"
    docker_host_port_start: int = 15000

    # Kubernetes Specific Settings
    k8s_namespace: str = "sandboxes"
    k8s_runtime_class: str = "gvisor"
    k8s_node_selector_key: str = "sandbox"
    k8s_node_selector_value: str = "true"
    k8s_use_pvc: bool = False

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
