from __future__ import annotations

import os
from typing import Literal, Optional
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

    # Pool Configurations (Stateless)
    warm_pool_size: int = 2
    warm_pool_acquire_timeout: float = 5.0

    # Stateful Session Configurations
    default_session_ttl: int = 300
    sweeper_interval_sec: float = 5.0
    workspaces_root: str = os.path.abspath(".workspaces")
    redis_url: Optional[str] = "redis://localhost:6379/0"
    sqlite_path: str = ".sandbox_ledger.db"

    # Docker Specific Settings
    docker_runtime: str = "runc"  # 'runc' for local dev, 'runsc' for gVisor production
    docker_network: str = "bridge"
    docker_host_port_start: int = 15000

    # Kubernetes Specific Settings
    k8s_namespace: str = "sandboxes"
    k8s_runtime_class: str = "gvisor"
    k8s_node_selector_key: str = "sandbox"
    k8s_node_selector_value: str = "true"

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
