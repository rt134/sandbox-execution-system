from __future__ import annotations

import asyncio
import os
import time
from typing import Dict, List, Optional
import docker
import httpx

from ..config import settings
from ..models import SandboxInstance, SandboxSpec
from .base import ContainerDriver


class DockerDriver(ContainerDriver):
    """
    Production-hardened Docker driver for local macOS/Linux and single-node instances.
    Enforces cgroup limits, privilege dropping, proxy routing, and runtime checks.
    """

    def __init__(self):
        socket_paths = [
            os.path.expanduser("~/.docker/run/docker.sock"),
            "/var/run/docker.sock",
        ]
        base_url = None
        for path in socket_paths:
            if os.path.exists(path):
                base_url = f"unix://{path}"
                break

        self.client = docker.DockerClient(base_url=base_url) if base_url else docker.from_env()

    async def create_sandbox(self, spec: SandboxSpec) -> SandboxInstance:
        return await asyncio.to_thread(self._sync_create_sandbox, spec)

    def _sync_create_sandbox(self, spec: SandboxSpec) -> SandboxInstance:
        now = time.time()
        labels = {
            "sandbox.managed": "true",
            "sandbox.type": spec.sandbox_type,
            "sandbox.id": spec.spec_id,
            "sandbox.created_at": str(int(now)),
            **spec.labels,
        }

        volumes = {}
        if spec.workspace_path:
            os.makedirs(spec.workspace_path, exist_ok=True)
            volumes[spec.workspace_path] = {"bind": "/workspace", "mode": "rw"}

        # Dynamic free port allocation: bind strictly to 127.0.0.1
        ports = {"5004/tcp": ("127.0.0.1", None)}

        env = {
            "WORKSPACE_DISK_QUOTA_MB": str(spec.disk_quota_mb),
            **spec.env,
        }
        if spec.auth_token:
            env["SHELLCTL_AUTH_TOKEN"] = spec.auth_token

        # Egress Proxy Configuration
        if settings.http_proxy:
            env["http_proxy"] = settings.http_proxy
            env["HTTP_PROXY"] = settings.http_proxy
        if settings.https_proxy:
            env["https_proxy"] = settings.https_proxy
            env["HTTPS_PROXY"] = settings.https_proxy
        if settings.no_proxy:
            env["no_proxy"] = settings.no_proxy
            env["NO_PROXY"] = settings.no_proxy

        runtime = settings.docker_runtime
        kwargs = {
            "image": spec.image,
            "detach": True,
            "labels": labels,
            "environment": env,
            "volumes": volumes,
            "ports": ports,
            "network": settings.docker_network,
            "pids_limit": spec.pids_limit,
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"],
        }
        if spec.cpu_limit:
            kwargs["nano_cpus"] = int(spec.cpu_limit * 1e9)
        if spec.mem_limit:
            kwargs["mem_limit"] = spec.mem_limit
            kwargs["memswap_limit"] = spec.mem_limit

        # Handle runtime selection: fail hard if requested runtime unavailable
        if runtime and runtime != "runc":
            try:
                kwargs["runtime"] = runtime
                container = self.client.containers.run(**kwargs)
            except Exception as e:
                if not settings.allow_runtime_fallback:
                    raise RuntimeError(
                        f"Requested container runtime '{runtime}' failed to launch: {e}. "
                        "Refusing to fall back silently to insecure runtime."
                    )
                kwargs.pop("runtime", None)
                container = self.client.containers.run(**kwargs)
        else:
            container = self.client.containers.run(**kwargs)

        container.reload()
        port_bindings = container.ports.get("5004/tcp")
        if not port_bindings:
            raise RuntimeError(f"Failed to retrieve port binding for container {container.id}")

        host_port = port_bindings[0]["HostPort"]
        endpoint = f"http://127.0.0.1:{host_port}"

        return SandboxInstance(
            sandbox_id=container.id[:12],
            driver="docker",
            endpoint=endpoint,
            created_at=now,
            status="running",
            metadata={
                "container_name": container.name,
                "host_port": host_port,
                "created_at": now,
            },
            auth_token=spec.auth_token,
        )

    async def destroy_sandbox(self, sandbox_id: str) -> None:
        await asyncio.to_thread(self._sync_destroy_sandbox, sandbox_id)

    def _sync_destroy_sandbox(self, sandbox_id: str) -> None:
        try:
            container = self.client.containers.get(sandbox_id)
            container.remove(force=True)
        except docker.errors.NotFound:
            pass

    async def get_sandbox_endpoint(self, sandbox_id: str) -> str:
        return await asyncio.to_thread(self._sync_get_endpoint, sandbox_id)

    def _sync_get_endpoint(self, sandbox_id: str) -> str:
        container = self.client.containers.get(sandbox_id)
        port_bindings = container.ports.get("5004/tcp")
        if port_bindings:
            return f"http://127.0.0.1:{port_bindings[0]['HostPort']}"
        ip = container.attrs["NetworkSettings"]["IPAddress"]
        return f"http://{ip}:5004"

    async def list_sandboxes(self, labels: Dict[str, str] = None) -> List[SandboxInstance]:
        return await asyncio.to_thread(self._sync_list_sandboxes, labels)

    def _sync_list_sandboxes(self, labels: Dict[str, str] = None) -> List[SandboxInstance]:
        query_labels = ["sandbox.managed=true"]
        if labels:
            for k, v in labels.items():
                query_labels.append(f"{k}={v}")

        containers = self.client.containers.list(filters={"label": query_labels})
        instances = []
        for c in containers:
            port_bindings = c.ports.get("5004/tcp")
            endpoint = f"http://127.0.0.1:{port_bindings[0]['HostPort']}" if port_bindings else ""
            c_labels = c.labels or {}
            try:
                created_ts = float(c_labels.get("sandbox.created_at", 0.0))
            except ValueError:
                created_ts = 0.0

            instances.append(
                SandboxInstance(
                    sandbox_id=c.id[:12],
                    driver="docker",
                    endpoint=endpoint,
                    created_at=created_ts or time.time(),
                    status=c.status,
                    metadata={"container_name": c.name, "created_at": created_ts},
                )
            )
        return instances

    async def healthcheck(self, endpoint: str, auth_token: Optional[str] = None) -> bool:
        """Poll the sandbox endpoint until /health responds 200 OK."""
        headers = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        async with httpx.AsyncClient(timeout=0.5, headers=headers) as client:
            for _ in range(20):  # Retry up to 2 seconds
                try:
                    resp = await client.get(f"{endpoint}/health")
                    if resp.status_code == 200:
                        return True
                except Exception:
                    await asyncio.sleep(0.1)
        return False
