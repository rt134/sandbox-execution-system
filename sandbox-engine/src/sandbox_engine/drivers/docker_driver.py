from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shutil
import time
from typing import Dict, List, Optional, Tuple
import docker
import httpx

from ..config import settings
from ..models import SandboxInstance, SandboxSpec
from .base import ContainerDriver

_SQUID_CONTAINER_NAMES = ("sandbox-squid", "squid")
_ENGINE_CONTAINER_NAMES = ("sandbox-engine",)


def _sandbox_network_name(spec_id: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]", "-", spec_id)[:32]
    digest = hashlib.sha256(spec_id.encode("utf-8")).hexdigest()[:8]
    return f"sandbox-net-{safe}-{digest}"


class DockerDriver(ContainerDriver):
    """
    Docker driver for local development and testing only.
    Production must use the Kubernetes driver with gVisor.
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

    def _connect_peers_to_network(self, network, net_name: str) -> None:
        for name in _SQUID_CONTAINER_NAMES + _ENGINE_CONTAINER_NAMES:
            try:
                peer = self.client.containers.get(name)
                peer.reload()
                if net_name not in peer.attrs.get("NetworkSettings", {}).get("Networks", {}):
                    network.connect(peer)
            except Exception:
                continue

    def _ensure_per_sandbox_network(self, spec_id: str) -> Tuple[str, object]:
        net_name = _sandbox_network_name(spec_id)
        try:
            network = self.client.networks.get(net_name)
        except Exception:
            network = self.client.networks.create(net_name, internal=True, driver="bridge")
        self._connect_peers_to_network(network, net_name)
        return net_name, network

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
            labels["sandbox.workspace_host_path"] = spec.workspace_path

        env = {
            "WORKSPACE_DISK_QUOTA_MB": str(spec.disk_quota_mb),
            "SHELLCTL_HOST": "auto",
            **spec.env,
        }
        if spec.auth_token:
            env["SHELLCTL_AUTH_TOKEN"] = spec.auth_token

        if settings.http_proxy:
            env["http_proxy"] = settings.http_proxy
            env["HTTP_PROXY"] = settings.http_proxy
        if settings.https_proxy:
            env["https_proxy"] = settings.https_proxy
            env["HTTPS_PROXY"] = settings.https_proxy
        if settings.no_proxy:
            env["no_proxy"] = settings.no_proxy
            env["NO_PROXY"] = settings.no_proxy

        per_sandbox_net_name: Optional[str] = None
        target_network = settings.docker_network
        ports = {"5004/tcp": ("127.0.0.1", None)}

        if settings.docker_network_internal:
            per_sandbox_net_name, _ = self._ensure_per_sandbox_network(spec.spec_id)
            target_network = per_sandbox_net_name
            ports = None
            labels["sandbox.network"] = per_sandbox_net_name

        runtime = settings.docker_runtime
        kwargs = {
            "image": spec.image,
            "detach": True,
            "labels": labels,
            "environment": env,
            "volumes": volumes,
            "network": target_network,
            "pids_limit": spec.pids_limit,
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"],
            "log_config": docker.types.LogConfig(type="json-file", config={"max-size": "10m", "max-file": "3"}),
        }
        if ports is not None:
            kwargs["ports"] = ports
        if spec.cpu_limit:
            kwargs["nano_cpus"] = int(spec.cpu_limit * 1e9)
        if spec.mem_limit:
            kwargs["mem_limit"] = spec.mem_limit
            kwargs["memswap_limit"] = spec.mem_limit

        container = None
        try:
            if runtime and runtime != "runc":
                try:
                    kwargs["runtime"] = runtime
                    container = self.client.containers.run(**kwargs)
                except Exception as e:
                    if not settings.allow_runtime_fallback:
                        raise RuntimeError(
                            f"Requested container runtime '{runtime}' failed to launch: {e}. "
                            "Refusing to fall back silently to insecure runtime."
                        ) from e
                    kwargs.pop("runtime", None)
                    container = self.client.containers.run(**kwargs)
            else:
                container = self.client.containers.run(**kwargs)
        except Exception:
            if container is not None:
                try:
                    container.remove(force=True)
                except Exception:
                    pass
            if per_sandbox_net_name:
                self._remove_network_if_unused(per_sandbox_net_name)
            raise

        container.reload()
        host_port = None
        if settings.docker_network_internal and per_sandbox_net_name:
            net_settings = container.attrs.get("NetworkSettings", {})
            ip = net_settings.get("Networks", {}).get(per_sandbox_net_name, {}).get("IPAddress") or net_settings.get(
                "IPAddress"
            )
            if not ip:
                self._sync_destroy_sandbox(container.id[:12], workspace_path=spec.workspace_path)
                raise RuntimeError(f"Failed to retrieve internal IP address for container {container.id}")
            endpoint = f"http://{ip}:5004"
        else:
            port_bindings = container.ports.get("5004/tcp")
            if not port_bindings:
                self._sync_destroy_sandbox(container.id[:12], workspace_path=spec.workspace_path)
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
                "network": per_sandbox_net_name,
                "workspace_path": spec.workspace_path,
                "created_at": now,
            },
            auth_token=spec.auth_token,
        )

    def _remove_network_if_unused(self, net_name: str) -> None:
        try:
            network = self.client.networks.get(net_name)
            network.reload()
            if len(network.attrs.get("Containers") or {}) == 0:
                network.remove()
        except Exception:
            pass

    async def destroy_sandbox(self, sandbox_id: str, remove_workspace: bool = False) -> None:
        await asyncio.to_thread(self._sync_destroy_sandbox, sandbox_id, None, remove_workspace)

    def _sync_destroy_sandbox(
        self, sandbox_id: str, workspace_path: Optional[str] = None, remove_workspace: bool = False
    ) -> None:
        net_name: Optional[str] = None
        ws_path = workspace_path
        try:
            container = self.client.containers.get(sandbox_id)
            c_labels = container.labels or {}
            net_name = c_labels.get("sandbox.network")
            if not ws_path:
                ws_path = c_labels.get("sandbox.workspace_host_path")
            container.remove(force=True)
        except docker.errors.NotFound:
            pass

        if net_name:
            self._remove_network_if_unused(net_name)

        if remove_workspace and ws_path and os.path.isdir(ws_path):
            shutil.rmtree(ws_path, ignore_errors=True)

    async def get_sandbox_endpoint(self, sandbox_id: str) -> str:
        return await asyncio.to_thread(self._sync_get_endpoint, sandbox_id)

    def _sync_get_endpoint(self, sandbox_id: str) -> str:
        container = self.client.containers.get(sandbox_id)
        c_labels = container.labels or {}
        net_name = c_labels.get("sandbox.network")
        if settings.docker_network_internal and net_name:
            net_settings = container.attrs.get("NetworkSettings", {})
            ip = net_settings.get("Networks", {}).get(net_name, {}).get("IPAddress") or net_settings.get("IPAddress")
            if ip:
                return f"http://{ip}:5004"
        port_bindings = container.ports.get("5004/tcp")
        if port_bindings:
            return f"http://127.0.0.1:{port_bindings[0]['HostPort']}"
        ip = container.attrs.get("NetworkSettings", {}).get("IPAddress")
        return f"http://{ip}:5004"

    async def list_sandboxes(self, labels: Dict[str, str] = None) -> List[SandboxInstance]:
        return await asyncio.to_thread(self._sync_list_sandboxes, labels)

    def _sync_list_sandboxes(self, labels: Dict[str, str] = None) -> List[SandboxInstance]:
        query_labels = ["sandbox.managed=true"]
        if labels:
            for k, v in labels.items():
                query_labels.append(f"{k}={v}")

        containers = self.client.containers.list(all=True, filters={"label": query_labels})
        instances = []
        for c in containers:
            c_labels = c.labels or {}
            net_name = c_labels.get("sandbox.network")
            if settings.docker_network_internal and net_name:
                net_settings = c.attrs.get("NetworkSettings", {})
                ip = net_settings.get("Networks", {}).get(net_name, {}).get("IPAddress") or net_settings.get(
                    "IPAddress"
                )
                endpoint = f"http://{ip}:5004" if ip else ""
            else:
                port_bindings = c.ports.get("5004/tcp") if c.ports else None
                endpoint = f"http://127.0.0.1:{port_bindings[0]['HostPort']}" if port_bindings else ""
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
                    metadata={"container_name": c.name, "created_at": created_ts, "network": net_name},
                )
            )
        return instances

    async def healthcheck(self, endpoint: str, auth_token: Optional[str] = None) -> bool:
        headers = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        async with httpx.AsyncClient(timeout=0.5, headers=headers) as client:
            for _ in range(20):
                try:
                    resp = await client.get(f"{endpoint}/health")
                    if resp.status_code == 200:
                        return True
                except Exception:
                    await asyncio.sleep(0.1)
        return False

    async def is_alive(self, sandbox_id: str) -> bool:
        return await asyncio.to_thread(self._sync_is_alive, sandbox_id)

    def _sync_is_alive(self, sandbox_id: str) -> bool:
        try:
            c = self.client.containers.get(sandbox_id)
            return c.status in ("running", "created")
        except Exception:
            return False
