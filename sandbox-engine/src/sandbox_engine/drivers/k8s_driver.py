from __future__ import annotations

import asyncio
import re
import time
from typing import Dict, List, Optional
import httpx
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from ..config import settings
from ..models import SandboxInstance, SandboxSpec
from .base import ContainerDriver


def normalize_k8s_mem(mem: str) -> str:
    """Normalize memory unit for Kubernetes (e.g. '512m' -> '512Mi', '1g' -> '1Gi')."""
    if not mem:
        return "512Mi"
    mem = mem.strip()
    if mem.endswith("m") and not mem.endswith("Mi"):
        return f"{mem[:-1]}Mi"
    elif mem.endswith("g") and not mem.endswith("Gi"):
        return f"{mem[:-1]}Gi"
    return mem


class KubernetesDriver(ContainerDriver):
    """
    Production-hardened Kubernetes driver for cloud-native clusters.
    Manages Pods on dedicated tainted nodes with gVisor RuntimeClass,
    RFC 1123 DNS-compliant naming, proper memory units, volume mounts, and dropped capabilities.
    """

    def __init__(self):
        self._initialized = False
        self._core_api: Optional[client.CoreV1Api] = None

    async def _init_client(self):
        if not self._initialized:
            try:
                config.load_incluster_config()
            except Exception:
                try:
                    await config.load_kube_config()
                except Exception:
                    pass
            self._core_api = client.CoreV1Api()
            self._initialized = True

    async def create_sandbox(self, spec: SandboxSpec) -> SandboxInstance:
        await self._init_client()
        namespace = settings.k8s_namespace
        now = time.time()

        # Sanitize Pod name to be RFC 1123 compliant (lowercase alphanumeric & hyphens only)
        clean_id = re.sub(r"[^a-z0-9\-]", "-", spec.spec_id.lower()).strip("-")[:40]
        pod_name = f"sandbox-{clean_id}"

        labels = {
            "sandbox.managed": "true",
            "sandbox.type": spec.sandbox_type,
            "sandbox.id": clean_id,
            "sandbox.created_at": str(int(now)),
            **spec.labels,
        }

        tolerations = [
            client.V1Toleration(
                key=settings.k8s_node_selector_key,
                operator="Exists",
                effect="NoSchedule",
            )
        ]
        node_selector = {settings.k8s_node_selector_key: settings.k8s_node_selector_value}

        # Container environment
        container_env = [
            client.V1EnvVar(name="WORKSPACE_DISK_QUOTA_MB", value=str(spec.disk_quota_mb))
        ]
        if spec.auth_token:
            container_env.append(client.V1EnvVar(name="SHELLCTL_AUTH_TOKEN", value=spec.auth_token))
        if settings.http_proxy:
            container_env.append(client.V1EnvVar(name="http_proxy", value=settings.http_proxy))
            container_env.append(client.V1EnvVar(name="HTTP_PROXY", value=settings.http_proxy))
        if settings.https_proxy:
            container_env.append(client.V1EnvVar(name="https_proxy", value=settings.https_proxy))
            container_env.append(client.V1EnvVar(name="HTTPS_PROXY", value=settings.https_proxy))
        if settings.no_proxy:
            container_env.append(client.V1EnvVar(name="no_proxy", value=settings.no_proxy))
            container_env.append(client.V1EnvVar(name="NO_PROXY", value=settings.no_proxy))

        container_env.extend([client.V1EnvVar(name=k, value=v) for k, v in spec.env.items()])

        # Normalize memory format (e.g. '512m' -> '512Mi')
        normalized_mem = normalize_k8s_mem(spec.mem_limit)

        # Volume mounts for stateful sandboxes
        pod_volumes = []
        volume_mounts = []
        if spec.workspace_path:
            vol_name = "workspace-volume"
            if settings.k8s_use_pvc:
                volume = client.V1Volume(
                    name=vol_name,
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=f"pvc-{clean_id}"
                    ),
                )
            else:
                volume = client.V1Volume(
                    name=vol_name,
                    empty_dir=client.V1EmptyDirVolumeSource(),
                )
            pod_volumes.append(volume)
            volume_mounts.append(client.V1VolumeMount(name=vol_name, mount_path="/workspace"))

        # Security context: drop all capabilities, no privilege escalation
        sec_context = client.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=client.V1Capabilities(drop=["ALL"]),
        )

        container = client.V1Container(
            name="sandbox-kernel",
            image=spec.image,
            ports=[client.V1ContainerPort(container_port=5004)],
            env=container_env,
            volume_mounts=volume_mounts if volume_mounts else None,
            security_context=sec_context,
            resources=client.V1ResourceRequirements(
                limits={
                    "cpu": str(spec.cpu_limit),
                    "memory": normalized_mem,
                    "ephemeral-storage": f"{spec.disk_quota_mb}Mi",
                },
                requests={"cpu": "100m", "memory": "128Mi"},
            ),
        )

        pod_spec = client.V1PodSpec(
            runtime_class_name=settings.k8s_runtime_class,
            tolerations=tolerations,
            node_selector=node_selector,
            automount_service_account_token=False,  # Security hardening
            containers=[container],
            volumes=pod_volumes if pod_volumes else None,
            restart_policy="Never",
        )

        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(name=pod_name, namespace=namespace, labels=labels),
            spec=pod_spec,
        )

        try:
            await self._core_api.create_namespaced_pod(namespace=namespace, body=pod)
        except ApiException as e:
            raise RuntimeError(f"Failed to create sandbox pod {pod_name}: {e}")

        # Wait for Pod IP
        pod_ip = None
        for _ in range(60):  # Retry up to 30 seconds
            await asyncio.sleep(0.5)
            curr = await self._core_api.read_namespaced_pod(name=pod_name, namespace=namespace)
            if curr.status.pod_ip:
                pod_ip = curr.status.pod_ip
                break

        if not pod_ip:
            await self.destroy_sandbox(pod_name)
            raise TimeoutError(f"Pod {pod_name} did not acquire an IP in time")

        endpoint = f"http://{pod_ip}:5004"
        return SandboxInstance(
            sandbox_id=pod_name,
            driver="k8s",
            endpoint=endpoint,
            created_at=now,
            status="running",
            metadata={"namespace": namespace, "pod_name": pod_name, "pod_ip": pod_ip, "created_at": now},
            auth_token=spec.auth_token,
        )

    async def destroy_sandbox(self, sandbox_id: str) -> None:
        await self._init_client()
        namespace = settings.k8s_namespace
        try:
            await self._core_api.delete_namespaced_pod(
                name=sandbox_id,
                namespace=namespace,
                grace_period_seconds=0,
            )
        except ApiException as e:
            if e.status != 404:
                pass

    async def get_sandbox_endpoint(self, sandbox_id: str) -> str:
        await self._init_client()
        namespace = settings.k8s_namespace
        pod = await self._core_api.read_namespaced_pod(name=sandbox_id, namespace=namespace)
        if not pod.status.pod_ip:
            raise RuntimeError(f"Pod {sandbox_id} does not have an IP")
        return f"http://{pod.status.pod_ip}:5004"

    async def list_sandboxes(self, labels: Dict[str, str] = None) -> List[SandboxInstance]:
        await self._init_client()
        namespace = settings.k8s_namespace
        selector_parts = ["sandbox.managed=true"]
        if labels:
            for k, v in labels.items():
                selector_parts.append(f"{k}={v}")
        label_selector = ",".join(selector_parts)

        pods = await self._core_api.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
        instances = []
        for pod in pods.items:
            endpoint = f"http://{pod.status.pod_ip}:5004" if pod.status.pod_ip else ""
            p_labels = pod.metadata.labels or {}
            try:
                created_ts = float(p_labels.get("sandbox.created_at", 0.0))
            except ValueError:
                created_ts = 0.0

            instances.append(
                SandboxInstance(
                    sandbox_id=pod.metadata.name,
                    driver="k8s",
                    endpoint=endpoint,
                    created_at=created_ts or time.time(),
                    status=pod.status.phase or "unknown",
                    metadata={"namespace": namespace, "pod_name": pod.metadata.name, "created_at": created_ts},
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
