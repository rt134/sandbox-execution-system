from __future__ import annotations

import asyncio
from typing import Dict, List, Optional
import httpx
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from ..config import settings
from ..models import SandboxInstance, SandboxSpec
from .base import ContainerDriver


class KubernetesDriver(ContainerDriver):
    """
    Kubernetes driver for cloud-native production clusters.
    Manages Pods on dedicated tainted nodes with gVisor RuntimeClass.
    """

    def __init__(self):
        self._initialized = False
        self._core_api: Optional[client.CoreV1Api] = None

    async def _init_client(self):
        if not self._initialized:
            try:
                # Try in-cluster config first (production pod)
                config.load_incluster_config()
            except Exception:
                try:
                    # Fallback to local kubeconfig (~/.kube/config)
                    await config.load_kube_config()
                except Exception as e:
                    # Allow initialization even without live cluster (for unit testing/offline)
                    pass
            self._core_api = client.CoreV1Api()
            self._initialized = True

    async def create_sandbox(self, spec: SandboxSpec) -> SandboxInstance:
        await self._init_client()
        namespace = settings.k8s_namespace
        pod_name = f"sandbox-{spec.spec_id[:16]}"

        labels = {
            "sandbox.managed": "true",
            "sandbox.type": spec.sandbox_type,
            "sandbox.id": spec.spec_id,
            **spec.labels,
        }

        # Tolerations for tainted node pool (sandbox=true:NoSchedule)
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
        container_env.extend([client.V1EnvVar(name=k, value=v) for k, v in spec.env.items()])

        # Container spec
        container = client.V1Container(
            name="sandbox-kernel",
            image=spec.image,
            ports=[client.V1ContainerPort(container_port=5004)],
            env=container_env,
            resources=client.V1ResourceRequirements(
                limits={
                    "cpu": str(spec.cpu_limit),
                    "memory": spec.mem_limit,
                    "ephemeral-storage": f"{spec.disk_quota_mb}Mi",
                },
                requests={"cpu": "100m", "memory": "128m"},
            ),
        )

        pod_spec = client.V1PodSpec(
            runtime_class_name=settings.k8s_runtime_class,
            tolerations=tolerations,
            node_selector=node_selector,
            automount_service_account_token=False,  # Security hardening
            containers=[container],
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
            status="running",
            metadata={"namespace": namespace, "pod_name": pod_name, "pod_ip": pod_ip},
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
            instances.append(
                SandboxInstance(
                    sandbox_id=pod.metadata.name,
                    driver="k8s",
                    endpoint=endpoint,
                    status=pod.status.phase or "unknown",
                    metadata={"namespace": namespace, "pod_name": pod.metadata.name},
                )
            )
        return instances

    async def healthcheck(self, endpoint: str) -> bool:
        async with httpx.AsyncClient(timeout=0.5) as client:
            for _ in range(20):
                try:
                    resp = await client.get(f"{endpoint}/health")
                    if resp.status_code == 200:
                        return True
                except Exception:
                    await asyncio.sleep(0.1)
        return False
