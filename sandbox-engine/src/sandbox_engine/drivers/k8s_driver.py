from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import socket
import time
import uuid
from typing import Dict, List, Optional, Set
import httpx
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from ..config import settings
from ..models import SandboxInstance, SandboxSpec
from .base import ContainerDriver

logger = logging.getLogger(__name__)


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
                await config.load_kube_config()
            self._core_api = client.CoreV1Api()
            self._initialized = True

    async def create_sandbox(self, spec: SandboxSpec) -> SandboxInstance:
        await self._init_client()
        namespace = settings.k8s_namespace
        now = time.time()

        # Sanitize Pod name to be RFC 1123 compliant (lowercase alphanumeric & hyphens only)
        # Resolves M9: Append random entropy to prevent pod name collision with terminating pods or fast session reuse
        clean_id = re.sub(r"[^a-z0-9\-]", "-", spec.spec_id.lower()).strip("-")[:32]
        rand_suffix = uuid.uuid4().hex[:6]
        pod_name = f"sandbox-{clean_id}-{rand_suffix}"

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

        # Resolves H2: Sandboxes have no direct DNS egress to prevent DNS tunneling exfiltration.
        # Inject host_aliases for Squid proxy hostname so sandboxes resolve the proxy via /etc/hosts
        host_aliases = []
        if settings.http_proxy:
            match = re.search(r"https?://([^:/]+)", settings.http_proxy)
            if match:
                proxy_host = match.group(1)
                if not re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", proxy_host):
                    try:
                        resolved_ip = socket.gethostbyname(proxy_host)
                        host_aliases.append(
                            client.V1HostAlias(
                                ip=resolved_ip,
                                hostnames=[proxy_host],
                            )
                        )
                    except Exception as e:
                        logger.warning("Could not resolve proxy host '%s' for host_aliases: %s", proxy_host, e)

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
        tmp_limit = f"{min(spec.disk_quota_mb, 512)}Mi"
        pod_volumes = [
            client.V1Volume(name="tmp-dir", empty_dir=client.V1EmptyDirVolumeSource(size_limit=tmp_limit))
        ]
        volume_mounts = [
            client.V1VolumeMount(name="tmp-dir", mount_path="/tmp")
        ]
        if spec.workspace_path:
            vol_name = "workspace-volume"
            workspace_id = spec.labels.get("sandbox.workspace_id") or clean_id
            tenant_id = spec.labels.get("sandbox.tenant_id") or "default"

            # Cryptographic collision-proof hashing: scoped to tenant_id and workspace_id
            scope_key = f"{tenant_id}:{workspace_id}"
            scope_hash = hashlib.sha256(scope_key.encode("utf-8")).hexdigest()[:24]
            pvc_name = f"pvc-{scope_hash}"

            if settings.k8s_use_pvc:
                # Key PVC by (tenant_id, workspace_id) hash and ensure it exists dynamically
                await self._ensure_workspace_pvc(namespace, pvc_name, workspace_id, tenant_id, spec.disk_quota_mb)
                volume = client.V1Volume(
                    name=vol_name,
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=pvc_name
                    ),
                )
            else:
                volume = client.V1Volume(
                    name=vol_name,
                    empty_dir=client.V1EmptyDirVolumeSource(size_limit=f"{spec.disk_quota_mb}Mi"),
                )
            pod_volumes.append(volume)
            volume_mounts.append(client.V1VolumeMount(name=vol_name, mount_path="/workspace"))

        # Hardened Security Context: drop all capabilities, no privilege escalation, read-only root, non-root user
        sec_context = client.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=client.V1Capabilities(drop=["ALL"]),
            read_only_root_filesystem=True,
            run_as_non_root=True,
            run_as_user=1000,
            run_as_group=1000,
            seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
        )

        # Resolves H1: Pin by digest uses IfNotPresent; mutable tags (:latest) pull Always to avoid stale-cache risks
        pull_policy = "IfNotPresent" if ("@" in spec.image or not spec.image.endswith(":latest")) else "Always"

        container = client.V1Container(
            name="sandbox-kernel",
            image=spec.image,
            image_pull_policy=pull_policy,
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
                requests={
                    "cpu": str(spec.cpu_limit),
                    "memory": normalized_mem,
                    "ephemeral-storage": f"{spec.disk_quota_mb}Mi",
                },
            ),
        )

        pod_spec_kwargs = dict(
            runtime_class_name=settings.k8s_runtime_class,
            tolerations=tolerations,
            node_selector=node_selector,
            automount_service_account_token=False,
            enable_service_links=False,
            security_context=client.V1PodSecurityContext(fs_group=1000, run_as_group=1000),
            containers=[container],
            volumes=pod_volumes if pod_volumes else None,
            restart_policy="Never",
            termination_grace_period_seconds=5,
        )
        if host_aliases:
            pod_spec_kwargs["host_aliases"] = host_aliases
        if spec.sandbox_type == "stateful":
            pod_spec_kwargs["active_deadline_seconds"] = settings.max_session_lifetime
        pod_spec = client.V1PodSpec(**pod_spec_kwargs)

        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(name=pod_name, namespace=namespace, labels=labels),
            spec=pod_spec,
        )

        try:
            await self._core_api.create_namespaced_pod(namespace=namespace, body=pod)
        except ApiException as e:
            raise RuntimeError(f"Failed to create sandbox pod {pod_name}: {e}")

        # Wait for Pod IP and Pod Ready condition (handles cold starts and image pulls up to 60s)
        pod_ip = None
        for _ in range(120):  # Retry up to 60 seconds
            await asyncio.sleep(0.5)
            curr = await self._core_api.read_namespaced_pod(name=pod_name, namespace=namespace)
            if curr.status and curr.status.pod_ip:
                pod_ip = curr.status.pod_ip
                # Check for Ready condition
                is_ready = False
                if curr.status.conditions:
                    for cond in curr.status.conditions:
                        if cond.type == "Ready" and cond.status == "True":
                            is_ready = True
                            break
                if not is_ready and curr.status.container_statuses:
                    is_ready = any(cs.ready for cs in curr.status.container_statuses if cs.name == "sandbox-kernel")

                # If ready or phase is Running with IP
                if is_ready:
                    break

        if not pod_ip:
            await self.destroy_sandbox(pod_name)
            raise TimeoutError(f"Pod {pod_name} did not acquire an IP or become Ready in time")

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

    async def destroy_sandbox(self, sandbox_id: str, remove_workspace: bool = False) -> None:
        await self._init_client()
        namespace = settings.k8s_namespace
        try:
            await self._core_api.delete_namespaced_pod(
                name=sandbox_id,
                namespace=namespace,
                grace_period_seconds=5,
            )
        except ApiException as e:
            if e.status != 404:
                logger.error("Failed to delete pod %s: %s", sandbox_id, e)
                raise

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
            for _ in range(50):  # Retries up to 5s to tolerate cold starts
                try:
                    resp = await client.get(f"{endpoint}/health")
                    if resp.status_code == 200:
                        return True
                except Exception:
                    await asyncio.sleep(0.1)
        return False

    async def _ensure_workspace_pvc(
        self, namespace: str, pvc_name: str, workspace_id: str, tenant_id: str, quota_mb: int
    ) -> None:
        """
        Dynamically provisions a Kubernetes PersistentVolumeClaim keyed by (tenant_id, workspace_id)
        if it does not already exist.
        """
        await self._init_client()
        now_label = str(int(time.time()))
        try:
            existing = await self._core_api.read_namespaced_persistent_volume_claim(
                name=pvc_name,
                namespace=namespace,
            )
            labels = dict(existing.metadata.labels or {})
            labels["sandbox.last_used"] = now_label
            patch = {"metadata": {"labels": labels}}
            await self._core_api.patch_namespaced_persistent_volume_claim(
                name=pvc_name, namespace=namespace, body=patch
            )
            return
        except ApiException as e:
            if e.status != 404:
                raise

        # PVC not found, dynamically create it
        pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(
                name=pvc_name,
                namespace=namespace,
                labels={
                    "sandbox.managed": "true",
                    "sandbox.tenant_id": str(tenant_id),
                    "sandbox.workspace_id": str(workspace_id),
                    "sandbox.created_at": now_label,
                    "sandbox.last_used": now_label,
                },
            ),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                resources=client.V1ResourceRequirements(
                    requests={"storage": f"{quota_mb}Mi"}
                ),
                storage_class_name=settings.k8s_storage_class,
            ),
        )
        try:
            await self._core_api.create_namespaced_persistent_volume_claim(
                namespace=namespace,
                body=pvc,
            )
        except ApiException as create_err:
            if create_err.status != 409:  # Conflict / already created concurrently is fine
                raise

    async def is_alive(self, sandbox_id: str) -> bool:
        """Checks if Pod exists and is running."""
        await self._init_client()
        try:
            pod = await self._core_api.read_namespaced_pod(name=sandbox_id, namespace=settings.k8s_namespace)
            return pod.status.phase in ("Running", "Pending")
        except ApiException as e:
            if e.status == 404:
                return False
            raise

    async def cleanup_orphaned_pvcs(self, active_workspace_keys: Set[str], max_age_seconds: Optional[float] = None) -> int:
        """
        Garbage collect PVCs that are not associated with any active workspace
        and not mounted by any running pod, preventing ResourceQuota exhaustion (150 PVC cap).
        Resolves M8: Uses UID and resourceVersion preconditions to prevent deleting newly re-acquired workspaces.
        """
        await self._init_client()
        namespace = settings.k8s_namespace
        deleted_count = 0
        if max_age_seconds is None:
            max_age_seconds = settings.workspace_retention_seconds
        try:
            pvcs = await self._core_api.list_namespaced_persistent_volume_claim(
                namespace=namespace,
                label_selector="sandbox.managed=true",
            )
            running_pods = await self._core_api.list_namespaced_pod(
                namespace=namespace,
                label_selector="sandbox.managed=true",
            )
            mounted_pvcs = set()
            for pod in running_pods.items:
                if pod.spec and pod.spec.volumes:
                    for vol in pod.spec.volumes:
                        if vol.persistent_volume_claim:
                            mounted_pvcs.add(vol.persistent_volume_claim.claim_name)

            now = time.time()
            for pvc in pvcs.items:
                pvc_name = pvc.metadata.name
                if pvc_name in mounted_pvcs:
                    continue  # In active use by a running pod

                labels = pvc.metadata.labels or {}
                tenant_id = labels.get("sandbox.tenant_id", "default")
                workspace_id = labels.get("sandbox.workspace_id", "")
                scope_key = f"{tenant_id}:{workspace_id}"

                if scope_key in active_workspace_keys:
                    continue  # Active session exists for this workspace

                last_used = float(labels.get("sandbox.last_used", labels.get("sandbox.created_at", 0.0)))
                if last_used > 0 and (now - last_used) > max_age_seconds:
                    logger.info(f"🧹 [PVC GC] Deleting orphaned PVC '{pvc_name}' for workspace '{scope_key}'")
                    try:
                        await self._core_api.delete_namespaced_persistent_volume_claim(
                            name=pvc_name,
                            namespace=namespace,
                            body=client.V1DeleteOptions(
                                preconditions=client.V1Preconditions(
                                    uid=pvc.metadata.uid,
                                    resource_version=pvc.metadata.resource_version,
                                )
                            ),
                        )
                        deleted_count += 1
                    except ApiException as e:
                        if e.status != 404:
                            logger.error(f"Failed to delete PVC {pvc_name}: {e}")
        except Exception as e:
            logger.error(f"Error during PVC garbage collection: {e}")
        return deleted_count
