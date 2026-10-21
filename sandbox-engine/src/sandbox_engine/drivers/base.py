from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List
from ..models import SandboxInstance, SandboxSpec


class ContainerDriver(ABC):
    """
    Abstract interface for managing sandbox runtimes.
    Implemented by DockerDriver (local/VM) and KubernetesDriver (cloud cluster).
    """

    @abstractmethod
    async def create_sandbox(self, spec: SandboxSpec) -> SandboxInstance:
        """Create and start a sandbox container/pod, returning its instance info and direct endpoint."""
        pass

    @abstractmethod
    async def destroy_sandbox(self, sandbox_id: str, remove_workspace: bool = False) -> None:
        """Forcefully destroy a container/pod; optionally remove host workspace (Docker)."""
        pass

    @abstractmethod
    async def get_sandbox_endpoint(self, sandbox_id: str) -> str:
        """Get the direct HTTP URL (e.g. http://<ip>:5004) for the sandbox."""
        pass

    @abstractmethod
    async def list_sandboxes(self, labels: Dict[str, str] = None) -> List[SandboxInstance]:
        """List active sandboxes matching labels."""
        pass

    @abstractmethod
    async def healthcheck(self, endpoint: str) -> bool:
        """Probe the sandbox's shellctl /health endpoint."""
        pass

    @abstractmethod
    async def is_alive(self, sandbox_id: str) -> bool:
        """Check if container or pod is actively running."""
        pass

    async def cleanup_orphaned_pvcs(self, active_workspace_keys: set[str], max_age_seconds: float = 86400.0) -> int:
        """Garbage collect unused or orphaned persistent volume claims (K8s driver only)."""
        return 0
