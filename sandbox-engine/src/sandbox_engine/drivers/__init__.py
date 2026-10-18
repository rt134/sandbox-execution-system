from __future__ import annotations

from ..config import settings
from .base import ContainerDriver
from .docker_driver import DockerDriver
from .k8s_driver import KubernetesDriver


def get_driver() -> ContainerDriver:
    """Factory to instantiate the configured container driver."""
    if settings.driver == "k8s":
        return KubernetesDriver()
    return DockerDriver()


__all__ = ["ContainerDriver", "DockerDriver", "KubernetesDriver", "get_driver"]
