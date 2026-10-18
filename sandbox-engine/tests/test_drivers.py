import pytest
from sandbox_engine.drivers.base import ContainerDriver
from sandbox_engine.drivers.docker_driver import DockerDriver
from sandbox_engine.drivers.k8s_driver import KubernetesDriver
from sandbox_engine.drivers import get_driver
from sandbox_engine.config import settings


def test_driver_subclasses():
    assert issubclass(DockerDriver, ContainerDriver)
    assert issubclass(KubernetesDriver, ContainerDriver)


def test_get_driver_factory():
    original = settings.driver
    try:
        settings.driver = "docker"
        driver = get_driver()
        assert isinstance(driver, DockerDriver)

        settings.driver = "k8s"
        driver_k8s = get_driver()
        assert isinstance(driver_k8s, KubernetesDriver)
    finally:
        settings.driver = original
