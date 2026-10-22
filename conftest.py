import os
from typing import Dict, List, Optional

import pytest

if "ENV" not in os.environ and "ENVIRONMENT" not in os.environ and "SANDBOX_ENVIRONMENT" not in os.environ:
    os.environ["ENV"] = "development"
    os.environ["SANDBOX_ENVIRONMENT"] = "development"

os.environ["SANDBOX_MAX_CONCURRENT_SESSIONS_PER_TENANT"] = "100"
# Avoid binding a fixed metrics port during repeated lifespan startup in tests.
os.environ["SANDBOX_ENABLE_PROMETHEUS_METRICS"] = "false"

from sandbox_engine.config import settings
from sandbox_engine.models import SandboxInstance, SandboxSpec

settings.enable_prometheus_metrics = False


class MockContainerDriver:
    async def create_sandbox(self, spec: SandboxSpec) -> SandboxInstance:
        return SandboxInstance(
            sandbox_id=f"mock-{spec.spec_id[:8]}",
            driver="docker",
            endpoint="http://127.0.0.1:59999",
            auth_token=spec.auth_token or "mock-token",
        )

    async def destroy_sandbox(self, sandbox_id: str, remove_workspace: bool = False) -> None:
        return None

    async def get_sandbox_endpoint(self, sandbox_id: str) -> str:
        return "http://127.0.0.1:59999"

    async def list_sandboxes(self, labels: Dict[str, str] = None) -> List[SandboxInstance]:
        return []

    async def healthcheck(self, endpoint: str, auth_token: Optional[str] = None) -> bool:
        return True

    async def is_alive(self, sandbox_id: str) -> bool:
        return True


def _docker_available() -> bool:
    host = os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock")
    if host.startswith("unix://"):
        sock_path = host.removeprefix("unix://")
        if not os.path.exists(sock_path) or not os.access(sock_path, os.R_OK | os.W_OK):
            return False
    try:
        import docker

        client = docker.from_env()
        client.ping()
        client.containers.list(limit=1)
        client.close()
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    if _docker_available():
        return
    skip = pytest.mark.skip(reason="Docker daemon not available")
    for item in items:
        if item.get_closest_marker("integration"):
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def isolated_sqlite_ledger(request, tmp_path, monkeypatch):
    if "postgres" in request.node.name or "test_postgres" in request.node.fspath.basename:
        return
    db_path = tmp_path / "sandbox_ledger.db"
    monkeypatch.setenv("SANDBOX_SQLITE_PATH", str(db_path))
    monkeypatch.setattr(settings, "sqlite_path", str(db_path), raising=False)
    monkeypatch.setenv("SANDBOX_POSTGRES_URL", "")
    monkeypatch.setattr(settings, "postgres_url", None, raising=False)


@pytest.fixture(autouse=True)
def mock_driver_unless_integration(request, monkeypatch):
    if request.node.get_closest_marker("integration"):
        return
    driver = MockContainerDriver()
    monkeypatch.setattr("sandbox_engine.drivers.get_driver", lambda: driver)
    monkeypatch.setattr("sandbox_engine.main.get_driver", lambda: driver)
