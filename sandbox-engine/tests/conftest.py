import os
from typing import Dict, List, Optional
import pytest

from sandbox_engine.models import SandboxInstance, SandboxSpec

# Set development environment for automated test runs unless explicitly set
if "ENV" not in os.environ and "ENVIRONMENT" not in os.environ and "SANDBOX_ENVIRONMENT" not in os.environ:
    os.environ["ENV"] = "development"
    os.environ["SANDBOX_ENVIRONMENT"] = "development"

os.environ["SANDBOX_MAX_CONCURRENT_SESSIONS_PER_TENANT"] = "100"
os.environ["SANDBOX_ENABLE_PROMETHEUS_METRICS"] = "false"


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


@pytest.fixture(autouse=True)
def mock_driver_for_app(monkeypatch):
    """Avoid requiring a live Docker daemon during API integration tests."""
    driver = MockContainerDriver()
    monkeypatch.setattr("sandbox_engine.drivers.get_driver", lambda: driver)
    monkeypatch.setattr("sandbox_engine.main.get_driver", lambda: driver)
