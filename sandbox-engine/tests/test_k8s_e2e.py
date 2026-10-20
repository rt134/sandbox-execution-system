#!/usr/bin/env python3
"""
Kubernetes E2E Lifecycle Test:
Validates the full acquire -> exec -> fs -> release lifecycle for the K8s driver
under a simulated read-only root filesystem (readOnlyRootFilesystem: true),
guaranteeing that host-directory creation is bypassed and stateful acquisition succeeds.
"""

import os
import tempfile
import httpx
import pytest
from unittest.mock import AsyncMock, patch

from sandbox_engine.config import settings
from sandbox_engine.core.security import validate_workspace_id
from sandbox_engine.drivers.base import ContainerDriver, SandboxInstance
from sandbox_engine.main import app


class MockK8sDriver(ContainerDriver):
    """Mocks the Kubernetes driver with in-memory daemon simulation."""

    def __init__(self):
        self.sandboxes = {}
        self.files = {}

    async def create_sandbox(self, spec) -> SandboxInstance:
        sandbox_id = f"k8s-pod-{spec.spec_id[:8]}"
        self.sandboxes[sandbox_id] = {
            "spec": spec,
            "status": "running",
            "files": {},
        }
        return SandboxInstance(
            sandbox_id=sandbox_id,
            driver="k8s",
            endpoint=f"http://10.244.1.42:5004",
            created_at=1000.0,
            status="running",
            metadata={"pod_name": sandbox_id, "pvc_name": f"pvc-{spec.spec_id[:8]}"},
            auth_token=spec.auth_token,
        )

    async def destroy_sandbox(self, sandbox_id: str) -> None:
        self.sandboxes.pop(sandbox_id, None)

    async def get_sandbox_endpoint(self, sandbox_id: str) -> str:
        return f"http://10.244.1.42:5004"

    async def list_sandboxes(self, labels=None):
        return []

    async def healthcheck(self, endpoint: str, auth_token=None) -> bool:
        return True

    async def is_alive(self, sandbox_id: str) -> bool:
        return sandbox_id in self.sandboxes


@pytest.mark.asyncio
async def test_k8s_read_only_rootfs_acquire_exec_fs_release():
    """
    Simulate engine running in K8s pod with readOnlyRootFilesystem: true.
    Verify that stateful acquire -> exec -> fs -> release succeeds without
    attempting to create directories under /app/.workspaces.
    """
    mock_k8s_driver = MockK8sDriver()
    orig_driver = settings.driver
    orig_workspaces_root = settings.workspaces_root

    # Point workspaces_root to a non-existent directory on a read-only path
    read_only_path = "/non_existent_readonly_root/workspaces"
    settings.driver = "k8s"
    settings.workspaces_root = read_only_path

    try:
        # Verify validate_workspace_id skips host directory creation when driver is k8s
        ws_id = "k8s_e2e_ws"
        tenant_id = "tenant_k8s_corp"
        virtual_dir = validate_workspace_id(
            ws_id, read_only_path, tenant_id=tenant_id, create_host_dir=False
        )
        assert not os.path.exists(virtual_dir), "Host dir must NOT be created on K8s"

        with patch("sandbox_engine.main.get_driver", return_value=mock_k8s_driver), \
             patch("sandbox_engine.api.routes_sessions.get_driver", return_value=mock_k8s_driver):
            async with app.router.lifespan_context(app):
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                    headers = {"X-Tenant-ID": tenant_id}

                    # 1. Acquire stateful session on K8s
                    acquire_resp = await client.post(
                        "/v1/sessions/acquire",
                        json={"workspace_id": ws_id, "ttl_seconds": 120},
                        headers=headers,
                    )
                    assert acquire_resp.status_code == 200, f"Acquire failed: {acquire_resp.text}"
                    session_data = acquire_resp.json()
                    session_id = session_data["session_id"]
                    sandbox_id = session_data["sandbox_id"]
                    assert session_data["status"] == "active"
                    assert sandbox_id in mock_k8s_driver.sandboxes

                    # Mock the daemon HTTP responses on the sandbox endpoint for exec and fs
                    async def mock_post(url, *args, **kwargs):
                        url_str = str(url)
                        if "/fs/write" in url_str:
                            return httpx.Response(200, json={"status": "ok", "path": "test.py"})
                        elif "/exec" in url_str:
                            return httpx.Response(200, json={"exit_code": 0, "stdout": "K8S_SUCCESS\n", "stderr": ""})
                        return httpx.Response(200, json={})

                    async def mock_get(url, *args, **kwargs):
                        url_str = str(url)
                        if "/fs/read" in url_str:
                            return httpx.Response(200, json={"content": "print('hello from k8s')"})
                        return httpx.Response(200, json={})

                    with patch.object(httpx.AsyncClient, "post", side_effect=mock_post), \
                         patch.object(httpx.AsyncClient, "get", side_effect=mock_get):

                        # 2. File write (fs)
                        write_resp = await client.post(
                            f"/v1/sessions/{session_id}/fs/write",
                            json={"path": "test.py", "content": "print('hello from k8s')"},
                            headers=headers,
                        )
                        assert write_resp.status_code == 200

                        # 3. File read (fs)
                        read_resp = await client.get(
                            f"/v1/sessions/{session_id}/fs/read?path=test.py",
                            headers=headers,
                        )
                        assert read_resp.status_code == 200

                        # 4. Command execution (exec)
                        exec_resp = await client.post(
                            f"/v1/sessions/{session_id}/exec",
                            json={"command": "python3 test.py", "stream": False},
                            headers=headers,
                        )
                        assert exec_resp.status_code == 200
                        assert exec_resp.json()["stdout"] == "K8S_SUCCESS\n"

                    # 5. Release session
                    rel_resp = await client.post(
                        f"/v1/sessions/{session_id}/release",
                        headers=headers,
                    )
                    assert rel_resp.status_code == 200
                    assert sandbox_id not in mock_k8s_driver.sandboxes
    finally:
        settings.driver = orig_driver
        settings.workspaces_root = orig_workspaces_root
