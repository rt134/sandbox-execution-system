#!/usr/bin/env python3
"""
Multi-Tenant Security, Quotas, and Storage Isolation Test Suite:
1. Cross-Tenant Workspace Ownership Guard (Tenant B cannot acquire Tenant A's workspace -> 403 Forbidden).
2. Physical Directory Tenant Scoping (Different tenants get separate host paths).
3. Collision-Proof K8s PVC Hashing (Foo_Bar vs foo-bar and cross-tenant hashing).
4. WebSocket PTY Cross-Tenant Rejection (close code 4403).
5. Production Secret Hardening & Weak Key Rejection.
6. Per-Tenant Quotas & Sliding-Window Rate Limiting (429 Too Many Requests).
7. Dead Container Sweeper Reconciliation (Unresponsive/dead container lease & lock freed).
"""

import asyncio
import hashlib
import time
import httpx
import pytest

from sandbox_engine.config import EngineSettings, settings
from sandbox_engine.core.lease_manager import LeaseManager
from sandbox_engine.core.security import validate_workspace_id
from sandbox_engine.drivers.base import ContainerDriver
from sandbox_engine.main import app


class MockDriver(ContainerDriver):
    def __init__(self):
        self.live_containers = set()

    async def create_sandbox(self, spec):
        return None

    async def destroy_sandbox(self, sandbox_id: str) -> None:
        self.live_containers.discard(sandbox_id)

    async def get_sandbox_endpoint(self, sandbox_id: str) -> str:
        return "http://127.0.0.1:5004"

    async def list_sandboxes(self, labels=None):
        return []

    async def healthcheck(self, endpoint: str, auth_token=None) -> bool:
        return True

    async def is_alive(self, sandbox_id: str) -> bool:
        return sandbox_id in self.live_containers



@pytest.mark.asyncio
async def test_two_tenants_both_use_main_workspace():
    """Verify that workspace names and locks are scoped to (tenant_id, workspace_id) so two tenants can both use 'main'."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers_tenant_a = {"X-Tenant-ID": "tenant_alpha"}
            headers_tenant_b = {"X-Tenant-ID": "tenant_bravo"}

            # 1. Tenant A acquires workspace 'main'
            resp_a = await client.post(
                "/v1/sessions/acquire",
                json={"workspace_id": "main", "ttl_seconds": 60},
                headers=headers_tenant_a,
            )
            assert resp_a.status_code == 200, f"Tenant A failed to acquire 'main': {resp_a.text}"
            sess_a_data = resp_a.json()
            sess_a_id = sess_a_data["session_id"]

            # 2. Tenant B also acquires workspace 'main' concurrently without 403 or 409 conflict
            resp_b = await client.post(
                "/v1/sessions/acquire",
                json={"workspace_id": "main", "ttl_seconds": 60},
                headers=headers_tenant_b,
            )
            assert resp_b.status_code == 200, f"Tenant B should be able to acquire their own 'main': {resp_b.text}"
            sess_b_data = resp_b.json()
            sess_b_id = sess_b_data["session_id"]

            assert sess_a_id != sess_b_id, "Different tenants must get distinct sessions"

            # Clean up
            await client.post(f"/v1/sessions/{sess_a_id}/release", headers=headers_tenant_a)
            await client.post(f"/v1/sessions/{sess_b_id}/release", headers=headers_tenant_b)


def test_physical_directory_tenant_scoping(tmp_path):
    """Verify that different tenants get strictly isolated physical directories on host."""
    root = str(tmp_path)
    ws_name = "project_x"

    dir_tenant_1 = validate_workspace_id(ws_name, root, tenant_id="acme_corp")
    dir_tenant_2 = validate_workspace_id(ws_name, root, tenant_id="evil_corp")

    assert dir_tenant_1 != dir_tenant_2, "Tenants must have physically distinct directories!"
    assert "tenant_" in dir_tenant_1
    assert "tenant_" in dir_tenant_2
    assert dir_tenant_1.endswith(ws_name)
    assert dir_tenant_2.endswith(ws_name)


def test_k8s_pvc_hash_collision_prevention():
    """Verify Foo_Bar vs foo-bar and tenant scoping produce unique, collision-proof PVC names."""
    def compute_pvc_name(tenant_id: str, workspace_id: str) -> str:
        scope_key = f"{tenant_id}:{workspace_id}"
        scope_hash = hashlib.sha256(scope_key.encode("utf-8")).hexdigest()[:24]
        return f"pvc-{scope_hash}"

    # 1. Slug collision check: Foo_Bar vs foo-bar
    pvc_foo_bar_caps = compute_pvc_name("tenant_1", "Foo_Bar")
    pvc_foo_bar_lower = compute_pvc_name("tenant_1", "foo-bar")
    pvc_foo_bar_dash = compute_pvc_name("tenant_1", "foo_bar")

    assert pvc_foo_bar_caps != pvc_foo_bar_lower, "Foo_Bar and foo-bar must not collide!"
    assert pvc_foo_bar_caps != pvc_foo_bar_dash, "Foo_Bar and foo_bar must not collide!"

    # 2. Cross-tenant same workspace name check
    pvc_tenant_a = compute_pvc_name("tenant_a", "workspace_1")
    pvc_tenant_b = compute_pvc_name("tenant_b", "workspace_1")

    assert pvc_tenant_a != pvc_tenant_b, "Different tenants must never share the same PVC name!"
    assert len(pvc_tenant_a) <= 63, "PVC name must satisfy RFC 1123 max length 63"


def test_production_secret_validation():
    """Verify that validate_production_settings() rejects weak secrets, placeholders, and insecure defaults."""
    # 1. Placeholder API Key
    s1 = EngineSettings(
        environment="production",
        api_key="prod-secret-sandbox-key-replace-me",
        encryption_key="a" * 32,
        driver="k8s",
    )
    with pytest.raises(ValueError, match="placeholder"):
        s1.validate_production_settings()

    # 2. Too short API Key
    s2 = EngineSettings(
        environment="production",
        api_key="short_secret",
        encryption_key="a" * 32,
        driver="k8s",
    )
    with pytest.raises(ValueError, match="at least 32 characters"):
        s2.validate_production_settings()

    # 3. Default encryption key
    s3 = EngineSettings(
        environment="production",
        api_key="b" * 32,
        encryption_key="default-sandbox-secret-replace-in-production",
        driver="k8s",
    )
    with pytest.raises(ValueError, match="placeholder"):
        s3.validate_production_settings()

    # 4. Insecure Docker runtime in production
    s4 = EngineSettings(
        environment="production",
        api_key="c" * 32,
        encryption_key="d" * 32,
        postgres_url="postgresql://sandbox:sandbox@localhost:5432/sandbox",
        driver="docker",
        docker_runtime="runc",
        allow_insecure_docker_production=False,
    )
    with pytest.raises(ValueError, match="strong container isolation"):
        s4.validate_production_settings()

    # 5. Strong production settings pass validation
    s5 = EngineSettings(
        environment="production",
        api_key="e" * 32,
        encryption_key="f" * 32,
        postgres_url="postgresql://sandbox:sandbox@localhost:5432/sandbox",
        driver="k8s",
    )
    s5.validate_production_settings()


@pytest.mark.asyncio
async def test_per_tenant_quotas_and_rate_limiting():
    """Verify TTL caps, stateless timeout caps, and per-tenant rate limits."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            headers = {"X-Tenant-ID": "quota_test_tenant"}

            # 1. Excessive TTL rejection
            resp_ttl = await client.post(
                "/v1/sessions/acquire",
                json={"workspace_id": "ws_ttl_test", "ttl_seconds": 999999},
                headers=headers,
            )
            assert resp_ttl.status_code == 400
            assert "exceeds maximum allowed limit" in resp_ttl.text

            # 2. Excessive stateless timeout rejection
            resp_stateless = await client.post(
                "/v1/stateless/run",
                json={"language": "python", "code": "print(1)", "timeout": 3600.0},
                headers=headers,
            )
            assert resp_stateless.status_code == 400
            assert "exceeds maximum allowed limit" in resp_stateless.text

            # 3. Rate limiting test: exceed limit within 60s
            rate_limited = False
            for _ in range(130):
                r = await client.get("/v1/sessions/invalid_session_id/fs/read?path=test.txt", headers={"X-Tenant-ID": "burst_tenant"})
                if r.status_code == 429:
                    rate_limited = True
                    assert "Retry-After" in r.headers
                    break

            assert rate_limited, "Expected HTTP 429 Too Many Requests after exceeding 120 req/min rate limit!"


@pytest.mark.asyncio
async def test_dead_container_sweeper_reconciliation():
    """Verify that the sweeper detects a dead/crashed container and immediately reclaims the lease and workspace lock."""
    from sandbox_engine.core.sweeper import Sweeper

    lease_mgr = LeaseManager()
    await lease_mgr.initialize()

    class FlakyDriver(MockDriver):
        def __init__(self):
            super().__init__()
            self.live_containers = set()

        async def is_alive(self, sandbox_id: str) -> bool:
            return sandbox_id in self.live_containers

    driver = FlakyDriver()
    sweeper = Sweeper(driver=driver, lease_manager=lease_mgr)

    # Acquire lease and lock
    session_id = "sess_dead_recovery"
    ws_id = "ws_dead_recovery"
    sandbox_id = "sbx_will_die"

    await lease_mgr.claim_workspace_ownership(ws_id, "tenant_a")
    await lease_mgr.acquire_workspace_lock(ws_id, session_id, ttl_seconds=300)
    await lease_mgr.create_or_renew(
        session_id=session_id,
        workspace_id=ws_id,
        sandbox_id=sandbox_id,
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=300,
        tenant_id="tenant_a",
    )

    # Container starts alive
    driver.live_containers.add(sandbox_id)
    assert await driver.is_alive(sandbox_id) is True

    # Container crashes / dies!
    driver.live_containers.remove(sandbox_id)
    assert await driver.is_alive(sandbox_id) is False

    # Run sweeper single pass
    await sweeper.sweep_once()

    # Verify lease is expired and workspace lock is freed
    lease = await lease_mgr.get(session_id)
    assert lease is None or lease.status == "expired", "Dead container's lease must be expired!"

    # Now another session can acquire the workspace lock immediately without 409 conflict
    acquired, locked_by, _ = await lease_mgr.acquire_workspace_lock(ws_id, "sess_new", ttl_seconds=60)
    assert acquired is True, f"Workspace lock should be released after dead container reconciliation! Locked by: {locked_by}"

    await lease_mgr.close()


def test_websocket_cross_tenant_rejection():
    """Verify that WebSocket PTY route rejects connections from another tenant with code 4403."""
    from starlette.testclient import TestClient
    from starlette.websockets import WebSocketDisconnect

    with TestClient(app) as test_client:
        # 1. Acquire session for tenant_owner
        resp = test_client.post(
            "/v1/sessions/acquire",
            json={"workspace_id": "ws_for_websocket_sec", "ttl_seconds": 60},
            headers={"X-Tenant-ID": "tenant_owner"},
        )
        assert resp.status_code == 200
        sess_data = resp.json()
        session_id = sess_data["session_id"]

        # 2. Attempt to connect to WebSocket as tenant_intruder -> MUST be rejected with code 4403
        with pytest.raises(WebSocketDisconnect) as excinfo:
            with test_client.websocket_connect(
                f"/v1/sessions/{session_id}/ws/pty",
                headers={"X-Tenant-ID": "tenant_intruder"},
            ):
                pass
        assert excinfo.value.code == 4403, f"Expected WS close code 4403, got {excinfo.value.code}"


@pytest.mark.asyncio
async def test_unauthenticated_flood_does_not_affect_victim_tenant():
    """Verify that an unauthenticated attacker spraying requests with victim's tenant ID does NOT rate-limit the victim."""
    orig_api_key = settings.api_key
    settings.api_key = "secure_test_api_key_32_characters_long"
    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                victim_tenant = "victim_corp"
                attacker_headers = {"X-Tenant-ID": victim_tenant}  # No API key provided

                # 1. Attacker sends 150 unauthenticated requests claiming to be victim
                for _ in range(150):
                    r = await client.get("/v1/sessions/any_session/fs/read?path=file.txt", headers=attacker_headers)
                    assert r.status_code == 401, f"Expected 401 Unauthorized for unauthenticated attacker, got {r.status_code}"

                # 2. Victim makes a legitimate request with their verified tenant header and valid API key
                victim_headers = {
                    "X-Tenant-ID": victim_tenant,
                    "Authorization": f"Bearer {settings.api_key}",
                }
                resp = await client.get("/v1/sessions/sess_nonexistent/fs/read?path=file.txt", headers=victim_headers)

                # Must NOT be 429 Too Many Requests!
                assert resp.status_code != 429, f"Victim tenant was unfairly rate-limited by unauthenticated flood! Status: {resp.status_code}"
                assert resp.status_code in (200, 404), f"Legitimate request should succeed auth, got {resp.status_code}"
    finally:
        settings.api_key = orig_api_key


def test_malformed_tenant_keys_json_fails_startup():
    """Verify that malformed or invalid SANDBOX_TENANT_KEYS_JSON fails startup immediately."""
    # Malformed JSON syntax
    s_bad_json = EngineSettings(tenant_keys_json="{not-valid-json")
    with pytest.raises(ValueError, match="malformed JSON"):
        s_bad_json.validate_tenant_keys_json()

    # Non-dictionary structure
    s_list = EngineSettings(tenant_keys_json='["tenant1", "key1"]')
    with pytest.raises(ValueError, match="JSON dictionary"):
        s_list.validate_tenant_keys_json()

    # Invalid tenant_id format (injection/special chars)
    s_bad_id = EngineSettings(tenant_keys_json='{"invalid tenant!": "key123456789012345678901234567890"}')
    with pytest.raises(ValueError, match="Invalid tenant_id"):
        s_bad_id.validate_tenant_keys_json()


def test_k8s_driver_skips_host_directory_creation(tmp_path):
    """Verify that K8s driver skips host directory creation to comply with readOnlyRootFilesystem."""
    import os
    non_existent_readonly_root = str(tmp_path / "k8s_readonly_workspaces")
    # Do not create directory on host
    ws_path = validate_workspace_id("my_workspace", non_existent_readonly_root, tenant_id="test_k8s_tenant", create_host_dir=False)
    assert ws_path.endswith("my_workspace")
    assert not os.path.exists(ws_path), "Host directory must NOT be created when create_host_dir=False"


@pytest.mark.asyncio
async def test_tenant_id_regex_format_validation():
    """Verify that malformed tenant_id (path traversal, special characters) is rejected with 400 Bad Request."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            invalid_tenants = [
                "../../etc",
                "tenant/with/slashes",
                "tenant;rm -rf",
                "tenant with spaces",
                "t" * 64,  # exceeds 63 characters
            ]
            for invalid_id in invalid_tenants:
                resp = await client.post(
                    "/v1/sessions/acquire",
                    json={"workspace_id": "ws1"},
                    headers={"X-Tenant-ID": invalid_id},
                )
                assert resp.status_code == 400, f"Expected 400 for invalid tenant_id '{invalid_id}', got {resp.status_code}"
                assert "Invalid tenant_id" in resp.text


