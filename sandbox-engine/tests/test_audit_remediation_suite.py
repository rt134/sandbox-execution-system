import asyncio
import json
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
import httpx

from sandbox_engine.config import settings
from sandbox_engine.core.admission import (
    try_acquire_in_flight,
    release_in_flight,
    try_acquire_tenant_slot,
    release_tenant_slot,
)
from sandbox_engine.core.lease_manager import LeaseManager, SQLiteLeaseBackend
from sandbox_engine.core.rate_limiter import rate_limiter
from sandbox_engine.core.metrics import RATE_LIMITER_FALLBACK_TOTAL
from sandbox_engine.main import app


@pytest.mark.asyncio
async def test_tenant_stateless_concurrency_cap_h3():
    """Verify that per-tenant stateless concurrency slot acquisition is bounded (H3)."""
    tenant_id = "tenant-test-h3"
    max_slots = 3

    # Acquire up to max_slots
    for _ in range(max_slots):
        assert await try_acquire_tenant_slot(tenant_id, max_slots=max_slots) is True

    # Next attempt must be rejected
    assert await try_acquire_tenant_slot(tenant_id, max_slots=max_slots) is False

    # Release one slot
    await release_tenant_slot(tenant_id)

    # Now acquisition must succeed
    assert await try_acquire_tenant_slot(tenant_id, max_slots=max_slots) is True

    # Clean up
    for _ in range(max_slots):
        await release_tenant_slot(tenant_id)


@pytest.mark.asyncio
async def test_admission_non_raising_503_m2():
    """Verify that when admission is saturated, 503 is returned cleanly with Retry-After (M2)."""
    orig_cap = settings.max_in_flight_requests
    try:
        settings.max_in_flight_requests = 1
        # Fill the single available slot
        assert await try_acquire_in_flight() is True

        # Second request over HTTP should get a 503 JSON response, not an unhandled 500
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/v1/stateless/run", json={"code": "print(1)"})
            assert resp.status_code == 503
            assert resp.headers.get("retry-after") == "5"
            assert resp.json()["detail"] == "Server at capacity"
            assert "x-request-id" in resp.headers
    finally:
        await release_in_flight()
        settings.max_in_flight_requests = orig_cap


@pytest.mark.asyncio
async def test_mark_expired_protects_reacquired_lease_m9(tmp_path):
    """Verify that mark_expired with from_status='reclaiming' protects active re-acquired leases (M9)."""
    db_file = str(tmp_path / "test_leases.db")
    backend = SQLiteLeaseBackend(db_path=db_file)

    # 1. Create a lease in active status
    lease = await backend.create_or_renew_lease(
        session_id="session-test-m9",
        workspace_id="ws-1",
        sandbox_id="sbx-1",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=60,
    )
    assert lease.status == "active"

    # 2. mark_expired with from_status='reclaiming' on active lease should NOT transition it (only 'reclaiming' can be expired)
    marked = await backend.mark_expired("session-test-m9", from_status="reclaiming")
    assert marked is False
    current = await backend.get_lease("session-test-m9")
    assert current is not None
    assert current.status == "active"

    # 3. Transition to reclaiming (as claim_expired does)
    backend._sync_update_status("session-test-m9", "reclaiming")
    current_reclaiming = await backend.get_lease("session-test-m9")
    assert current_reclaiming.status == "reclaiming"

    # 4. mark_expired on reclaiming lease MUST successfully mark it expired
    marked_rec = await backend.mark_expired("session-test-m9", from_status="reclaiming")
    assert marked_rec is True
    expired = await backend.get_lease("session-test-m9")
    assert expired.status == "expired"


@pytest.mark.asyncio
async def test_sweeper_dead_container_reconciliation_and_leader_election_n3_n4(tmp_path):
    """Verify that sweeper reconciles dead containers (N3) and respects TTL leader election (N4)."""
    from sandbox_engine.core.sweeper import Sweeper
    from unittest.mock import AsyncMock

    db_file = str(tmp_path / "test_sweeper.db")
    backend = SQLiteLeaseBackend(db_path=db_file)
    lease_mgr = LeaseManager(backend=backend)

    # Create one dead container lease and one live container lease
    await backend.create_or_renew_lease(
        session_id="session-dead-1",
        workspace_id="ws-dead",
        sandbox_id="sbx-dead",
        endpoint="http://127.0.0.1:5001",
        ttl_seconds=300,
    )
    await backend.create_or_renew_lease(
        session_id="session-live-1",
        workspace_id="ws-live",
        sandbox_id="sbx-live",
        endpoint="http://127.0.0.1:5002",
        ttl_seconds=300,
    )

    # Acquire workspace lock for dead session
    await backend.acquire_workspace_lock("ws-dead", "session-dead-1", ttl_seconds=300)

    # Mock driver where sbx-dead is dead and sbx-live is alive
    mock_driver = AsyncMock()
    async def mock_is_alive(sandbox_id: str) -> bool:
        return sandbox_id == "sbx-live"
    mock_driver.is_alive.side_effect = mock_is_alive

    sweeper = Sweeper(driver=mock_driver, lease_manager=lease_mgr, interval_sec=1.0)
    await sweeper.sweep_once()

    # Dead session must be marked 'expired' (N3) and its lock released
    dead_lease = await backend.get_lease("session-dead-1")
    assert dead_lease.status == "expired"
    lock = await backend.get_workspace_lock("ws-dead")
    assert lock is None

    # Live session must still be 'active'
    live_lease = await backend.get_lease("session-live-1")
    assert live_lease.status == "active"

    # Leader election test (N4): Engine 1 holds lease, Engine 2 is denied until expiry
    assert await lease_mgr.try_acquire_sweeper_leader("engine-1", ttl_seconds=15.0) is True
    assert await lease_mgr.try_acquire_sweeper_leader("engine-2", ttl_seconds=15.0) is False
    # Engine 1 renewal succeeds
    assert await lease_mgr.try_acquire_sweeper_leader("engine-1", ttl_seconds=15.0) is True


@pytest.mark.asyncio
async def test_auth_api_key_only_and_tenant_keys_validation_n1_n2():
    """Verify that config validation works with tenant-keys only (N1) and api-key-only auth works (N2)."""
    from sandbox_engine.core.security import authenticate_tenant_credentials
    from sandbox_engine.config import SandboxSettings

    # N1: Validation does not crash with UnboundLocalError when api_key is None
    cfg = SandboxSettings(
        environment="production",
        tenant_keys_json='{"tenant-1": "0123456789abcdef0123456789abcdef"}',
        api_key=None,
        encryption_key="a" * 32,
        postgres_url="postgresql://sandbox:sandbox@localhost:5432/sandbox",
    )
    # validate_production_settings must execute without UnboundLocalError
    cfg.validate_production_settings()

    # N2: API-key-only auth does not return 500 when valid key is provided
    orig_env = settings.environment
    orig_key = settings.api_key
    orig_tenant_keys = settings.tenant_keys_json
    try:
        settings.environment = "production"
        settings.api_key = "0123456789abcdef0123456789abcdef"
        settings.tenant_keys_json = None

        # Correct API key authentication succeeds without 500
        tid = await authenticate_tenant_credentials(
            auth_header=f"Bearer {settings.api_key}",
            raw_tenant="custom-tenant",
            enforce_rate_limit=False,
        )
        assert tid == "custom-tenant"

        # Invalid key returns 401
        with pytest.raises(HTTPException) as exc_info:
            await authenticate_tenant_credentials(
                auth_header="Bearer wrong-key-value",
                raw_tenant="custom-tenant",
                enforce_rate_limit=False,
            )
        assert exc_info.value.status_code == 401
    finally:
        settings.environment = orig_env
        settings.api_key = orig_key
        settings.tenant_keys_json = orig_tenant_keys


@pytest.mark.asyncio
async def test_healthz_production_privacy_l3():
    """Verify that /healthz in production does not leak driver class names or internal pool sizes (L3)."""
    orig_env = settings.environment
    try:
        # Development mode
        settings.environment = "development"
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp_dev = await client.get("/healthz")
            assert resp_dev.status_code == 200
            assert "configured_driver" in resp_dev.json()

        # Production mode
        settings.environment = "production"
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp_prod = await client.get("/healthz")
            assert resp_prod.status_code == 200
            data = resp_prod.json()
            assert data == {"status": "healthy"}
            assert "configured_driver" not in data
            assert "warm_pool_available" not in data
    finally:
        settings.environment = orig_env


@pytest.mark.asyncio
async def test_request_id_injected_m10():
    """Verify that X-Request-ID is propagated or generated on all responses (M10)."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # Generated ID
        resp1 = await client.get("/livez")
        assert "x-request-id" in resp1.headers

        # Propagated client ID
        custom_id = "req-custom-trace-12345"
        resp2 = await client.get("/livez", headers={"X-Request-ID": custom_id})
        assert resp2.headers.get("x-request-id") == custom_id
