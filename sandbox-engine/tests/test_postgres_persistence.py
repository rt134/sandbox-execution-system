import asyncio
import os
import time
import pytest
import pytest_asyncio
import asyncpg
from httpx import AsyncClient, ASGITransport

from sandbox_engine.config import settings
from sandbox_engine.core.lease_manager import PostgresLeaseBackend, LeaseManager
from sandbox_engine.main import app


TEST_PG_URL = os.environ.get("SANDBOX_POSTGRES_URL", "postgresql://sandbox:sandbox@localhost:5432/sandbox")


@pytest_asyncio.fixture
async def pg_backend():
    backend = PostgresLeaseBackend(TEST_PG_URL)
    try:
        await backend.initialize()
    except Exception as e:
        pytest.skip(f"Docker PostgreSQL not available: {e}")
    yield backend
    await backend.close()


@pytest.mark.asyncio
async def test_postgres_lease_lifecycle(pg_backend):
    session_id = f"test_sess_{int(time.time() * 1000)}"
    workspace_id = f"test_ws_{int(time.time() * 1000)}"
    sandbox_id = "sb_pg_123"
    endpoint = "http://127.0.0.1:5004"
    auth_token = "secret_tok_abc"

    # 1. Create Lease
    lease = await pg_backend.create_or_renew_lease(
        session_id=session_id,
        workspace_id=workspace_id,
        sandbox_id=sandbox_id,
        endpoint=endpoint,
        ttl_seconds=300,
        auth_token=auth_token,
    )
    assert lease.session_id == session_id
    assert lease.workspace_id == workspace_id
    assert lease.status == "active"
    assert lease.auth_token == auth_token

    # 2. Get Lease
    retrieved = await pg_backend.get_lease(session_id)
    assert retrieved is not None
    assert retrieved.sandbox_id == sandbox_id
    assert retrieved.auth_token == auth_token

    # 3. Heartbeat
    hb = await pg_backend.heartbeat(session_id, ttl_seconds=600)
    assert hb is not None
    assert hb.expires_at > lease.expires_at

    # 4. List Active Leases
    active = await pg_backend.list_active_leases()
    assert any(l.session_id == session_id for l in active)

    # 5. Release Lease
    released = await pg_backend.release_lease(session_id)
    assert released is True

    # Check status is released
    after_release = await pg_backend.get_lease(session_id)
    assert after_release.status == "released"


@pytest.mark.asyncio
async def test_postgres_workspace_atomic_lock(pg_backend):
    workspace_id = f"atomic_ws_{int(time.time() * 1000)}"
    session_1 = "session_owner_1"
    session_2 = "session_competitor_2"

    # 1. Session 1 acquires lock
    success, owner, expires_at = await pg_backend.acquire_workspace_lock(
        workspace_id=workspace_id, session_id=session_1, ttl_seconds=300
    )
    assert success is True
    assert owner == session_1
    assert expires_at > time.time()

    # 2. Session 2 attempts to acquire same lock -> CONFLICT!
    success_2, owner_2, expires_at_2 = await pg_backend.acquire_workspace_lock(
        workspace_id=workspace_id, session_id=session_2, ttl_seconds=300
    )
    assert success_2 is False
    assert owner_2 == session_1
    assert expires_at_2 == expires_at

    # 3. Refresh lock by Session 1 succeeds
    refreshed = await pg_backend.refresh_workspace_lock(workspace_id, session_1, ttl_seconds=600)
    assert refreshed is True

    # 4. Release lock by Session 1
    released = await pg_backend.release_workspace_lock(workspace_id, session_1)
    assert released is True

    # 5. Now Session 2 can acquire lock
    success_3, owner_3, _ = await pg_backend.acquire_workspace_lock(
        workspace_id=workspace_id, session_id=session_2, ttl_seconds=300
    )
    assert success_3 is True
    assert owner_3 == session_2

    # Clean up
    await pg_backend.release_workspace_lock(workspace_id, session_2)


@pytest.mark.asyncio
async def test_postgres_audit_logging(pg_backend):
    session_id = f"audit_sess_{int(time.time() * 1000)}"
    workspace_id = f"audit_ws_{int(time.time() * 1000)}"

    # Create lease (should write audit log)
    await pg_backend.create_or_renew_lease(
        session_id=session_id,
        workspace_id=workspace_id,
        sandbox_id="sb_audit",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=300,
    )

    # Release lease (should write audit log)
    await pg_backend.release_lease(session_id)

    # Query audit logs directly
    async with pg_backend._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT event_type FROM session_audit_logs WHERE session_id = $1 ORDER BY id ASC",
            session_id,
        )
        events = [r["event_type"] for r in rows]
        assert "lease_created" in events
        assert "lease_released" in events


def test_production_env_validation(monkeypatch):
    """Verify that production environment requires PostgreSQL database URL in ENV."""
    # Temporarily set environment to production
    monkeypatch.setattr(settings, "environment", "production")
    monkeypatch.setattr(settings, "postgres_url", None)

    with pytest.raises(ValueError, match="Production environment requires PostgreSQL database URL"):
        settings.get_effective_postgres_url()

    # Setting connection string succeeds in production
    monkeypatch.setattr(settings, "postgres_url", "postgresql://prod_user:pass@db.internal:5432/prod_sandboxes")
    assert settings.get_effective_postgres_url() == "postgresql://prod_user:pass@db.internal:5432/prod_sandboxes"


@pytest.mark.asyncio
async def test_readyz_probe_reports_database_ready():
    """Verify that the /readyz probe validates PostgreSQL connection readiness."""
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/readyz")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "ready"
            assert data["database"] == "ready"


@pytest.mark.asyncio
async def test_token_encryption_at_rest(pg_backend):
    """Verify that sensitive daemon auth tokens are encrypted at rest in PostgreSQL."""
    session_id = f"enc_sess_{int(time.time() * 1000)}"
    workspace_id = f"enc_ws_{int(time.time() * 1000)}"
    plaintext_token = "secret_raw_token_xyz_12345"

    lease = await pg_backend.create_or_renew_lease(
        session_id=session_id,
        workspace_id=workspace_id,
        sandbox_id="sb_enc",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=300,
        auth_token=plaintext_token,
        tenant_id="tenant-alpha",
    )
    assert lease.auth_token == plaintext_token
    assert lease.tenant_id == "tenant-alpha"

    # Inspect the raw PostgreSQL table row
    async with pg_backend._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT auth_token, tenant_id FROM leases WHERE session_id = $1",
            session_id,
        )
        stored_token = row["auth_token"]
        assert stored_token != plaintext_token
        assert stored_token.startswith("gAAAAA")  # Fernet signature
        assert row["tenant_id"] == "tenant-alpha"

    # Decryption works seamlessly upon fetching through backend
    fetched = await pg_backend.get_lease(session_id)
    assert fetched.auth_token == plaintext_token


@pytest.mark.asyncio
async def test_atomic_claim_expired_leases(pg_backend):
    """Verify that expired leases are atomically claimed without sweeper race conditions."""
    session_id = f"exp_sess_{int(time.time() * 1000)}"
    workspace_id = f"exp_ws_{int(time.time() * 1000)}"

    # Create lease that already expired 10 seconds ago
    now = time.time()
    await pg_backend.create_or_renew_lease(
        session_id=session_id,
        workspace_id=workspace_id,
        sandbox_id="sb_exp",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=-10,
    )

    # Claim expired leases
    claimed = await pg_backend.claim_expired_leases(cutoff_time=now)
    assert any(l.session_id == session_id for l in claimed)

    # Status in database should now be 'reclaiming'
    updated = await pg_backend.get_lease(session_id)
    assert updated.status == "reclaiming"

    # Subsequent claim should return nothing (no double sweep)
    second_claim = await pg_backend.claim_expired_leases(cutoff_time=now)
    assert not any(l.session_id == session_id for l in second_claim)


def test_workspace_id_newline_rejected():
    """Verify that workspace_id containing trailing newline is strictly rejected."""
    from sandbox_engine.core.security import validate_workspace_id
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        validate_workspace_id("ws\n")
    assert exc_info.value.status_code == 400
    assert "Invalid workspace_id" in exc_info.value.detail


def test_production_mode_refuses_to_start_without_auth(monkeypatch):
    """Verify that production mode refuses to start without any auth configured."""
    monkeypatch.setattr(settings, "environment", "production")
    monkeypatch.setattr(settings, "tenant_keys_json", None)
    monkeypatch.setattr(settings, "api_key", None)
    monkeypatch.setattr(settings, "encryption_key", "b" * 32)
    monkeypatch.setattr(settings, "postgres_url", "postgresql://user:pass@host:5432/db")
    monkeypatch.setattr(settings, "driver", "k8s")

    with pytest.raises(ValueError, match="Production requires either SANDBOX_TENANT_KEYS_JSON or SANDBOX_API_KEY"):
        settings.validate_production_settings()


def test_production_mode_accepts_either_tenant_keys_or_api_key(monkeypatch):
    """Verify that production mode accepts either SANDBOX_TENANT_KEYS_JSON or SANDBOX_API_KEY alone."""
    monkeypatch.setattr(settings, "environment", "production")
    monkeypatch.setattr(settings, "encryption_key", "d" * 32)
    monkeypatch.setattr(settings, "postgres_url", "postgresql://user:pass@host:5432/db")
    monkeypatch.setattr(settings, "driver", "k8s")

    # 1. API key only
    monkeypatch.setattr(settings, "tenant_keys_json", None)
    monkeypatch.setattr(settings, "api_key", "a" * 32)
    settings.validate_production_settings()

    # 2. Tenant keys only
    monkeypatch.setattr(settings, "api_key", None)
    monkeypatch.setattr(settings, "tenant_keys_json", '{"prodtenant": "' + ("c" * 32) + '"}')
    settings.validate_production_settings()


