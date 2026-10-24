import asyncio
import os
import time
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from sandbox_engine.config import settings
from sandbox_engine.core.lease_manager import (
    LeaseManager,
    SQLiteLeaseBackend,
    PostgresLeaseBackend,
)
from sandbox_engine.core.metrics import LEASE_QUARANTINE_TOTAL
from sandbox_engine.core.sweeper import Sweeper
from sandbox_engine.models import LeaseRecord


@pytest.mark.asyncio
async def test_sqlite_quarantine_bad_token_on_list_active(tmp_path):
    """Assert SQLite backend catches decrypt errors per row, quarantines row, emits metric, and returns healthy rows."""
    db_path = str(tmp_path / "quarantine_test.db")
    backend = SQLiteLeaseBackend(db_path=db_path)
    now = time.time()

    # Create one valid lease and one bad lease
    await backend.create_or_renew_lease(
        session_id="good-sess",
        workspace_id="ws-good",
        sandbox_id="sb-good",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=300,
        auth_token="good-token",
    )
    await backend.create_or_renew_lease(
        session_id="bad-sess",
        workspace_id="ws-bad",
        sandbox_id="sb-bad",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=300,
        auth_token="corrupt-token",
    )

    initial_quarantine_count = LEASE_QUARANTINE_TOTAL.labels(reason="decryption_error")._value.get()

    # Mock decrypt_token to raise on 'corrupt-token' as in production
    def mock_decrypt(token):
        if token == "corrupt-token":
            raise ValueError("Failed to decrypt stored auth token")
        return token

    with patch("sandbox_engine.core.lease_manager.decrypt_token", side_effect=mock_decrypt):
        active = await backend.list_active_leases()

    # Healthy lease returned, bad lease excluded
    assert len(active) == 1
    assert active[0].session_id == "good-sess"

    # Metric incremented
    new_quarantine_count = LEASE_QUARANTINE_TOTAL.labels(reason="decryption_error")._value.get()
    assert new_quarantine_count == initial_quarantine_count + 1

    # Row status is now quarantined in the db
    raw_bad = backend._sync_get_lease_raw("bad-sess")
    assert raw_bad is not None
    assert raw_bad[6] == "quarantined"


@pytest.mark.asyncio
async def test_sqlite_quarantine_bad_token_on_claim_expired(tmp_path):
    """Assert claim_expired_leases quarantines bad tokens without stranding expired rows."""
    db_path = str(tmp_path / "quarantine_exp_test.db")
    backend = SQLiteLeaseBackend(db_path=db_path)
    now = time.time()

    # Create two expired leases: one healthy, one bad
    await backend.create_or_renew_lease(
        session_id="exp-good",
        workspace_id="ws-exp-good",
        sandbox_id="sb-exp-good",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=-10,
        auth_token="good-token",
    )
    await backend.create_or_renew_lease(
        session_id="exp-bad",
        workspace_id="ws-exp-bad",
        sandbox_id="sb-exp-bad",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=-10,
        auth_token="corrupt-token",
    )

    def mock_decrypt(token):
        if token == "corrupt-token":
            raise ValueError("Failed to decrypt stored auth token")
        return token

    with patch("sandbox_engine.core.lease_manager.decrypt_token", side_effect=mock_decrypt):
        claimed = await backend.claim_expired_leases(cutoff_time=now)

    # Valid expired lease returned
    assert len(claimed) == 1
    assert claimed[0].session_id == "exp-good"

    # Bad expired lease quarantined
    raw_bad = backend._sync_get_lease_raw("exp-bad")
    assert raw_bad[6] == "quarantined"


# Helper to inspect raw sqlite row
def _sync_get_raw(self, session_id):
    import sqlite3
    with sqlite3.connect(self.db_path) as conn:
        cursor = conn.execute("SELECT * FROM leases WHERE session_id = ?", (session_id,))
        return cursor.fetchone()

SQLiteLeaseBackend._sync_get_lease_raw = _sync_get_raw


@pytest.mark.asyncio
async def test_sweeper_steps_wrapped_independently():
    """Assert sweeper executes all steps independently even if preceding steps fail."""
    mock_driver = AsyncMock()
    mock_driver.is_alive = AsyncMock(return_value=True)
    mock_driver.list_sandboxes = AsyncMock(return_value=[])
    mock_driver.cleanup_orphaned_pvcs = AsyncMock()

    mock_lease_mgr = AsyncMock()
    # Step 1 raises an exception
    mock_lease_mgr.claim_expired = AsyncMock(side_effect=RuntimeError("Step 1 failed!"))
    # Step 2 succeeds
    mock_lease_mgr.cleanup_expired_workspace_locks = AsyncMock()
    # Step 3 succeeds
    healthy_lease = LeaseRecord(
        session_id="live-1",
        workspace_id="ws-1",
        sandbox_id="sb-1",
        endpoint="http://127.0.0.1:5004",
        created_at=time.time(),
        expires_at=time.time() + 300,
        status="active",
        auth_token="tok",
    )
    mock_lease_mgr.list_active = AsyncMock(return_value=[healthy_lease])
    # Step 4 succeeds
    mock_lease_mgr.get_dead_engines = AsyncMock(return_value=[])
    # Step 8 succeeds
    mock_lease_mgr.prune_database_records = AsyncMock()
    mock_lease_mgr.count_active_locks = AsyncMock(return_value=0)

    sweeper = Sweeper(driver=mock_driver, lease_manager=mock_lease_mgr)

    # Run sweep_once
    await sweeper.sweep_once(force=True)

    # Verify Step 2 executed despite Step 1 failure
    mock_lease_mgr.cleanup_expired_workspace_locks.assert_awaited_once()

    # Verify Step 3 executed
    mock_lease_mgr.list_active.assert_awaited_once()

    # Verify Step 4 executed
    mock_lease_mgr.get_dead_engines.assert_awaited_once()

    # Verify Step 7 executed
    mock_driver.cleanup_orphaned_pvcs.assert_awaited_once()

    # Verify Step 8 executed
    mock_lease_mgr.prune_database_records.assert_awaited_once()


@pytest.mark.asyncio
async def test_sweeper_step5_safely_skips_orphan_gc_when_list_active_fails():
    """Assert Step 5 does not destroy running sandboxes if list_active fails in Step 3."""
    now = time.time()
    mock_driver = AsyncMock()
    running_stateful = MagicMock()
    running_stateful.sandbox_id = "sb-important-running"
    running_stateful.created_at = now - 500  # Older than grace period

    mock_driver.list_sandboxes = AsyncMock(return_value=[running_stateful])
    mock_driver.destroy_sandbox = AsyncMock()

    mock_lease_mgr = AsyncMock()
    mock_lease_mgr.claim_expired = AsyncMock(return_value=[])
    mock_lease_mgr.cleanup_expired_workspace_locks = AsyncMock()
    # Step 3 list_active fails!
    mock_lease_mgr.list_active = AsyncMock(side_effect=Exception("Database error in list_active"))
    mock_lease_mgr.get_dead_engines = AsyncMock(return_value=[])
    mock_lease_mgr.prune_database_records = AsyncMock()
    mock_lease_mgr.count_active_locks = AsyncMock(return_value=0)

    sweeper = Sweeper(driver=mock_driver, lease_manager=mock_lease_mgr)
    await sweeper.sweep_once(force=True)

    # Crucial: destroy_sandbox must NOT be called for running_stateful!
    mock_driver.destroy_sandbox.assert_not_called()
    # But step 8 must still run
    mock_lease_mgr.prune_database_records.assert_awaited_once()


@pytest.mark.asyncio
async def test_postgres_quarantine_bad_token_on_list_active():
    """Assert Postgres backend catches decrypt errors per row, quarantines row, emits metric, and returns healthy rows."""
    TEST_PG_URL = os.environ.get("SANDBOX_POSTGRES_URL", "postgresql://sandbox:sandbox@localhost:5432/sandbox")
    backend = PostgresLeaseBackend(TEST_PG_URL)
    try:
        await backend.initialize()
    except Exception as e:
        pytest.skip(f"Docker PostgreSQL not available: {e}")

    try:
        now = time.time()
        good_sess = f"good_pg_{int(now * 1000)}"
        bad_sess = f"bad_pg_{int(now * 1000)}"

        await backend.create_or_renew_lease(
            session_id=good_sess,
            workspace_id="ws-good-pg",
            sandbox_id="sb-good-pg",
            endpoint="http://127.0.0.1:5004",
            ttl_seconds=300,
            auth_token="good-token",
        )
        await backend.create_or_renew_lease(
            session_id=bad_sess,
            workspace_id="ws-bad-pg",
            sandbox_id="sb-bad-pg",
            endpoint="http://127.0.0.1:5004",
            ttl_seconds=300,
            auth_token="will-be-corrupted",
        )

        # Corrupt the encrypted token in the database to simulate encryption key rotation failure
        async with backend._pool.acquire() as conn:
            await conn.execute("UPDATE leases SET auth_token = 'corrupted-fernet-ciphertext' WHERE session_id = $1", bad_sess)

        initial_quarantine_count = LEASE_QUARANTINE_TOTAL.labels(reason="decryption_error")._value.get()

        with patch.object(settings, "environment", "production"):
            active = await backend.list_active_leases()

        assert any(l.session_id == good_sess for l in active)
        assert not any(l.session_id == bad_sess for l in active)

        new_quarantine_count = LEASE_QUARANTINE_TOTAL.labels(reason="decryption_error")._value.get()
        assert new_quarantine_count > initial_quarantine_count

        async with backend._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT status FROM leases WHERE session_id = $1", bad_sess)
            assert row["status"] == "quarantined"
    finally:
        await backend.close()


@pytest.mark.asyncio
async def test_postgres_quarantine_bad_token_on_claim_expired():
    """Assert Postgres backend claim_expired_leases quarantines bad tokens and does not strand expired rows."""
    TEST_PG_URL = os.environ.get("SANDBOX_POSTGRES_URL", "postgresql://sandbox:sandbox@localhost:5432/sandbox")
    backend = PostgresLeaseBackend(TEST_PG_URL)
    try:
        await backend.initialize()
    except Exception as e:
        pytest.skip(f"Docker PostgreSQL not available: {e}")

    try:
        now = time.time()
        good_sess = f"exp_good_pg_{int(now * 1000)}"
        bad_sess = f"exp_bad_pg_{int(now * 1000)}"

        await backend.create_or_renew_lease(
            session_id=good_sess,
            workspace_id="ws-exp-good-pg",
            sandbox_id="sb-exp-good-pg",
            endpoint="http://127.0.0.1:5004",
            ttl_seconds=-10,
            auth_token="good-token",
        )
        await backend.create_or_renew_lease(
            session_id=bad_sess,
            workspace_id="ws-exp-bad-pg",
            sandbox_id="sb-exp-bad-pg",
            endpoint="http://127.0.0.1:5004",
            ttl_seconds=-10,
            auth_token="will-be-corrupted",
        )

        async with backend._pool.acquire() as conn:
            await conn.execute("UPDATE leases SET auth_token = 'corrupted-fernet-ciphertext' WHERE session_id = $1", bad_sess)

        with patch.object(settings, "environment", "production"):
            claimed = await backend.claim_expired_leases(cutoff_time=now)

        assert any(l.session_id == good_sess for l in claimed)
        assert not any(l.session_id == bad_sess for l in claimed)

        async with backend._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT status FROM leases WHERE session_id = $1", bad_sess)
            assert row["status"] == "quarantined"
    finally:
        await backend.close()


@pytest.mark.asyncio
async def test_acquire_session_not_500_with_bad_lease_row():
    """Assert acquire_session succeeds with 200 instead of 500 when database contains a bad lease row."""
    import httpx
    from sandbox_engine.main import app
    from sandbox_engine.core.lease_manager import LeaseManager
    from sandbox_engine.drivers import get_driver

    lease_mgr = LeaseManager()
    await lease_mgr.initialize()
    app.state.lease_manager = lease_mgr
    app.state.driver = get_driver()

    await lease_mgr.create_or_renew(
        session_id="bad-sess-api",
        workspace_id="ws-bad-api",
        sandbox_id="sb-bad-api",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=300,
        auth_token="will-fail",
    )

    def mock_decrypt(token):
        if token == "will-fail":
            raise ValueError("Failed to decrypt stored auth token")
        return token

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        with patch("sandbox_engine.core.lease_manager.decrypt_token", side_effect=mock_decrypt):
            resp = await client.post(
                "/v1/sessions/acquire",
                json={"workspace_id": f"ws-test-ok-{int(time.time() * 1000)}"},
            )
            assert resp.status_code == 200, f"Unexpected status {resp.status_code}: {resp.text}"
            assert resp.json().get("session_id")
