import asyncio
import json
import os
import subprocess
import time
from unittest.mock import AsyncMock, MagicMock
import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from sandbox_engine.api.routes_sessions import router as sessions_router
from sandbox_engine.config import settings
from sandbox_engine.core.lease_manager import (
    LeaseManager,
    RedisLeaseBackend,
    SQLiteLeaseBackend,
)
from sandbox_engine.core.security import (
    authenticate_tenant_credentials,
    safe_compare_digest,
    validate_session_id,
    validate_workspace_id,
)
from sandbox_engine.core.sweeper import Sweeper
from sandbox_engine.main import app
from sandbox_engine.models import LeaseRecord, SandboxInstance


@pytest.mark.asyncio
async def test_probes_unauthenticated():
    """Verify that /livez, /healthz, and /readyz succeed without any authentication headers."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # 1. /livez
        resp_livez = await client.get("/livez")
        assert resp_livez.status_code == 200
        assert resp_livez.json() == {"status": "alive"}

        # 2. /healthz
        resp_healthz = await client.get("/healthz")
        assert resp_healthz.status_code == 200
        assert resp_healthz.json().get("status") == "healthy"

        # 3. /readyz
        resp_readyz = await client.get("/readyz")
        assert resp_readyz.status_code in (200, 503)


@pytest.mark.asyncio
async def test_non_ascii_api_key_returns_401():
    """Verify that non-ASCII API keys return 401 instead of raising TypeError (500)."""
    # Test safe_compare_digest directly
    assert not safe_compare_digest("këy-non-ascii", "valid-key-ascii")
    assert not safe_compare_digest("valid-key-ascii", "këy-non-ascii")
    assert safe_compare_digest("këy-üñícode", "këy-üñícode")

    # Test via authenticate_tenant_credentials
    orig_key = settings.api_key
    try:
        settings.api_key = "super-secret-key-for-testing-123456"
        with pytest.raises(HTTPException) as exc_info:
            await authenticate_tenant_credentials(
                provided_key="këy-with-non-ascii-characters",
                raw_tenant="test-tenant",
            )
        assert exc_info.value.status_code == 401
    finally:
        settings.api_key = orig_key


def test_session_id_validation():
    """Verify session_id format and length rules."""
    # Valid session IDs
    assert validate_session_id("sess_12345") == "sess_12345"
    assert validate_session_id("a" * 63) == "a" * 63
    assert validate_session_id("tenant-1.session_01") == "tenant-1.session_01"

    # Oversized > 63 chars
    with pytest.raises(HTTPException) as exc:
        validate_session_id("a" * 64)
    assert exc.value.status_code == 400

    # Invalid characters
    for invalid in ["sess/123", "sess 123", "sess\n123", "sess;rm", ""]:
        with pytest.raises(HTTPException) as exc:
            validate_session_id(invalid)
        assert exc.value.status_code == 400


def test_workspace_id_capped_at_63():
    """Verify workspace_id is capped at 63 chars for K8s label compliance."""
    # 63 chars valid
    assert validate_workspace_id("w" * 63, create_host_dir=False)

    # 64 chars invalid (exceeds K8s label cap)
    with pytest.raises(HTTPException) as exc:
        validate_workspace_id("w" * 64, create_host_dir=False)
    assert exc.value.status_code == 400


def test_python_runner_scoping_and_signature(tmp_path):
    """Verify Python runner scoping: single scope, no TypeError masking, clean sys.exit."""
    runner_script = os.path.abspath("daemon/runner/python_runner.py")

    def run_runner(payload):
        p = subprocess.Popen(
            ["python3", runner_script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = p.communicate(json.dumps(payload))
        return p.returncode, json.loads(stdout)

    # 1. Scoping: import math; def main(i): return math.sqrt(...)
    rc, res = run_runner({
        "code": "import math\ndef main(i):\n    return math.sqrt(25)",
        "inputs": {},
    })
    assert rc == 0
    assert res["output"] == 5.0

    # 2. Function using top-level helper function
    rc, res = run_runner({
        "code": "def helper(x):\n    return x * 2\ndef main(i):\n    return helper(21)",
        "inputs": {},
    })
    assert rc == 0
    assert res["output"] == 42

    # 3. TypeError inside main(i) is reported accurately and not re-run
    rc, res = run_runner({
        "code": "def main(i):\n    return 10 + 'string'",
        "inputs": {},
    })
    assert rc == 1
    assert "TypeError" in res["error"]

    # 4. sys.exit(0) returns valid JSON with exit_code 0
    rc, res = run_runner({
        "code": "import sys\nprint('finished')\nsys.exit(0)",
        "inputs": {},
    })
    assert rc == 0
    assert res["exit_code"] == 0
    assert "finished" in res["logs"]


@pytest.mark.asyncio
async def test_sqlite_backend_full_cycle(tmp_path):
    """Verify SQLiteLeaseBackend can be instantiated and all abstract methods execute."""
    db_file = str(tmp_path / "test_sqlite.db")
    backend = SQLiteLeaseBackend(db_file)

    # 1. create_or_renew_lease
    lease = await backend.create_or_renew_lease(
        session_id="s1",
        workspace_id="ws1",
        sandbox_id="sbx1",
        endpoint="http://127.0.0.1:5004",
        ttl_seconds=10,
        tenant_id="tenant-a",
    )
    assert lease.session_id == "s1"
    assert lease.status == "active"

    # 2. log_audit_event
    await backend.log_audit_event(session_id="s1", event_type="test_event", details={"foo": "bar"})

    # 3. claim_expired_leases
    claimed = await backend.claim_expired_leases(cutoff_time=time.time() + 20)
    assert len(claimed) == 1
    assert claimed[0].session_id == "s1"
    assert claimed[0].status == "reclaiming"

    # 4. Engine heartbeat and dead engine cleanup
    await backend.register_engine_heartbeat("eng-1")
    await backend.delete_dead_engine("eng-1")
    dead = await backend.get_dead_engines(max_idle_sec=0.01)
    assert "eng-1" not in dead

    # 5. Prune database records
    await backend.prune_database_records(retention_seconds=0.0)


@pytest.mark.asyncio
async def test_redis_backend_instantiation():
    """Verify RedisLeaseBackend can be instantiated and methods call redis mock."""
    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.sadd = AsyncMock(return_value=1)
    mock_redis.srem = AsyncMock(return_value=1)
    mock_redis.delete = AsyncMock(return_value=1)
    mock_redis.smembers = AsyncMock(return_value=[b"s1"])
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.lpush = AsyncMock(return_value=1)
    mock_redis.ltrim = AsyncMock(return_value=True)
    mock_redis.exists = AsyncMock(return_value=0)

    backend = RedisLeaseBackend(mock_redis)
    # Verify no abstract methods missing
    assert len(RedisLeaseBackend.__abstractmethods__) == 0

    await backend.register_engine_heartbeat("eng-test")
    mock_redis.set.assert_called()

    await backend.log_audit_event("s1", "event", {"k": "v"})
    mock_redis.lpush.assert_called()

    await backend.delete_dead_engine("eng-test")
    mock_redis.delete.assert_called()


@pytest.mark.asyncio
async def test_sweeper_skips_pending_leases():
    """Verify sweeper reconciler ignores sandbox_id == 'pending' without attempting to kill it."""
    mock_driver = AsyncMock()
    mock_driver.is_alive = AsyncMock(return_value=False)
    mock_driver.destroy_sandbox = AsyncMock()
    mock_driver.cleanup_orphaned_pvcs = AsyncMock(return_value=0)
    mock_driver.list_sandboxes = AsyncMock(return_value=[])

    mock_lease_mgr = AsyncMock()
    # Return a pending lease in list_active
    pending_lease = LeaseRecord(
        session_id="pending-sess",
        workspace_id="ws1",
        sandbox_id="pending",
        endpoint="pending",
        created_at=time.time(),
        expires_at=time.time() + 300,
        status="active",
        tenant_id="tenant-1",
    )
    mock_lease_mgr.claim_expired = AsyncMock(return_value=[])
    mock_lease_mgr.cleanup_expired_workspace_locks = AsyncMock()
    mock_lease_mgr.list_active = AsyncMock(return_value=[pending_lease])
    mock_lease_mgr.mark_expired = AsyncMock()
    mock_lease_mgr.register_engine_heartbeat = AsyncMock()
    mock_lease_mgr.get_dead_engines = AsyncMock(return_value=[])
    mock_lease_mgr.count_active_locks = AsyncMock(return_value=0)
    mock_lease_mgr.prune_database_records = AsyncMock()

    sweeper = Sweeper(driver=mock_driver, lease_manager=mock_lease_mgr, interval_sec=1.0)
    await sweeper.sweep_once()

    # Verify is_alive was NEVER called with "pending"
    mock_driver.is_alive.assert_not_called()
    # Verify pending lease was NOT marked expired
    mock_lease_mgr.mark_expired.assert_not_called()


def test_allow_insecure_docker_production_only_true_values():
    import os

    from sandbox_engine.config import _allow_insecure_docker_production

    os.environ["ALLOW_INSECURE_DOCKER_PRODUCTION"] = "0"
    assert not _allow_insecure_docker_production()
    os.environ["ALLOW_INSECURE_DOCKER_PRODUCTION"] = "1"
    assert _allow_insecure_docker_production()
    del os.environ["ALLOW_INSECURE_DOCKER_PRODUCTION"]


def test_unknown_environment_rejected():
    from sandbox_engine.config import EngineSettings

    s = EngineSettings(environment="live")
    with pytest.raises(ValueError, match="Unknown environment"):
        s.validate_environment_name()


def test_clamp_session_ttl_respects_absolute_lifetime():
    from sandbox_engine.core.ttl import clamp_session_ttl
    from sandbox_engine.models import LeaseRecord

    record = LeaseRecord(
        session_id="s1",
        workspace_id="w",
        sandbox_id="sb",
        endpoint="http://127.0.0.1:1",
        created_at=time.time() - 8000,
        expires_at=time.time() + 100,
    )
    assert clamp_session_ttl(record, 3600) == 1


@pytest.mark.asyncio
async def test_pvc_gc_does_not_raise_nameerror():
    from sandbox_engine.drivers.k8s_driver import KubernetesDriver

    driver = KubernetesDriver()
    driver._initialized = True
    driver._core_api = AsyncMock()
    driver._core_api.list_namespaced_persistent_volume_claim = AsyncMock(return_value=MagicMock(items=[]))
    driver._core_api.list_namespaced_pod = AsyncMock(return_value=MagicMock(items=[]))
    deleted = await driver.cleanup_orphaned_pvcs(set())
    assert deleted == 0
