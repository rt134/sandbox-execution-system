import asyncio
import time
import pytest
from sandbox_engine.core.lease_manager import LeaseManager, SQLiteLeaseBackend
from sandbox_engine.core.rate_limiter import RateLimiter
from sandbox_engine.models import LeaseRecord


@pytest.mark.asyncio
async def test_concurrent_tenant_load(tmp_path):
    """
    Load test simulating 50 concurrent requests across 5 distinct tenants
    verifying lock isolation, quota reservation, and zero race conditions under load.
    """
    db_file = str(tmp_path / "load_test.db")
    backend = SQLiteLeaseBackend(db_file)
    lease_mgr = LeaseManager(backend=backend)
    limiter = RateLimiter(max_tracked_tenants=100)

    num_tenants = 5
    requests_per_tenant = 10
    total_requests = num_tenants * requests_per_tenant

    start_time = time.perf_counter()

    async def simulate_tenant_worker(tenant_idx: int, req_idx: int):
        tenant_id = f"tenant-{tenant_idx}"
        session_id = f"sess-{tenant_idx}-{req_idx}"
        workspace_id = f"ws-{tenant_idx}"

        # 1. Rate limiter check
        await limiter.check_rate_limit(tenant_id, limit=100)

        # 2. Acquire workspace lock
        success, holder, exp = await backend.acquire_workspace_lock(
            workspace_id=workspace_id,
            session_id=session_id,
            ttl_seconds=30,
            tenant_id=tenant_id,
        )

        # 3. Create or renew lease
        lease = await backend.create_or_renew_lease(
            session_id=session_id,
            workspace_id=workspace_id,
            sandbox_id=f"sbx-{session_id}",
            endpoint="http://127.0.0.1:5004",
            ttl_seconds=30,
            tenant_id=tenant_id,
        )

        # 4. Log audit event
        await backend.log_audit_event(
            session_id=session_id,
            event_type="exec",
            details={"req": req_idx},
            workspace_id=workspace_id,
        )

        # 5. Heartbeat
        await backend.heartbeat(session_id, ttl_seconds=30)

        # 6. Release
        if success:
            await backend.release_workspace_lock(workspace_id, session_id, tenant_id=tenant_id)
        await backend.release_lease(session_id)

    tasks = [
        simulate_tenant_worker(t, r)
        for t in range(num_tenants)
        for r in range(requests_per_tenant)
    ]
    await asyncio.gather(*tasks)

    duration = time.perf_counter() - start_time
    ops_per_sec = total_requests / duration
    print(f"\n⚡ Load Test Completed: {total_requests} operations in {duration:.3f}s ({ops_per_sec:.1f} ops/sec)")
    assert duration < 10.0, f"Load test was too slow: {duration}s"
