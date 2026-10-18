#!/usr/bin/env python3
"""
Independent test suite for Phase 3:
Tests Stateful Sessions, Workspace Persistence, Sliding Heartbeats, and Sweeper GC.
"""

import asyncio
import time
import httpx
import pytest

from sandbox_engine.main import app
from sandbox_engine.models import SessionAcquireRequest


@pytest.mark.asyncio
async def test_stateful_workspace_persistence():
    print("\n--- Test 3.1: Workspace Persistence Across Restarts ---")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=30.0) as client:
            ws_id = "agent_project_persistence_test"

            # 1. Acquire Session 1
            print("1. Acquiring session 1...")
            resp1 = await client.post("/v1/sessions/acquire", json={
                "session_id": "sess_1",
                "workspace_id": ws_id,
                "ttl_seconds": 60,
            })
            assert resp1.status_code == 200, f"Acquire failed: {resp1.text}"
            data1 = resp1.json()
            assert data1["session_id"] == "sess_1"
            print(f"   Session 1 acquired. Sandbox: {data1['sandbox_id']}")

            # 2. Write file inside Session 1
            print("2. Writing code file into workspace...")
            write_resp = await client.post("/v1/sessions/sess_1/fs/write", json={
                "path": "calculator.py",
                "content": "def multiply(x, y):\n    return x * y\n",
            })
            assert write_resp.status_code == 200

            # 3. Execute file inside Session 1
            print("3. Executing file inside session 1...")
            exec_resp = await client.post("/v1/sessions/sess_1/exec", json={
                "command": "python3 -c 'import calculator; print(\"RESULT:\", calculator.multiply(6, 7))'",
                "stream": False,
            })
            assert exec_resp.status_code == 200
            assert "RESULT: 42" in exec_resp.json().get("stdout", "")
            print("   Executed successfully: RESULT: 42")

            # 4. Release Session 1 (destroys container, leaves workspace on disk)
            print("4. Releasing session 1 (destroying container)...")
            rel_resp = await client.post("/v1/sessions/sess_1/release")
            assert rel_resp.status_code == 200

            # 5. Acquire Session 2 on the same workspace_id
            print("5. Acquiring new session 2 on the same workspace...")
            resp2 = await client.post("/v1/sessions/acquire", json={
                "session_id": "sess_2",
                "workspace_id": ws_id,
                "ttl_seconds": 60,
            })
            assert resp2.status_code == 200
            data2 = resp2.json()
            assert data2["sandbox_id"] != data1["sandbox_id"], "New container should have been allocated"
            print(f"   Session 2 acquired. New sandbox: {data2['sandbox_id']}")

            # 6. Read back file from Session 2 - verify persistence!
            print("6. Verifying file persisted in session 2...")
            read_resp = await client.get("/v1/sessions/sess_2/fs/read?path=calculator.py")
            assert read_resp.status_code == 200
            assert "def multiply(x, y):" in read_resp.text
            print("   ✅ File survived container termination and was read successfully!")

            # Clean up
            await client.post("/v1/sessions/sess_2/release")


@pytest.mark.asyncio
async def test_ttl_expiry_and_sweeper():
    print("\n--- Test 3.2: TTL Expiration & Sweeper Auto-Reclaim ---")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=30.0) as client:
            # 1. Acquire session with short TTL = 2s
            print("1. Acquiring session with short TTL = 2 seconds...")
            resp = await client.post("/v1/sessions/acquire", json={
                "session_id": "sess_short_ttl",
                "workspace_id": "ws_short_ttl",
                "ttl_seconds": 2,
            })
            assert resp.status_code == 200
            sandbox_id = resp.json()["sandbox_id"]
            print(f"   Session acquired. Sandbox ID: {sandbox_id}")

            # Verify container is running
            driver = app.state.driver
            sandboxes = await driver.list_sandboxes()
            assert any(s.sandbox_id == sandbox_id for s in sandboxes), "Container should be running"

            # 2. Sleep for 3 seconds (past TTL)
            print("2. Sleeping for 3 seconds without heartbeats...")
            await asyncio.sleep(3.0)

            # 3. Trigger sweeper check
            print("3. Triggering sweeper cycle...")
            await app.state.sweeper.sweep_once()

            # 4. Verify container has been terminated by sweeper!
            sandboxes_after = await driver.list_sandboxes()
            still_running = any(s.sandbox_id == sandbox_id for s in sandboxes_after)
            assert not still_running, f"Container {sandbox_id} should have been destroyed by sweeper!"

            lease = await app.state.lease_manager.get("sess_short_ttl")
            assert lease.status == "expired", f"Lease status should be 'expired', got '{lease.status}'"
            print("   ✅ Sweeper successfully detected expiration and terminated the container!")


@pytest.mark.asyncio
async def test_sliding_heartbeat():
    print("\n--- Test 3.3: Sliding Heartbeat Extension ---")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=30.0) as client:
            # 1. Acquire session with TTL = 2s
            resp = await client.post("/v1/sessions/acquire", json={
                "session_id": "sess_heartbeat_test",
                "workspace_id": "ws_heartbeat",
                "ttl_seconds": 2,
            })
            data = resp.json()
            initial_expires = data["expires_at"]

            # 2. Wait 1 second and send heartbeat to extend by 4 seconds
            await asyncio.sleep(1.0)
            hb_resp = await client.post("/v1/sessions/sess_heartbeat_test/heartbeat", json={
                "ttl_seconds": 4
            })
            assert hb_resp.status_code == 200
            new_expires = hb_resp.json()["expires_at"]
            assert new_expires > initial_expires + 1.0, "Heartbeat should have extended expires_at"

            # 3. Wait another 1.5 seconds (past the initial 2s expiration, but before the new 4s expiration)
            await asyncio.sleep(1.5)
            await app.state.sweeper.sweep_once()

            # Verify session is still alive
            lease = await app.state.lease_manager.get("sess_heartbeat_test")
            assert lease.status == "active", "Session should still be active due to heartbeat"
            print("   ✅ Sliding heartbeat kept the session alive past original expiration!")

            # Clean up
            await client.post("/v1/sessions/sess_heartbeat_test/release")


if __name__ == "__main__":
    asyncio.run(test_stateful_workspace_persistence())
    asyncio.run(test_ttl_expiry_and_sweeper())
    asyncio.run(test_sliding_heartbeat())
    print("\n" + "=" * 60)
    print("🎉 ALL PHASE 3 STATEFUL & SWEEPER TESTS PASSED!")
    print("=" * 60)
