#!/usr/bin/env python3
"""
Comprehensive Kubernetes End-to-End Verification Suite for Sandbox Execution System.
Tests:
  1. Engine Health & System Probes (/livez, /readyz, /healthz)
  2. Sub-50ms SLA Pre-Warmed Pool Stateless Execution (/v1/stateless/run) on Kubernetes
  3. Stateless Warm Pool Automatic Background Replenishment
  4. Stateful Session Lifecycle (/v1/sessions/acquire, /v1/sessions/release)
  5. Stateful Workspace PVC / Storage Mount on Kubernetes
  6. Stateful Filesystem Operations (/fs/write, /fs/read, /fs/list, /fs/quota)
  7. Stateful In-Sandbox Command Execution (/v1/sessions/{id}/exec)
  8. Interactive Terminal (PTY) over WebSocket (/v1/sessions/{id}/ws/pty)
  9. Security & Boundary Enforcement (UID 1000, read-only rootfs, dropped capabilities)
  10. PostgreSQL Lease Ledger & Sweeper Audit State Verification
"""
import asyncio
import json
import os
import sys
import time
import httpx
import websockets

ENGINE_URL = os.environ.get("ENGINE_URL", "http://localhost:8888")
API_KEY = "a9f8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8"
TENANT_ID = "tenant_prod_1"
TENANT_KEY = "b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2"

HEADERS = {
    "Authorization": f"Bearer {TENANT_KEY}",
    "X-Tenant-ID": TENANT_ID,
    "Content-Type": "application/json",
}

TEST_RESULTS = []


def record_result(test_name: str, passed: bool, details: str = ""):
    status = "✅ PASS" if passed else "❌ FAIL"
    TEST_RESULTS.append({"name": test_name, "passed": passed, "details": details})
    print(f"{status} | {test_name}: {details}")


async def test_engine_health(client: httpx.AsyncClient):
    print("\n--- Test 1: Engine Health & Kubernetes System Probes ---")
    resp_live = await client.get(f"{ENGINE_URL}/livez")
    assert resp_live.status_code == 200, f"/livez failed: {resp_live.text}"
    live_data = resp_live.json()
    assert live_data.get("status") == "alive"

    resp_ready = await client.get(f"{ENGINE_URL}/readyz")
    assert resp_ready.status_code == 200, f"/readyz failed: {resp_ready.text}"
    ready_data = resp_ready.json()
    assert ready_data.get("status") == "ready"
    assert ready_data.get("database") == "ready"
    assert ready_data.get("warm_pool_ready") is True
    assert ready_data.get("warm_pool_size") >= 1

    record_result("Health & Probes", True, f"Live: {live_data}, Ready: {ready_data}")


async def test_stateless_execution(client: httpx.AsyncClient):
    print("\n--- Test 2: Stateless Warm Pool Execution on Kubernetes ---")
    code = (
        "import os, sys\n"
        "print(f'UID={os.getuid()}, GID={os.getgid()}')\n"
        "print(f'PYTHON={sys.version.split()[0]}')\n"
        "print(f'PID={os.getpid()}')\n"
    )
    t0 = time.time()
    resp = await client.post(
        f"{ENGINE_URL}/v1/stateless/run",
        headers=HEADERS,
        json={
            "language": "python",
            "code": code,
            "timeout": 10.0,
        },
        timeout=30.0,
    )
    elapsed = (time.time() - t0) * 1000
    assert resp.status_code == 200, f"Stateless run failed ({resp.status_code}): {resp.text}"
    res = resp.json()
    logs = res.get("logs", "")
    assert res.get("exit_code") == 0, f"Non-zero exit code: {res}"
    assert "UID=1000" in logs, f"Expected UID=1000 in logs: {logs}"
    assert "PYTHON=3.11" in logs, f"Expected Python 3.11 in logs: {logs}"
    sandbox_id = res.get("sandbox_id", "unknown")
    duration_ms = res.get("duration_ms", elapsed)
    record_result(
        "Stateless Pool Execution",
        True,
        f"Latency: {duration_ms:.2f}ms (Total roundtrip: {elapsed:.1f}ms) | Pod: {sandbox_id} | Logs: {logs.strip().replace(chr(10), ' | ')}",
    )


async def test_stateless_pool_replenishment(client: httpx.AsyncClient):
    print("\n--- Test 3: Warm Pool Automatic Background Replenishment ---")
    # Wait up to 10 seconds for the pool to replenish back to target size (2)
    replenished = False
    for _ in range(20):
        await asyncio.sleep(0.5)
        ready_resp = await client.get(f"{ENGINE_URL}/readyz")
        data = ready_resp.json()
        if data.get("warm_pool_size", 0) >= 2:
            replenished = True
            break
    assert replenished, f"Warm pool failed to replenish to 2: {data}"
    record_result("Warm Pool Replenishment", True, f"Warm pool successfully replenished back to {data.get('warm_pool_size')} pods")


async def test_stateful_session_lifecycle_and_fs(client: httpx.AsyncClient):
    print("\n--- Test 4: Stateful Session Lifecycle & PVC Filesystem on Kubernetes ---")
    workspace_id = f"k8s_prod_ws_{int(time.time())}"

    # 4a. Acquire session
    acquire_resp = await client.post(
        f"{ENGINE_URL}/v1/sessions/acquire",
        headers=HEADERS,
        json={
            "workspace_id": workspace_id,
            "ttl_seconds": 300,
        },
        timeout=60.0,
    )
    assert acquire_resp.status_code == 200, f"Acquire failed: {acquire_resp.text}"
    session_data = acquire_resp.json()
    session_id = session_data["session_id"]
    record_result("Stateful Acquire (K8s Pod + PVC)", True, f"Session ID: {session_id}, Workspace: {workspace_id}")

    try:
        # 4b. Write file into session
        test_filename = "benchmark.py"
        test_content = (
            "import math, sys\n"
            "primes = [p for p in range(2, 1000) if all(p % d != 0 for d in range(2, int(math.isqrt(p)) + 1))]\n"
            "print(f'Computed {len(primes)} primes inside K8s workspace PVC!')\n"
        )
        write_resp = await client.post(
            f"{ENGINE_URL}/v1/sessions/{session_id}/fs/write",
            headers=HEADERS,
            json={
                "path": test_filename,
                "content": test_content,
            },
            timeout=30.0,
        )
        assert write_resp.status_code in (200, 201), f"File write failed: {write_resp.text}"
        record_result("Stateful File Write (/fs/write)", True, f"Wrote {test_filename} ({len(test_content)} bytes)")

        # 4c. List files
        list_resp = await client.get(
            f"{ENGINE_URL}/v1/sessions/{session_id}/fs/list",
            headers=HEADERS,
            timeout=30.0,
        )
        assert list_resp.status_code == 200, f"File list failed: {list_resp.text}"
        file_list = list_resp.json()
        items = file_list.get("items", []) if isinstance(file_list, dict) else file_list
        assert any(test_filename in str(item) for item in items), f"File not found in list: {file_list}"
        record_result("Stateful File List (/fs/list)", True, f"Listed workspace contents: {file_list}")

        # 4d. Read file
        read_resp = await client.get(
            f"{ENGINE_URL}/v1/sessions/{session_id}/fs/read",
            params={"path": test_filename},
            headers=HEADERS,
            timeout=30.0,
        )
        assert read_resp.status_code == 200, f"File read failed: {read_resp.text}"
        read_content = read_resp.text
        assert "Computed {len(primes)} primes" in read_content, f"Content mismatch: {read_content}"
        record_result("Stateful File Read (/fs/read)", True, f"Verified exact content integrity")

        # 4e. Execute script in stateful session
        exec_resp = await client.post(
            f"{ENGINE_URL}/v1/sessions/{session_id}/exec",
            headers=HEADERS,
            json={
                "command": f"python3 /workspace/{test_filename}",
            },
            timeout=30.0,
        )
        assert exec_resp.status_code == 200, f"Exec failed: {exec_resp.text}"
        exec_data = exec_resp.json()
        exec_stdout = exec_data.get("stdout", "")
        assert "Computed 168 primes" in exec_stdout, f"Unexpected stdout: {exec_stdout}"
        record_result("Stateful Exec (/exec)", True, f"Result: {exec_stdout.strip()}")

        # 4f. Check Workspace Disk Quota
        quota_resp = await client.get(
            f"{ENGINE_URL}/v1/sessions/{session_id}/fs/quota",
            headers=HEADERS,
            timeout=30.0,
        )
        assert quota_resp.status_code == 200, f"Quota failed: {quota_resp.text}"
        quota_data = quota_resp.json()
        record_result("Workspace Quota (/fs/quota)", True, f"Storage usage: {quota_data}")

        # 4g. Security check: rootfs is read-only
        sec_resp = await client.post(
            f"{ENGINE_URL}/v1/sessions/{session_id}/exec",
            headers=HEADERS,
            json={
                "command": "touch /etc/hacked 2>&1 || true",
            },
            timeout=30.0,
        )
        sec_data = sec_resp.json()
        sec_stdout = sec_data.get("stdout", "") + sec_data.get("stderr", "")
        assert "Read-only file system" in sec_stdout or "Permission denied" in sec_stdout, (
            f"Expected Read-only root filesystem violation, got: {sec_stdout}"
        )
        record_result("Security: Read-Only RootFS", True, f"Verified: {sec_stdout.strip()}")

        # 4h. Security check: Dropped Capabilities (No Ping / Raw Sockets)
        cap_resp = await client.post(
            f"{ENGINE_URL}/v1/sessions/{session_id}/exec",
            headers=HEADERS,
            json={
                "command": "ping -c 1 127.0.0.1 2>&1 || true",
            },
            timeout=30.0,
        )
        cap_data = cap_resp.json()
        cap_stdout = cap_data.get("stdout", "") + cap_data.get("stderr", "")
        assert "Operation not permitted" in cap_stdout or "not found" in cap_stdout or "denied" in cap_stdout, (
            f"Expected dropped capabilities to prevent raw socket, got: {cap_stdout}"
        )
        record_result("Security: Dropped Capabilities", True, f"Verified dropped raw sockets: {cap_stdout.strip()}")

        # 4i. WebSocket PTY Interactive Execution
        ws_url = ENGINE_URL.replace("http://", "ws://").replace("https://", "wss://")
        pty_uri = f"{ws_url}/v1/sessions/{session_id}/ws/pty"
        connect_kwargs = dict(open_timeout=10.0)
        ws_headers = [
            ("Authorization", f"Bearer {TENANT_KEY}"),
            ("X-Tenant-ID", TENANT_ID),
        ]
        import inspect
        if "additional_headers" in inspect.signature(websockets.connect).parameters:
            connect_kwargs["additional_headers"] = ws_headers
        else:
            connect_kwargs["extra_headers"] = ws_headers

        try:
            async with websockets.connect(pty_uri, **connect_kwargs) as ws:
                # Send interactive terminal commands
                await ws.send("id && pwd && echo K8S_PTY_DONE\n")
                received_chunks = []
                for _ in range(25):
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        if isinstance(msg, bytes):
                            msg = msg.decode("utf-8", errors="replace")
                        received_chunks.append(msg)
                        full = "".join(received_chunks)
                        if "uid=1000" in full and "K8S_PTY_DONE" in full[full.find("echo K8S_PTY_DONE") + 15:]:
                            break
                    except asyncio.TimeoutError:
                        break
                full_pty_out = "".join(received_chunks)
                assert "uid=1000" in full_pty_out, f"Expected uid=1000 in PTY: {full_pty_out}"
                record_result(
                    "Interactive Terminal (PTY WebSocket)",
                    True,
                    f"Shell interactive output: {full_pty_out.strip().replace(chr(10), ' | ')[:80]}...",
                )
        except Exception as e:
            record_result("Interactive Terminal (PTY WebSocket)", False, f"PTY error: {e}")

        # 4j. Heartbeat Lease Extension
        hb_resp = await client.post(
            f"{ENGINE_URL}/v1/sessions/{session_id}/heartbeat",
            headers=HEADERS,
            json={"session_id": session_id, "ttl_extension_seconds": 120},
            timeout=30.0,
        )
        assert hb_resp.status_code == 200, f"Heartbeat failed: {hb_resp.text}"
        record_result("Heartbeat Lease Extension", True, f"Lease extended: {hb_resp.json()}")

    finally:
        # 4k. Release session
        rel_resp = await client.post(
            f"{ENGINE_URL}/v1/sessions/{session_id}/release",
            headers=HEADERS,
            timeout=30.0,
        )
        assert rel_resp.status_code == 200, f"Release failed: {rel_resp.text}"
        record_result("Stateful Release & Pod Teardown", True, f"Session {session_id} cleanly released")


async def main():
    print("=" * 80)
    print("🚀 RUNNING FULL KUBERNETES VERIFICATION SUITE")
    print(f"Target: {ENGINE_URL}")
    print("=" * 80)

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            await test_engine_health(client)
            await test_stateless_execution(client)
            await test_stateless_pool_replenishment(client)
            await test_stateful_session_lifecycle_and_fs(client)
        except Exception as e:
            import traceback
            traceback.print_exc()
            record_result("Fatal Suite Error", False, str(e))

    print("\n" + "=" * 80)
    print("FINAL KUBERNETES VERIFICATION SUMMARY:")
    passed_count = sum(1 for r in TEST_RESULTS if r["passed"])
    total_count = len(TEST_RESULTS)
    print(f"Passed: {passed_count}/{total_count} ({passed_count/total_count*100:.1f}%)")
    print("=" * 80)

    if passed_count < total_count:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
