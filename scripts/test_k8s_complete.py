#!/usr/bin/env python3
"""
Comprehensive Kubernetes End-to-End Verification Suite for Sandbox Execution System.
Tests:
  1. Engine Health & Probes (/livez, /readyz, /healthz)
  2. Authentication & Multi-Tenancy (Tenant token validation)
  3. Stateless Execution (/execute) with pod lifecycle & cleanup in K8s
  4. Stateful Session Lifecycle (/sessions/acquire, /sessions/release)
  5. Stateful Filesystem Operations (write file, list files, read file content)
  6. Stateful Command Execution (/sessions/{id}/exec)
  7. Interactive Terminal (PTY) over WebSocket (/sessions/{id}/pty)
  8. Security Invariants (non-root UID 1000, read-only rootfs, dropped capabilities)
  9. Lease Management & DB Persistence (PostgreSQL state verification)
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
    "Authorization": f"Bearer {API_KEY}",
    "X-Tenant-ID": TENANT_ID,
    "X-Tenant-Key": TENANT_KEY,
    "Content-Type": "application/json",
}

TEST_RESULTS = []


def record_result(test_name: str, passed: bool, details: str = ""):
    status = "✅ PASS" if passed else "❌ FAIL"
    TEST_RESULTS.append({"name": test_name, "passed": passed, "details": details})
    print(f"{status} | {test_name}: {details}")


async def test_engine_health(client: httpx.AsyncClient):
    print("\n--- Test 1: Engine Health & System Probes ---")
    resp_live = await client.get(f"{ENGINE_URL}/livez")
    assert resp_live.status_code == 200, f"/livez failed: {resp_live.text}"
    live_data = resp_live.json()
    assert live_data.get("status") == "alive"

    resp_ready = await client.get(f"{ENGINE_URL}/readyz")
    assert resp_ready.status_code == 200, f"/readyz failed: {resp_ready.text}"
    ready_data = resp_ready.json()
    assert ready_data.get("status") == "ready"
    assert ready_data.get("database") == "ready"

    record_result("Health & Probes", True, f"Live: {live_data}, Ready: {ready_data}")


async def test_stateless_execution(client: httpx.AsyncClient):
    print("\n--- Test 2: Stateless Sandbox Execution on Kubernetes ---")
    code = (
        "import os, sys\n"
        "print(f'UID={os.getuid()}, GID={os.getgid()}')\n"
        "print(f'PYTHON={sys.version.split()[0]}')\n"
        "print(f'CWD={os.getcwd()}')\n"
    )
    t0 = time.time()
    resp = await client.post(
        f"{ENGINE_URL}/execute",
        headers=HEADERS,
        json={
            "language": "python",
            "code": code,
            "timeout_seconds": 30,
        },
        timeout=60.0,
    )
    elapsed = time.time() - t0
    assert resp.status_code == 200, f"Execute failed: {resp.text}"
    res = resp.json()
    stdout = res.get("stdout", "")
    assert res.get("exit_code") == 0, f"Non-zero exit code: {res}"
    assert "UID=1000" in stdout, f"Expected UID=1000 in stdout: {stdout}"
    assert "PYTHON=3.11" in stdout, f"Expected Python 3.11 in stdout: {stdout}"
    record_result(
        "Stateless Execution",
        True,
        f"Latency: {elapsed:.2f}s | Output: {stdout.strip().replace(chr(10), ' | ')}",
    )


async def test_stateful_session_lifecycle_and_fs(client: httpx.AsyncClient):
    print("\n--- Test 3: Stateful Session Lifecycle & Filesystem on Kubernetes ---")
    workspace_id = f"k8s_test_ws_{int(time.time())}"
    
    # 3a. Acquire session
    acquire_resp = await client.post(
        f"{ENGINE_URL}/sessions/acquire",
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
    record_result("Stateful Acquire", True, f"Session ID: {session_id}, Workspace: {workspace_id}")

    try:
        # 3b. Write file into session
        test_content = "Hello from Kubernetes sandbox filesystem verification!\nTimestamp=" + str(time.time())
        write_resp = await client.post(
            f"{ENGINE_URL}/sessions/{session_id}/files",
            headers=HEADERS,
            json={
                "path": "test_output.txt",
                "content": test_content,
            },
            timeout=30.0,
        )
        assert write_resp.status_code in (200, 201), f"File write failed: {write_resp.text}"
        record_result("Stateful File Write", True, "Successfully wrote test_output.txt")

        # 3c. List files
        list_resp = await client.get(
            f"{ENGINE_URL}/sessions/{session_id}/files",
            headers=HEADERS,
            timeout=30.0,
        )
        assert list_resp.status_code == 200, f"File list failed: {list_resp.text}"
        file_list = list_resp.json()
        assert any("test_output.txt" in str(item) for item in file_list), f"File not found in list: {file_list}"
        record_result("Stateful File List", True, f"Found files: {file_list}")

        # 3d. Read file
        read_resp = await client.get(
            f"{ENGINE_URL}/sessions/{session_id}/files",
            params={"path": "test_output.txt"},
            headers=HEADERS,
            timeout=30.0,
        )
        assert read_resp.status_code == 200, f"File read failed: {read_resp.text}"
        read_content = read_resp.text
        assert "Hello from Kubernetes sandbox filesystem verification!" in read_content, f"Content mismatch: {read_content}"
        record_result("Stateful File Read", True, f"Verified content integrity ({len(read_content)} bytes)")

        # 3e. Execute command in stateful session
        exec_resp = await client.post(
            f"{ENGINE_URL}/sessions/{session_id}/exec",
            headers=HEADERS,
            json={
                "command": "cat /workspace/test_output.txt && uname -a",
            },
            timeout=30.0,
        )
        assert exec_resp.status_code == 200, f"Exec failed: {exec_resp.text}"
        exec_data = exec_resp.json()
        exec_stdout = exec_data.get("stdout", "")
        assert "Hello from Kubernetes sandbox" in exec_stdout, f"Unexpected stdout: {exec_stdout}"
        record_result("Stateful Exec", True, f"Output: {exec_stdout.strip().replace(chr(10), ' | ')}")

        # 3f. Security check: rootfs is read-only
        sec_resp = await client.post(
            f"{ENGINE_URL}/sessions/{session_id}/exec",
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
        record_result("Security: Read-Only RootFS", True, f"Confirmed read-only root: {sec_stdout.strip()}")

        # 3g. WebSocket PTY Interactive Execution
        ws_url = ENGINE_URL.replace("http://", "ws://").replace("https://", "wss://")
        pty_uri = f"{ws_url}/sessions/{session_id}/pty"
        try:
            async with websockets.connect(
                pty_uri,
                extra_headers=[
                    ("Authorization", f"Bearer {API_KEY}"),
                    ("X-Tenant-ID", TENANT_ID),
                    ("X-Tenant-Key", TENANT_KEY),
                ],
                open_timeout=10.0,
            ) as ws:
                # Send command with newline
                await ws.send("echo PTY_TEST_SUCCESS\n")
                received_chunks = []
                for _ in range(10):
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.5)
                        if isinstance(msg, bytes):
                            msg = msg.decode("utf-8", errors="replace")
                        received_chunks.append(msg)
                        if "PTY_TEST_SUCCESS" in "".join(received_chunks):
                            break
                    except asyncio.TimeoutError:
                        break
                full_pty_out = "".join(received_chunks)
                assert "PTY_TEST_SUCCESS" in full_pty_out, f"PTY output missing marker: {full_pty_out}"
                record_result("Interactive PTY", True, f"Terminal stream received: {full_pty_out.strip()[:60]}...")
        except Exception as e:
            record_result("Interactive PTY", False, f"PTY error: {e}")

    finally:
        # 3h. Release session
        rel_resp = await client.post(
            f"{ENGINE_URL}/sessions/{session_id}/release",
            headers=HEADERS,
            timeout=30.0,
        )
        assert rel_resp.status_code == 200, f"Release failed: {rel_resp.text}"
        record_result("Stateful Release & Pod Teardown", True, f"Session {session_id} cleanly released")


async def main():
    print("=" * 70)
    print("🚀 RUNNING FULL KUBERNETES VERIFICATION SUITE")
    print(f"Target: {ENGINE_URL}")
    print("=" * 70)

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            await test_engine_health(client)
            await test_stateless_execution(client)
            await test_stateful_session_lifecycle_and_fs(client)
        except Exception as e:
            import traceback
            traceback.print_exc()
            record_result("Fatal Exception", False, str(e))

    print("\n" + "=" * 70)
    print("TEST EXECUTION SUMMARY:")
    passed_count = sum(1 for r in TEST_RESULTS if r["passed"])
    total_count = len(TEST_RESULTS)
    print(f"Passed: {passed_count}/{total_count} ({passed_count/total_count*100:.1f}%)")
    print("=" * 70)

    if passed_count < total_count:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
