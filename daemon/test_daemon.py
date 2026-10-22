#!/usr/bin/env python3
"""
Independent test suite for Phase 1: In-Sandbox Daemon (shellctl).
Tests /health, /execute, /exec, /fs/*, and /ws/pty against a running container.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

try:
    import pytest
except ImportError:
    pytest = None

BASE_URL = os.environ.get("SHELLCTL_BASE_URL", "http://localhost:5004")
AUTH_TOKEN = os.environ.get("SHELLCTL_AUTH_TOKEN", "")

# If run via pytest and daemon is not running, skip
if pytest:
    try:
        req = urllib.request.Request(f"{BASE_URL}/health")
        with urllib.request.urlopen(req, timeout=0.5):
            pass
    except Exception:
        pytestmark = pytest.mark.skip(reason=f"Standalone shellctl daemon not reachable on {BASE_URL}")

def log_test(name, passed, detail=""):
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"[{status}] {name} {detail}")
    if not passed:
        sys.exit(1)

def _auth_headers(extra=None):
    h = dict(extra or {})
    if AUTH_TOKEN:
        h["Authorization"] = f"Bearer {AUTH_TOKEN}"
    return h

def http_post(path, data):
    headers = _auth_headers({"Content-Type": "application/json"})
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=json.dumps(data).encode("utf-8"),
        headers=headers,
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))

def http_get(path):
    headers = _auth_headers()
    req = urllib.request.Request(f"{BASE_URL}{path}", headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))

def test_health():
    resp = http_get("/health")
    passed = resp.get("status") == "ok" and "uptime_sec" in resp
    log_test("GET /health", passed, f"(uptime: {resp.get('uptime_sec'):.2f}s, go: {resp.get('go_version')})")

def test_execute_stateless():
    # 1. Pure computation
    code = """
def main(inputs):
    print("User print log message")
    return {"doubled": inputs["val"] * 2}
"""
    t0 = time.time()
    resp = http_post("/execute", {
        "language": "python",
        "code": code,
        "inputs": {"val": 21},
        "timeout": 5.0
    })
    total_time_ms = (time.time() - t0) * 1000

    passed = (
        resp.get("output", {}).get("doubled") == 42 and
        "User print log message" in resp.get("logs", "") and
        resp.get("exit_code") == 0
    )
    log_test(
        "POST /execute (Stateless Python)", 
        passed, 
        f"(duration: {resp.get('duration_ms'):.2f}ms, roundtrip: {total_time_ms:.2f}ms, output: {resp.get('output')})"
    )

def test_execute_error_handling():
    code = """
def main(inputs):
    raise ValueError("Intentional execution error")
"""
    resp = http_post("/execute", {
        "language": "python",
        "code": code,
        "inputs": {},
    })
    passed = resp.get("exit_code") == 1 and "Intentional execution error" in (resp.get("error") or "")
    log_test("POST /execute (Error Handling)", passed)

def test_exec_buffered():
    resp = http_post("/exec", {
        "command": "python3 --version && node --version && whoami",
        "stream": False
    })
    stdout = resp.get("stdout", "")
    passed = (
        "Python 3.11" in stdout and
        "v20." in stdout and
        "sandbox" in stdout and
        resp.get("exit_code") == 0
    )
    log_test("POST /exec (Buffered Mode)", passed, f"(output: {stdout.strip().replace(chr(10), ' | ')})")

def test_filesystem():
    # 1. Write file
    write_resp = http_post("/fs/write", {
        "path": "test_dir/hello.py",
        "content": "def greet(name):\n    return f'Hello {name}'\n"
    })
    log_test("POST /fs/write", write_resp.get("status") == "ok")

    # 2. List directory
    list_resp = http_get("/fs/list?path=test_dir")
    items = list_resp.get("items", [])
    found = any(item.get("name") == "hello.py" for item in items)
    log_test("GET /fs/list", found, f"(found {len(items)} items)")

    # 3. Read file
    req = urllib.request.Request(f"{BASE_URL}/fs/read?path=test_dir/hello.py", headers=_auth_headers())
    with urllib.request.urlopen(req) as resp:
        content = resp.read().decode("utf-8")
        log_test("GET /fs/read", "Hello {name}" in content)

    # 4. Path traversal prevention (Security)
    traversal_blocked = False
    try:
        http_post("/fs/write", {
            "path": "../../etc/evil.txt",
            "content": "malicious"
        })
    except urllib.error.HTTPError as e:
        traversal_blocked = (e.code == 403)
    log_test("Security: Path Traversal Prevention", traversal_blocked, "(Access Denied as expected)")

    # 5. Delete file
    del_resp = http_post("/fs/delete", {
        "path": "test_dir",
        "recursive": True
    })
    log_test("POST /fs/delete", del_resp.get("status") == "deleted")

import asyncio
import websockets

async def async_test_pty():
    ws_base = BASE_URL.replace("http://", "ws://").replace("https://", "wss://")
    uri = f"{ws_base}/ws/pty"
    ws_kwargs = {}
    if AUTH_TOKEN:
        import inspect
        params = inspect.signature(websockets.connect).parameters
        if "additional_headers" in params:
            ws_kwargs["additional_headers"] = {"Authorization": f"Bearer {AUTH_TOKEN}"}
        else:
            ws_kwargs["extra_headers"] = [("Authorization", f"Bearer {AUTH_TOKEN}")]
    async with websockets.connect(uri, **ws_kwargs) as ws:
        # 1. Send window resize event
        resize_msg = json.dumps({"type": "resize", "cols": 100, "rows": 30})
        await ws.send(resize_msg)

        # 2. Send interactive command
        await ws.send(b"echo PTY_INTERACTIVE_OK\n")

        # 3. Read output frames
        received_output = ""
        for _ in range(10):
            try:
                frame = await asyncio.wait_for(ws.recv(), timeout=2.0)
                if isinstance(frame, bytes):
                    received_output += frame.decode("utf-8", errors="ignore")
                else:
                    received_output += str(frame)
                if "PTY_INTERACTIVE_OK" in received_output:
                    break
            except asyncio.TimeoutError:
                break

        passed = "PTY_INTERACTIVE_OK" in received_output
        log_test("GET /ws/pty (Interactive WebSocket PTY)", passed, f"(received PTY response)")

def test_exec_streaming():
    req = urllib.request.Request(
        f"{BASE_URL}/exec",
        data=json.dumps({
            "command": "for i in 1 2 3; do echo Chunk-$i; sleep 0.05; done",
            "stream": True
        }).encode("utf-8"),
        headers=_auth_headers({"Content-Type": "application/json"}),
    )
    chunks = []
    with urllib.request.urlopen(req, timeout=10) as resp:
        for line in resp:
            line_str = line.decode("utf-8").strip()
            if line_str:
                chunks.append(json.loads(line_str))

    texts = [c.get("text", "") for c in chunks if "text" in c]
    has_exit = any("exit_code" in c and c["exit_code"] == 0 for c in chunks)
    all_text = "".join(texts)
    passed = "Chunk-1" in all_text and "Chunk-2" in all_text and "Chunk-3" in all_text and has_exit
    log_test("POST /exec (Chunked Streaming Mode)", passed, f"(received {len(chunks)} NDJSON chunks)")

def main():
    print("=" * 60)
    print("Running Phase 1 Standalone In-Sandbox Daemon Verification")
    print("=" * 60)
    time.sleep(1) # Ensure daemon has settled
    test_health()
    test_execute_stateless()
    test_execute_error_handling()
    test_exec_buffered()
    test_exec_streaming()
    asyncio.run(async_test_pty())
    test_filesystem()
    print("=" * 60)
    print("🎉 ALL PHASE 1 CHECKS PASSED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    main()
