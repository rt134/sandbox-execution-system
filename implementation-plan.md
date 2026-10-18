# Sandbox Execution System: 4-Phase Implementation Plan

This document outlines the detailed engineering roadmap for developing the **Sandbox Execution System**. The development is divided into **4 sequential, independently testable phases**. Each phase has its own deliverables, verification criteria, and test commands that must pass before advancing to the next phase.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        4-PHASE IMPLEMENTATION ROADMAP                        │
├──────────────────────────────────────────────────────────────────────────────┤
│ Phase 1: In-Sandbox Daemon (shellctl) & Base Container Image                 │
│          • Testable by: Running container standalone & hitting :5004 directly│
├──────────────────────────────────────────────────────────────────────────────┤
│ Phase 2: sandbox-engine Service, Dual Drivers & Warm Pool (Stateless Mode)   │
│          • Testable by: Benchmark script measuring <50ms stateless execution │
├──────────────────────────────────────────────────────────────────────────────┤
│ Phase 3: Stateful Sessions, Lease Manager, Sweeper & SSRF Egress Proxy       │
│          • Testable by: TTL expiry test, workspace persistence & SSRF blocks │
├──────────────────────────────────────────────────────────────────────────────┤
│ Phase 4: sandbox-client Python SDK & End-to-End Suite                        │
│          • Testable by: Pytest suite verifying DAG nodes & Pydantic AI tools │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: In-Sandbox Daemon (`shellctl`) & Base Container Image

### 1.1 Objective
Build the self-contained execution kernel that runs inside every container/Pod. It listens on port `5004` and exposes sub-50ms stateless execution, streaming process execution, native POSIX PTY, and filesystem APIs.

### 1.2 Deliverables
1. **`daemon/` (Go static binary `shellctl`):**
   * Statically compiled Go binary (`CGO_ENABLED=0`) with `<8MB` RAM footprint.
   * Internal HTTP & WebSocket server listening on `:5004`:
     * `GET  /health`: Health probe.
     * `POST /execute`: Sub-50ms stateless code runner (accepts `{language, code, inputs, timeout}`, executes via Python subprocess with tmpfs isolation, returns JSON `{output, logs, exit_code, duration_ms}`).
     * `POST /exec`: Stateful process execution (`bash`, `pytest`, `git`) with chunked streaming stdout/stderr.
     * `GET  /ws/pty`: Native interactive POSIX PTY terminal over WebSocket using `creack/pty` with full `SIGWINCH` resize handling.
     * `POST /fs/write`: File upload/write to `/workspace`.
     * `GET  /fs/read`: Stream file content from `/workspace`.
     * `GET  /fs/list`: Directory listing with file metadata.
     * `POST /fs/delete`: File/directory deletion.
2. **`daemon/Dockerfile`:**
   * Multi-stage build (`golang:alpine` builder $\rightarrow$ minimal Ubuntu/Debian base).
   * Installs Python 3.11+, Node.js, git, curl, bash.
   * Creates non-root user `sandbox` (UID 1000, GID 1000) with permissions on `/workspace`.
   * Sets `/usr/local/bin/shellctl` as the default entrypoint.

### 1.3 Independent Verification & Tests (Exit Gate)
* **Test 1.1 (Go Unit Tests):**
  ```bash
  cd daemon && go test -v ./...
  ```
* **Test 1.2 (Container Standalone Run):**
  ```bash
  docker build -t sandbox-base:latest daemon/
  docker run -d -p 5004:5004 --name test-sandbox sandbox-base:latest
  ```
* **Test 1.3 (Stateless Execution Verification):**
  ```bash
  curl -X POST http://localhost:5004/execute \
    -H "Content-Type: application/json" \
    -d '{"language": "python", "code": "def main(inputs): return {\"res\": inputs[\"x\"] * 2}", "inputs": {"x": 21}}'
  # Expected: {"output": {"res": 42}, "duration_ms": <20, "exit_code": 0}
  ```
* **Test 1.4 (Filesystem & Streaming Exec Verification):**
  * Write a file via `/fs/write`, read it back via `/fs/read`, run `cat /workspace/<file>` via `/exec`.
* **Test 1.5 (Interactive PTY Verification):**
  * Run a test script opening `ws://localhost:5004/ws/pty`, send terminal commands, verify raw terminal response.

---

## Phase 2: `sandbox-engine` Service, Pluggable Drivers & Warm Pool (Stateless Mode)

### 2.1 Objective
Build the FastAPI control plane service that manages the container lifecycle across both Docker and Kubernetes, maintaining a warm pool to guarantee the `<50ms` stateless SLA.

### 2.2 Deliverables
1. **Abstract Container Driver Layer:**
   * `sandbox-engine/src/sandbox_engine/drivers/base.py`:
     ```python
     class ContainerDriver(ABC):
         async def create_sandbox(self, spec: SandboxSpec) -> SandboxInstance: ...
         async def destroy_sandbox(self, sandbox_id: str) -> None: ...
         async def get_sandbox_endpoint(self, sandbox_id: str) -> str: ...
         async def list_sandboxes(self, labels: dict[str, str]) -> list[SandboxSummary]: ...
     ```
2. **`DockerDriver`:**
   * Uses Docker Engine API over `/var/run/docker.sock`.
   * Configurable runtime (`runsc` for gVisor, or fallback `runc` for macOS/local dev).
   * Attaches containers to internal bridge network.
3. **`KubernetesDriver`:**
   * Uses `kubernetes_asyncio` to manage Pods in the `sandboxes` namespace.
   * Schedules on tainted node pool (`sandbox=true:NoSchedule`) with `runtimeClassName: gvisor`.
   * Exposes direct Pod IP for `<15ms` internal network hops (bypassing slow `kube-apiserver`).
4. **`WarmPoolManager`:**
   * In-memory queue maintaining $N$ pre-spawned idle sandboxes.
   * "Replenish-on-consume" loop: on execution, pops a warm container, dispatches code directly to `http://<sandbox-ip>:5004/execute`, destroys the consumed container asynchronously, and triggers a background worker to spawn a replacement.
5. **FastAPI Route:**
   * `POST /v1/stateless/run`: Stateless execution endpoint.

### 2.3 Independent Verification & Tests (Exit Gate)
* **Test 2.1 (Driver Abstraction Test):**
  * Unit tests validating driver instantiation and behavior under both `SANDBOX_DRIVER=docker` and `SANDBOX_DRIVER=k8s`.
* **Test 2.2 (Warm Pool Latency Benchmark):**
  * Start engine with `SANDBOX_DRIVER=docker` and `WARM_POOL_SIZE=3`.
  * Run automated benchmark executing 10 sequential stateless Python tasks.
  * **Pass criteria:** 90th percentile latency is strictly **`< 50ms`**.
* **Test 2.3 (Background Replenish Verification):**
  * Validate that warm pool size remains at target capacity after burst calls without blocking callers.

---

## Phase 3: Stateful Sessions, Lease Manager, Sweeper & SSRF Proxy

### 3.1 Objective
Implement persistent agent sessions with workspace volume mounting, sliding-window TTL heartbeats, an autonomous background sweeper for leak prevention, and strict SSRF egress firewalling.

### 3.2 Deliverables
1. **`LeaseManager`:**
   * Redis-backed session ledger (with local SQLite fallback for standalone development).
   * Key schema: `lease:session:{id}` storing `workspace_id`, `container_id`, `expires_at`.
   * Sliding heartbeat logic (`heartbeat(session_id, ttl=300)`).
2. **`Sweeper` (Garbage Collector):**
   * Background asyncio task running every 15 seconds.
   * Reconciles active containers/pods from the driver against active leases in Redis.
   * Terminates orphan/expired containers while keeping `/data/workspaces/{id}` intact on disk.
3. **Stateful FastAPI Routes:**
   * `POST /v1/sessions/acquire`: Creates or attaches to a stateful workspace session.
   * `POST /v1/sessions/{id}/heartbeat`: Extends session TTL.
   * `POST /v1/sessions/{id}/release`: Explicit session termination.
   * `GET  /v1/sessions/{id}/ws/pty`: Reverse-proxies WebSocket connection directly to the container's `shellctl` PTY.
4. **SSRF Egress Proxy (Squid):**
   * `deploy/squid/squid.conf`: Blocks AWS/GCP metadata (`169.254.169.254`), private RFC1918 subnets (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `127.0.0.0/8`), and internal K8s API server.
   * `deploy/docker-compose.yml` and `deploy/k8s/` manifests wiring sandboxes through Squid.

### 3.3 Independent Verification & Tests (Exit Gate)
* **Test 3.1 (Workspace Disk Persistence):**
  * Acquire session `sess_1` with `workspace_id="test_ws"`.
  * Write files to `/workspace/test.py`.
  * Release `sess_1`.
  * Acquire session `sess_2` with the same `workspace_id="test_ws"`.
  * Verify `/workspace/test.py` is present and unchanged.
* **Test 3.2 (Lease Expiry & Sweeper GC):**
  * Acquire a session with `ttl_seconds=5`.
  * Wait 16 seconds without sending heartbeats.
  * Verify the sweeper terminates the container and marks the ledger status as `EXPIRED`.
* **Test 3.3 (SSRF Egress Firewall Test):**
  * Execute `curl -s -m 2 http://169.254.169.254/latest/meta-data/` inside the container.
  * Verify the connection is blocked with `403 Forbidden` from Squid.

---

## Phase 4: `sandbox-client` Python SDK & End-to-End Test Suite

### 4.1 Objective
Package a clean, lightweight asynchronous Python SDK (`httpx` + `websockets`) that can be installed (`pip install -e sandbox-client`) and used by DAG nodes and Autonomous Agents.

### 4.2 Deliverables
1. **`sandbox-client/`:**
   * `client.py`: `SandboxClient(base_url="http://sandbox-engine:8080")`.
   * `session.py`: `SandboxSession` async context manager (`session.fs`, `session.exec_stream`, `session.terminal`).
   * `fs.py`: File operations client (`write`, `read`, `list`, `delete`).
   * `terminal.py`: Interactive PTY WebSocket connection (`send`, `resize`, `recv`).
   * `models.py`: Strongly-typed Pydantic models (`CodeResult`, `ExecChunk`).
   * `adapters/`: Prebuilt Pydantic AI tool functions (`run_bash`, `read_file`, `write_file`) and DAG node runner adapter.
2. **`tests/e2e/`:**
   * Full end-to-end integration test suite.

### 4.3 Independent Verification & Tests (Exit Gate)
* **Test 4.1 (SDK Unit Tests):**
  ```bash
  pytest sandbox-client/tests/
  ```
* **Test 4.2 (Stateless DAG Node Flow):**
  ```python
  result = await client.run_code(
      language="python",
      code="def main(inputs): return {'doubled': inputs['val'] * 2}",
      inputs={"val": 21}
  )
  assert result.output == {"doubled": 42}
  assert result.duration_ms < 50
  ```
* **Test 4.3 (Autonomous Agent Coding Flow):**
  ```python
  async with client.acquire_session(workspace_id="test_repo", ttl_seconds=60) as session:
      await session.fs.write("/workspace/math_lib.py", "def add(a, b): return a + b\n")
      await session.fs.write("/workspace/test_math.py", "from math_lib import add\ndef test_add(): assert add(2, 3) == 5\n")
      output = ""
      async for chunk in session.exec_stream("pytest test_math.py"):
          output += chunk.text
      assert "1 passed" in output
  ```
* **Test 4.4 (Interactive PTY Flow):**
  * Open PTY terminal via SDK, write commands, read interactive output chunks.

---

## Phase Transition Checklist

| Phase | Core Deliverable | Primary Test Command | Status |
| :--- | :--- | :--- | :--- |
| **Phase 1** | `shellctl` daemon + Base Dockerfile | `docker run` & direct `curl :5004` | ✅ Completed |
| **Phase 2** | `sandbox-engine` + Warm Pool + Drivers | Latency benchmark (`<50ms` target) | ✅ Completed |
| **Phase 3** | Stateful Sessions + Sweeper + Squid | TTL expiry & SSRF block test | ✅ Completed |
| **Phase 4** | `sandbox-client` SDK + E2E Suite | `pytest tests/e2e` | ✅ Completed |
