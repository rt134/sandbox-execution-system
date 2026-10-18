# REST & WebSocket API Reference (`sandbox-engine`)

The `sandbox-engine` control plane exposes a unified HTTP and WebSocket interface on port `8080` to manage both stateless executions and persistent agent sessions.

**Base URL:** `http://<sandbox-engine-host>:8080`

---

## 0. Authentication & Security

When `SANDBOX_API_KEY` is configured on the engine, all REST endpoints and WebSocket handshakes require authentication.

### Supported Authentication Methods:
* **HTTP Bearer Token:** `Authorization: Bearer <your-api-key>`
* **HTTP Custom Header:** `X-API-Key: <your-api-key>`
* **WebSocket Query Parameter:** `ws://host:8080/v1/sessions/{id}/ws/pty?token=<your-api-key>`

Unauthenticated requests receive `401 Unauthorized` with `{"detail": "Missing or invalid API key"}`. All token comparisons are executed using constant-time algorithms to prevent timing attacks.

---

## 1. Stateless Code Execution

### `POST /v1/stateless/run`
Executes untrusted code with sub-50ms latency using a pre-warmed container from the memory pool. Immediately destroys the consumed container in the background and restores the pool.

#### Request Headers:
* `Content-Type: application/json`

#### Request Body:
```json
{
  "language": "python",
  "code": "def main(inputs):\n    return {'result': inputs['x'] * 2}",
  "inputs": {
    "x": 21
  },
  "timeout": 10.0
}
```

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `language` | `string` | No (default: `"python"`) | Execution runtime: `"python"`, `"node"`, `"bash"`. |
| `code` | `string` | **Yes** | Source code to execute. |
| `inputs` | `object` | No | Key-value dictionary passed to `main(inputs)`. |
| `timeout` | `float` | No (default: `10.0`) | Execution deadline in seconds (max 300). |

#### Response (`200 OK`):
```json
{
  "output": {
    "result": 42
  },
  "logs": "Execution logs and print statements\n",
  "error": null,
  "exit_code": 0,
  "duration_ms": 16.05,
  "sandbox_id": "029016de581f"
}
```

#### cURL Example:
```bash
curl -X POST http://localhost:8080/v1/stateless/run \
  -H "Content-Type: application/json" \
  -d '{
    "language": "python",
    "code": "def main(inputs): return {\"message\": f\"Hello, {inputs.get(\"name\", \"world\")}!\"}",
    "inputs": {"name": "Alice"}
  }'
```

---

## 2. Stateful Agent Sessions

### `POST /v1/sessions/acquire`
Creates a new stateful session or reattaches to an existing active session. Mounts a persistent workspace volume into `/workspace`.

#### Request Body:
```json
{
  "session_id": "sess_custom_id_123",
  "workspace_id": "repo_482",
  "ttl_seconds": 300
}
```

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `session_id` | `string` | No | Unique session ID. If omitted, an ID is generated automatically. |
| `workspace_id` | `string` | **Yes** | Identifier of the workspace directory to mount into `/workspace`. |
| `ttl_seconds` | `integer` | No (default: `300`) | Inactivity timeout in seconds before auto-teardown. |

#### Response (`200 OK`):
```json
{
  "session_id": "sess_custom_id_123",
  "workspace_id": "repo_482",
  "sandbox_id": "a93863b4a31b",
  "endpoint": "http://127.0.0.1:49940",
  "expires_at": 1729272300.5,
  "status": "active"
}
```

#### Error Responses:
* **`400 Bad Request`**: `workspace_id` failed validation or attempted path traversal.
  ```json
  {"detail": "Invalid workspace_id: must match ^[a-zA-Z0-9_-]{1,64}$"}
  ```
* **`409 Conflict`**: Workspace concurrency lock conflict. The workspace is actively locked by another session.
  ```json
  {"detail": "Workspace 'repo_482' is currently locked by active session 'sess_other_999'"}
  ```

---

### `POST /v1/sessions/{session_id}/heartbeat`
Sends a sliding heartbeat to extend the session lease expiration.

#### Request Body:
```json
{
  "ttl_seconds": 300
}
```

#### Response (`200 OK`):
```json
{
  "session_id": "sess_custom_id_123",
  "expires_at": 1729272600.0,
  "status": "active"
}
```

---

### `POST /v1/sessions/{session_id}/release`
Explicitly releases and terminates the sandbox container. Files in `/workspace` remain preserved on disk.

#### Response (`200 OK`):
```json
{
  "status": "released",
  "session_id": "sess_custom_id_123"
}
```

---

## 3. Proxied Execution & File System

Every call to these endpoints **automatically refreshes the session lease**.

### `POST /v1/sessions/{session_id}/exec`
Executes a shell command inside the session's workspace.

#### Request Body:
```json
{
  "command": "pytest test_calc.py",
  "cwd": "/workspace",
  "env": {
    "ENV_VAR": "production"
  },
  "timeout": 300.0,
  "stream": false
}
```

#### Buffered Response (`stream: false`):
```json
{
  "stdout": "============================= test session starts ==============================\n1 passed in 0.03s\n",
  "stderr": "",
  "exit_code": 0,
  "duration_ms": 42.10,
  "error": null
}
```

#### Streaming Response (`stream: true`):
* `Content-Type: application/x-ndjson`
Emits NDJSON chunks line-by-line as output arrives:
```json
{"stream": "stdout", "text": "============================= test session starts ==============================\n"}
{"stream": "stdout", "text": "collected 1 item\n"}
{"stream": "stdout", "text": "test_calc.py . [100%]\n"}
{"exit_code": 0, "error": ""}
```

---

### `POST /v1/sessions/{session_id}/fs/write`
Creates or overwrites a file in the workspace. Validates that the write will not exceed the workspace disk quota.

#### Request Body:
```json
{
  "path": "calculator/main.py",
  "content": "def add(a, b): return a + b\n",
  "is_base64": false
}
```

#### Response (`200 OK`):
```json
{
  "path": "calculator/main.py",
  "bytes_written": 32
}
```

#### Error Response:
* **`413 Payload Too Large`**: Workspace disk quota exceeded.
  ```json
  {"detail": "Disk quota exceeded: write would cause workspace to exceed 1024MB limit"}
  ```

---

### `GET /v1/sessions/{session_id}/fs/quota`
Inspects workspace disk storage consumption against the configured disk quota.

#### Response (`200 OK`):
```json
{
  "used_bytes": 1048576,
  "used_mb": 1.0,
  "quota_bytes": 1073741824,
  "quota_mb": 1024.0,
  "available_bytes": 1072693248,
  "available_mb": 1023.0,
  "usage_percent": 0.1
}
```

---

### `GET /v1/sessions/{session_id}/fs/read`
Streams raw file contents from the workspace.

#### Query Parameters:
* `path` *(required)*: Relative path to the file inside `/workspace`.

#### Response (`200 OK`):
* `Content-Type: application/octet-stream`
* Returns raw file bytes.

---

### `GET /v1/sessions/{session_id}/fs/list`
Lists directory contents with file metadata.

#### Query Parameters:
* `path` *(optional, default: `.`)*: Directory to list.

#### Response (`200 OK`):
```json
{
  "path": "/workspace/calculator",
  "items": [
    {
      "name": "main.py",
      "path": "calculator/main.py",
      "size": 32,
      "is_dir": false,
      "mode": "-rw-r--r--",
      "modified_at": "2026-10-18T16:00:00Z"
    }
  ]
}
```

---

### `POST /v1/sessions/{session_id}/fs/delete`
Deletes a file or directory from the workspace.

#### Request Body:
```json
{
  "path": "temp_file.txt",
  "recursive": false
}
```

---

## 4. WebSocket Interactive PTY Terminal

### `WS /v1/sessions/{session_id}/ws/pty`
Provides an interactive POSIX pseudo-terminal (`/dev/pts`) session connected directly to a live `bash` shell.

#### Protocol:
1. **Connect:** Upgrade HTTP connection to `ws://<host>:8080/v1/sessions/{session_id}/ws/pty`.
2. **Send Keystrokes / Input:** Send text or binary frames into the WebSocket.
3. **Send Window Resize (SIGWINCH):**
   ```json
   {"type": "resize", "cols": 120, "rows": 40}
   ```
4. **Receive Terminal Stream:** Receives raw ANSI terminal escape sequences and characters from stdout/stderr.

---

## 5. System Health, Probes & Observability

### `GET /healthz`
Liveness probe returning engine process status, active driver, and pool availability.

#### Response (`200 OK`):
```json
{
  "status": "healthy",
  "driver": "DockerDriver",
  "configured_driver": "docker",
  "warm_pool_available": 2,
  "active_leases": 1
}
```

---

### `GET /readyz`
Kubernetes readiness probe verifying that the container driver daemon and the persistence ledger (Redis or SQLite) are responsive and accepting traffic.

#### Success Response (`200 OK`):
```json
{
  "status": "ready",
  "driver": "ready",
  "ledger": "ready"
}
```

#### Degraded Response (`503 Service Unavailable`):
```json
{
  "status": "not_ready",
  "driver": "driver unreachable: Connection refused",
  "ledger": "ready"
}
```

---

### `GET /metrics`
Prometheus metrics exporter endpoint. Scraped by Prometheus or OpenTelemetry collectors.

#### Response (`200 OK` - `text/plain`):
```prometheus
# HELP sandbox_warm_pool_available Number of warm containers currently ready
# TYPE sandbox_warm_pool_available gauge
sandbox_warm_pool_available 2.0

# HELP sandbox_warm_pool_target Target warm pool size
# TYPE sandbox_warm_pool_target gauge
sandbox_warm_pool_target 2.0

# HELP sandbox_active_leases Number of active session leases
# TYPE sandbox_active_leases gauge
sandbox_active_leases 1.0

# HELP sandbox_active_workspace_locks Number of active workspace concurrency locks held
# TYPE sandbox_active_workspace_locks gauge
sandbox_active_workspace_locks 1.0

# HELP sandbox_commands_executed_total Total commands executed in sandboxes
# TYPE sandbox_commands_executed_total counter
sandbox_commands_executed_total{status="success"} 42.0

# HELP sandbox_command_duration_seconds Execution latency in seconds
# TYPE sandbox_command_duration_seconds histogram
sandbox_command_duration_seconds_bucket{le="0.1"} 38.0
sandbox_command_duration_seconds_bucket{le="1.0"} 41.0
sandbox_command_duration_seconds_bucket{le="+Inf"} 42.0

# HELP sandbox_sweeper_reclaims_total Total expired or orphaned containers reclaimed by sweeper
# TYPE sandbox_sweeper_reclaims_total counter
sandbox_sweeper_reclaims_total{reason="expired_lease"} 3.0
```
