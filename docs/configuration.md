# Configuration & Environment Variables

The `sandbox-engine` service is configured using environment variables or a `.env` file loaded via `pydantic-settings`.

All environment variables use the `SANDBOX_` prefix.

---

## 1. Master Configuration Table

| Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `SANDBOX_DRIVER` | `string` | `"docker"` | Container driver: `"docker"` (local dev/VM) or `"k8s"` (cluster). |
| `SANDBOX_IMAGE` | `string` | `"sandbox-base:latest"` | Base container image containing `shellctl`. |
| `SANDBOX_WARM_POOL_SIZE` | `integer` | `2` | Number of pre-warmed idle containers in memory. |
| `SANDBOX_WARM_POOL_ACQUIRE_TIMEOUT` | `float` | `5.0` | Seconds to wait for a warm container before spawning on-demand. |
| `SANDBOX_DEFAULT_SESSION_TTL` | `integer` | `300` | Inactivity lease timeout in seconds for agent sessions. |
| `SANDBOX_SWEEPER_INTERVAL_SEC` | `float` | `5.0` | Frequency in seconds of the background GC reconciliation loop. |
| `SANDBOX_WORKSPACES_ROOT` | `string` | `".workspaces"` | Host directory where agent workspace volumes are stored. |
| `SANDBOX_REDIS_URL` | `string` | `"redis://localhost:6379/0"` | Redis lease ledger URL. Automatically falls back to SQLite if unreachable. |
| `SANDBOX_SQLITE_PATH` | `string` | `".sandbox_ledger.db"` | SQLite database file for local development lease tracking. |
| `SANDBOX_DOCKER_RUNTIME` | `string` | `"runc"` | Docker runtime: `"runc"` (macOS/dev) or `"runsc"` (gVisor prod). |
| `SANDBOX_DOCKER_NETWORK` | `string` | `"bridge"` | Docker network where sandbox containers are attached. |
| `SANDBOX_DOCKER_HOST_PORT_START` | `integer` | `15000` | Port range starting point for host bindings on Docker. |
| `SANDBOX_K8S_NAMESPACE` | `string` | `"sandboxes"` | Kubernetes namespace where sandbox Pods are created. |
| `SANDBOX_K8S_RUNTIME_CLASS` | `string` | `"gvisor"` | Kubernetes `RuntimeClass` name for gVisor isolation. |
| `SANDBOX_K8S_NODE_SELECTOR_KEY` | `string` | `"sandbox"` | Node selector key for scheduling onto dedicated nodes. |
| `SANDBOX_K8S_NODE_SELECTOR_VALUE` | `string` | `"true"` | Node selector value for dedicated node matching. |
| `SANDBOX_HOST` | `string` | `"0.0.0.0"` | Bind IP for the FastAPI server. |
| `SANDBOX_PORT` | `integer` | `8080` | Bind port for the FastAPI server. |

---

## 2. Example Configurations

### A. Local Development (`.env.local`)
Zero external dependencies needed (uses local Docker and SQLite fallback):
```ini
SANDBOX_DRIVER=docker
SANDBOX_IMAGE=sandbox-base:latest
SANDBOX_WARM_POOL_SIZE=2
SANDBOX_DOCKER_RUNTIME=runc
SANDBOX_REDIS_URL=
SANDBOX_SQLITE_PATH=.sandbox_ledger.db
SANDBOX_PORT=8080
```

### B. Production Kubernetes Cluster (`.env.prod`)
```ini
SANDBOX_DRIVER=k8s
SANDBOX_IMAGE=my-registry.io/sandbox-base:v1.0.0
SANDBOX_WARM_POOL_SIZE=10
SANDBOX_REDIS_URL=redis://redis-cluster.sandboxes.svc.cluster.local:6379/0
SANDBOX_K8S_NAMESPACE=sandboxes
SANDBOX_K8S_RUNTIME_CLASS=gvisor
SANDBOX_K8S_NODE_SELECTOR_KEY=sandbox
SANDBOX_K8S_NODE_SELECTOR_VALUE=true
SANDBOX_DEFAULT_SESSION_TTL=600
```
