# Configuration & Environment Variables

The `sandbox-engine` service is configured using environment variables or a `.env` file loaded via `pydantic-settings`.

All environment variables use the `SANDBOX_` prefix.

---

## 1. Master Configuration Table

| Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| **Driver & Images** | | | |
| `SANDBOX_DRIVER` | `string` | `"docker"` | Container driver: `"docker"` (local dev/VM) or `"k8s"` (cluster). |
| `SANDBOX_IMAGE` | `string` | `"sandbox-base:latest"` | Base container image containing `shellctl`. |
| **Authentication & Security** | | | |
| `SANDBOX_API_KEY` | `string` | `None` | API key required for all caller-to-engine REST & WebSocket requests. When set, requests must pass `Authorization: Bearer <key>` or `X-API-Key`. |
| `SANDBOX_DAEMON_AUTH_TOKEN` | `string` | `None` | Shared secret token passed to in-container daemon `shellctl`. If omitted, the engine generates an internal cryptographic token per container. |
| `SANDBOX_CORS_ALLOWED_ORIGINS`| `list[string]`| `["*"]` | Allowed CORS origins. When credentials are used, wildcards are automatically disabled. |
| **Resource Limits & Anti-DoS** | | | |
| `SANDBOX_PIDS_LIMIT` | `integer` | `256` | Linux cgroups PID limit (`pids.max`) per container. Prevents fork bombs and thread exhaustion. |
| `SANDBOX_DEFAULT_MEM_LIMIT` | `string` | `"512m"` | Default container memory limit (e.g., `"512m"`, `"1g"`). Normalized to Mi/Gi on Kubernetes. |
| `SANDBOX_DEFAULT_CPU_LIMIT` | `float` | `1.0` | Default container CPU core allocation. |
| `SANDBOX_DEFAULT_WORKSPACE_DISK_QUOTA_MB` | `integer` | `1024` | Per-workspace disk quota in megabytes. Pre-write checks reject payloads exceeding this quota with `413 Payload Too Large`. |
| **Pool & Concurrency Management** | | | |
| `SANDBOX_WARM_POOL_SIZE` | `integer` | `2` | Number of pre-warmed idle containers kept in memory for stateless instant execution. |
| `SANDBOX_WARM_POOL_ACQUIRE_TIMEOUT` | `float` | `5.0` | Seconds to wait for a warm container before spawning on-demand. |
| `SANDBOX_MAX_CONCURRENT_SPAWNS` | `integer` | `10` | Bounded semaphore cap on simultaneous container spawns to prevent host thrashing. |
| **Stateful Sessions & Persistent Data Source (PostgreSQL)** | | | |
| `SANDBOX_POSTGRES_URL` | `string` | `"postgresql://sandbox:sandbox@localhost:5432/sandbox"` (local) | Primary PostgreSQL connection URL for all persistent state (leases, atomic locks, audit logs). Aliases: `POSTGRES_URL`, `DATABASE_URL`. |
| `ENV` / `SANDBOX_ENVIRONMENT`| `string` | `"development"` | Environment mode: `"development"`, `"production"`. In `"production"`, database URL is strictly required in ENV. |
| `SANDBOX_DEFAULT_SESSION_TTL` | `integer` | `300` | Inactivity lease timeout in seconds before auto-reclamation. |
| `SANDBOX_SWEEPER_INTERVAL_SEC` | `float` | `5.0` | Frequency in seconds of the background garbage collector reconciliation loop. |
| `SANDBOX_ORPHAN_GRACE_PERIOD_SEC` | `float` | `60.0` | Grace period in seconds before the sweeper will reclaim an unleased container (prevents races on newborn sessions). |
| `SANDBOX_WORKSPACES_ROOT` | `string` | `".workspaces"` | Canonical host directory where agent workspace volumes are stored. |
| `SANDBOX_REDIS_URL` | `string` | `None` | Optional secondary Redis connection URL. |
| `SANDBOX_REQUIRE_REDIS` | `boolean` | `false` | When `true`, refuses silent SQLite fallback if Redis is configured. |
| `SANDBOX_SQLITE_PATH` | `string` | `".sandbox_ledger.db"` | SQLite database path used only for offline development/testing fallback. |
| **Egress Proxying (SSRF Firewall)** | | | |
| `SANDBOX_HTTP_PROXY` | `string` | `None` | Outbound HTTP proxy URL injected into sandboxes (e.g., `http://squid:3128`). |
| `SANDBOX_HTTPS_PROXY` | `string` | `None` | Outbound HTTPS proxy URL injected into sandboxes (e.g., `http://squid:3128`). |
| `SANDBOX_NO_PROXY` | `string` | `"localhost,127.0.0.1"` | Domains/IPs excluded from outbound proxying. |
| **Docker Driver Settings** | | | |
| `SANDBOX_DOCKER_RUNTIME` | `string` | `"runc"` | Docker runtime engine: `"runc"` (dev/macOS) or `"runsc"` (gVisor prod). |
| `SANDBOX_ALLOW_RUNTIME_FALLBACK` | `boolean` | `false` | When `false`, fails fast if the requested runtime (e.g., `runsc`) is unavailable on the host. |
| `SANDBOX_DOCKER_NETWORK` | `string` | `"bridge"` | Docker network name to attach sandboxes to. |
| `SANDBOX_DOCKER_HOST_PORT_START` | `integer` | `15000` | Port range starting point for host port mappings. |
| **Kubernetes Driver Settings** | | | |
| `SANDBOX_K8S_NAMESPACE` | `string` | `"sandboxes"` | Kubernetes namespace where sandbox Pods are launched. |
| `SANDBOX_K8S_RUNTIME_CLASS` | `string` | `"gvisor"` | Kubernetes `RuntimeClass` name for gVisor isolation. |
| `SANDBOX_K8S_NODE_SELECTOR_KEY` | `string` | `"sandbox"` | Node selector label key for dedicated worker node pools. |
| `SANDBOX_K8S_NODE_SELECTOR_VALUE` | `string` | `"true"` | Node selector label value for dedicated worker node pools. |
| `SANDBOX_K8S_USE_PVC` | `boolean` | `false` | When `true`, mounts dynamic PVCs to `/workspace` instead of host paths. |
| **Server & Telemetry** | | | |
| `SANDBOX_HOST` | `string` | `"0.0.0.0"` | Bind IP address for the engine FastAPI server. |
| `SANDBOX_PORT` | `integer` | `8080` | Bind port for the engine FastAPI server. |
| `SANDBOX_ENABLE_PROMETHEUS_METRICS` | `boolean` | `true` | Exposes Prometheus metrics on `GET /metrics`. |

---

## 2. Example Configurations

### A. Local Development (`.env.local`)
Uses local Docker PostgreSQL (started via `scripts/start-local-postgres.sh` or `deploy/docker-compose.yml`):
```ini
ENV=development
SANDBOX_DRIVER=docker
SANDBOX_IMAGE=sandbox-base:latest
SANDBOX_WARM_POOL_SIZE=2
SANDBOX_DOCKER_RUNTIME=runc
SANDBOX_ALLOW_RUNTIME_FALLBACK=true

# Persistent Data Source (Local Docker Postgres)
SANDBOX_POSTGRES_URL=postgresql://sandbox:sandbox@localhost:5432/sandbox

SANDBOX_PORT=8080
SANDBOX_PIDS_LIMIT=256
SANDBOX_DEFAULT_WORKSPACE_DISK_QUOTA_MB=1024
```

### B. Production Multi-Replica Kubernetes Cluster (`.env.prod`)
Hardened zero-trust deployment with PostgreSQL persistence (strictly required in ENV), gVisor, egress firewall, and Prometheus metrics:
```ini
ENV=production
SANDBOX_DRIVER=k8s
SANDBOX_IMAGE=my-registry.io/sandbox-base:v1.0.0
SANDBOX_API_KEY=sk_live_prod_secret_token_77a941

# Persistent Data Source (Strictly required in ENV in production)
SANDBOX_POSTGRES_URL=postgresql://prod_user:StrongSecretPassword@postgres-ha.sandboxes.svc.cluster.local:5432/sandboxes_prod

# Pool and Concurrency
SANDBOX_WARM_POOL_SIZE=10
SANDBOX_MAX_CONCURRENT_SPAWNS=20

# Multi-Replica State & Leases
SANDBOX_DEFAULT_SESSION_TTL=600
SANDBOX_ORPHAN_GRACE_PERIOD_SEC=60.0

# Egress Firewall
SANDBOX_HTTP_PROXY=http://squid-proxy.sandboxes.svc.cluster.local:3128
SANDBOX_HTTPS_PROXY=http://squid-proxy.sandboxes.svc.cluster.local:3128

# Kernel Isolation & Node Scheduling
SANDBOX_K8S_NAMESPACE=sandboxes
SANDBOX_K8S_RUNTIME_CLASS=gvisor
SANDBOX_K8S_NODE_SELECTOR_KEY=sandbox
SANDBOX_K8S_NODE_SELECTOR_VALUE=true
SANDBOX_K8S_USE_PVC=true

# Security & Quota Controls
SANDBOX_PIDS_LIMIT=256
SANDBOX_DEFAULT_WORKSPACE_DISK_QUOTA_MB=2048
SANDBOX_ENABLE_PROMETHEUS_METRICS=true
```
