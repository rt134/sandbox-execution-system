# Sandbox Execution System — AI Development Rules

This is the **sandbox-execution-system** monorepo: a security-hardened platform for **stateless code execution (<50ms)** and **stateful AI agent sandboxes** (persistent `/workspace`, streaming exec, interactive PTY). See `README.md`, `product-details.md` (full spec), and `docs/` before changing behavior.

---

## Stack

- **Components:** three independently built parts — `daemon/` (Go), `sandbox-engine/` (Python service), `sandbox-client/` (Python SDK).
- **Daemon (`shellctl`):** Go 1.22 static binary on `:5004` inside every sandbox container; deps `creack/pty`, `gorilla/websocket`.
- **Engine:** Python ≥3.9 (CI: 3.10–3.12), FastAPI + uvicorn on `:8080`, pydantic v2 / pydantic-settings, `docker` SDK, `kubernetes-asyncio`, `asyncpg` (PostgreSQL), `cryptography` (Fernet), `prometheus-client`.
- **Client SDK:** async `httpx` + `websockets` + pydantic v2; optional Pydantic AI tool adapters.
- **Infra:** Docker / docker-compose (Engine + Postgres/Redis + Squid), Kubernetes manifests in `k8s/`, gVisor (`runsc`) runtime, Squid SSRF egress proxy.

---

## Project Structure

```
sandbox-execution-system/
├── daemon/                         # Go shellctl daemon (in-sandbox kernel)
│   ├── main.go                     # Server, auth + logging middleware
│   ├── handlers/                   # health, execute, exec, pty, fs (+ fs_test.go)
│   ├── runner/python_runner.py     # Stateless code runner invoked by the daemon
│   ├── Dockerfile                  # Builds sandbox-base:latest
│   └── test_daemon.py              # Standalone daemon verification script
├── sandbox-engine/                 # FastAPI control plane
│   ├── src/sandbox_engine/
│   │   ├── main.py                 # App + async lifespan
│   │   ├── config.py               # EngineSettings (SANDBOX_ env prefix)
│   │   ├── models.py               # Pydantic request/lease schemas
│   │   ├── api/                    # routes_stateless, routes_sessions, routes_ws (PTY proxy)
│   │   ├── core/                   # warm_pool, lease_manager, sweeper, security, metrics
│   │   └── drivers/                # base.ContainerDriver, docker_driver, k8s_driver
│   └── tests/
├── sandbox-client/                 # Async Python SDK
│   ├── src/sandbox_client/         # client, session, fs, terminal, models, adapters/agent_tools
│   └── tests/                      # test_client_models.py, test_e2e.py
├── deploy/                         # docker-compose.yml, squid/ (egress proxy)
├── k8s/                            # Deployment, HPA, PDB, RBAC, NetworkPolicy, RuntimeClass, quotas
├── docs/                           # Architecture, API, SDK, daemon, config, security, deployment
├── scripts/start-local-postgres.sh
├── .github/workflows/              # CI: flake8, docker build, go vet/test, pytest
├── product-details.md
└── ai-guidelines/ + AGENTS.md
```

Update this tree when modules are added or moved.

---

## Coding Standards

**Python (engine + client)**
- `snake_case` functions/variables, `PascalCase` classes, `UPPER_SNAKE_CASE` constants.
- Start modules with `from __future__ import annotations`; type-annotate all signatures. Code must stay compatible with Python 3.9 syntax (`requires-python >= 3.9`).
- Async-first: all I/O (drivers, HTTP, DB, WebSocket) uses `async`/`await`. Never block the event loop with sync Docker/HTTP/DB calls.
- Use pydantic v2 models for API payloads and settings; no ad-hoc dicts across module boundaries.
- No mutable default arguments.
- Must pass CI flake8: `--max-line-length=127`, no syntax errors / undefined names (`E9,F63,F7,F82`).

**Go (daemon)**
- Standard `gofmt` formatting; must pass `go vet ./...`.
- Keep the binary small and static; avoid new dependencies unless essential.
- Handlers live in `daemon/handlers/`; middleware (auth, logging) stays in `main.go`.

---

## Architecture Rules

- **Pluggable drivers:** all container lifecycle goes through `drivers/base.py::ContainerDriver`. New runtime behavior must be implemented in **both** `DockerDriver` and `KubernetesDriver` (or explicitly unsupported in one). Never call the Docker/K8s SDKs from `api/` or `core/` directly. Driver choice comes only from `SANDBOX_DRIVER` via `get_driver()`.
- **Layering:** `api/` (routes, validation, auth) → `core/` (warm pool, leases, sweeper, security) → `drivers/`. Routes stay thin.
- **Configuration:** every tunable is a field on `EngineSettings` in `config.py` (env prefix `SANDBOX_`). No magic numbers or `os.environ` reads scattered in code. When adding a setting, document it in `docs/configuration.md`.
- **Persistence:** PostgreSQL (`SANDBOX_POSTGRES_URL`) is the source of truth for leases, locks, and audit logs; it is required when `ENV=production`. SQLite/Redis are dev/secondary fallbacks only — don't make new features depend on them.
- **Stateless path:** must preserve the <50ms SLA — warm containers are consumed and replenished; do not add per-request container spawns or blocking work on this path.
- **Stateful path:** sessions are lease-based with TTL renewal; the sweeper reclaims expired leases and orphaned containers (respect `orphan_grace_period_sec`). Any new resource must be reclaimable by the sweeper.
- **Engine ↔ daemon contract:** changes to daemon endpoints (`/health`, execute, exec, pty, fs) must be mirrored in the engine proxy, the client SDK, and `docs/api-reference.md` / `docs/daemon-shellctl.md`.
- **Client SDK:** keep it lightweight (httpx, websockets, pydantic only). It must not import from `sandbox_engine`.

---

## Security

**Never read or modify:** `.env*` (except `.env.example`), `*.pem`, `*.key`, `**/secrets/**`, `**/.sandbox_ledger.db*`, `**/.workspaces/`

- This system runs untrusted code — treat every change to isolation as security-critical. Do not weaken: gVisor runtime (`runsc` / `RuntimeClass gvisor`), `SANDBOX_PIDS_LIMIT`, memory/CPU limits, disk quotas, request/stream/WebSocket size caps, network policies, or the Squid egress ACLs (block `169.254.0.0/16`, RFC1918).
- Validate all workspace IDs with `WORKSPACE_ID_REGEX` and all file paths against `/workspace` traversal.
- Auth: caller → engine via `SANDBOX_API_KEY` / tenant keys (Bearer or `X-API-Key`); engine → daemon via per-container `SHELLCTL_AUTH_TOKEN`, compared in constant time. `/health` is the only unauthenticated endpoint.
- Daemon tokens are encrypted at rest (Fernet in `core/security.py`) before persisting to the database.
- Never log or print secrets (API keys, daemon tokens, DB URLs with passwords). Pull secrets from settings/env, never hard-code them.
- Do not set `SANDBOX_ALLOW_RUNTIME_FALLBACK=true` or wildcard CORS in production examples/manifests.

---

## Testing

- **Engine (CI):** `PYTHONPATH="sandbox-engine/src:sandbox-client/src" pytest sandbox-engine/tests/` — requires PostgreSQL (`scripts/start-local-postgres.sh`, `SANDBOX_POSTGRES_URL=postgresql://sandbox:sandbox@localhost:5432/sandbox`) and the `sandbox-base:latest` image (`docker build -t sandbox-base:latest daemon/`).
- **Daemon:** `cd daemon && go vet ./... && go test -v ./handlers`; full image check via `python3 daemon/test_daemon.py`.
- **Benchmarks / suites (scripts):** `cd sandbox-engine && PYTHONPATH=src python3 tests/test_stateless_bench.py` (<50ms SLA) and `tests/test_stateful_suite.py`.
- **E2E:** `PYTHONPATH="sandbox-engine/src:sandbox-client/src" python3 sandbox-client/tests/test_e2e.py`.
- Async tests use `pytest-asyncio` (`@pytest.mark.asyncio`, `@pytest_asyncio.fixture`).
- When mutating global `settings` in tests, restore the original value in `finally`.
- Add tests for both drivers when touching driver logic, and for quota/auth/tenant isolation when touching `core/security.py` or routes.
