from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# 1. Warm Pool Metrics
WARM_POOL_AVAILABLE = Gauge(
    "sandbox_warm_pool_available",
    "Number of pre-warmed sandbox instances available in the pool",
)
WARM_POOL_ACQUISITIONS_TOTAL = Counter(
    "sandbox_warm_pool_acquisitions_total",
    "Total number of times a warm sandbox was acquired",
    ["result"],  # "hit", "miss"
)

# 2. Stateful Sessions & Workspace Concurrency
ACTIVE_SESSIONS = Gauge(
    "sandbox_active_sessions",
    "Number of currently active stateful sessions",
)
WORKSPACE_LOCKS_ACTIVE = Gauge(
    "sandbox_workspace_locks_active",
    "Number of active workspace concurrency locks",
)

# 3. Stateless Executions
STATELESS_RUNS_TOTAL = Counter(
    "sandbox_stateless_runs_total",
    "Total count of stateless code execution requests",
    ["language", "status"],  # status: "success", "error", "timeout"
)
STATELESS_DURATION_SECONDS = Histogram(
    "sandbox_stateless_duration_seconds",
    "Duration of stateless code executions in seconds",
    ["language"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)

# 4. Background Sweeper Reconciliations
SWEEPER_RECLAIMS_TOTAL = Counter(
    "sandbox_sweeper_reclaims_total",
    "Total number of expired leases or orphan sandboxes reclaimed by sweeper",
    ["reason"],  # "expired_lease", "orphan_container"
)
