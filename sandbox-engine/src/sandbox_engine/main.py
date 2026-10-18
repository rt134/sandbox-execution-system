from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routes_stateless import router as stateless_router
from .api.routes_sessions import router as sessions_router
from .api.routes_ws import router as ws_router
from .config import settings
from .core.lease_manager import LeaseManager
from .core.sweeper import Sweeper
from .core.warm_pool import WarmPoolManager
from .drivers import get_driver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sandbox.engine")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🚀 Initializing sandbox-engine with driver: {settings.driver}")
    driver = get_driver()

    # 1. Initialize Lease Manager (Redis with SQLite fallback)
    lease_mgr = LeaseManager()
    await lease_mgr.initialize()

    # 2. Initialize Sweeper GC Loop
    sweeper = Sweeper(driver=driver, lease_manager=lease_mgr, interval_sec=settings.sweeper_interval_sec)
    sweeper.start()

    # 3. Initialize Stateless Warm Pool
    warm_pool = WarmPoolManager(driver=driver, target_size=settings.warm_pool_size)
    await warm_pool.start()

    app.state.driver = driver
    app.state.lease_manager = lease_mgr
    app.state.sweeper = sweeper
    app.state.warm_pool = warm_pool

    yield

    # Teardown
    logger.info("🛑 Shutting down sandbox-engine...")
    await sweeper.stop()
    await warm_pool.stop()


app = FastAPI(
    title="Sandbox Execution Engine",
    description="Unified execution engine for stateless code and stateful agent sandboxes",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(stateless_router)
app.include_router(sessions_router)
app.include_router(ws_router)


@app.get("/healthz")
async def healthz():
    driver_name = getattr(app.state, "driver", None).__class__.__name__ if hasattr(app.state, "driver") else "unknown"
    pool_size = app.state.warm_pool.queue.qsize() if hasattr(app.state, "warm_pool") else 0
    active_leases = len(await app.state.lease_manager.list_active()) if hasattr(app.state, "lease_manager") else 0
    return {
        "status": "healthy",
        "driver": driver_name,
        "configured_driver": settings.driver,
        "warm_pool_available": pool_size,
        "active_leases": active_leases,
    }


from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from fastapi.responses import Response
from .core.metrics import ACTIVE_SESSIONS, WARM_POOL_AVAILABLE, WORKSPACE_LOCKS_ACTIVE


@app.get("/metrics", tags=["Observability"])
async def get_metrics():
    """Prometheus metrics endpoint for scraping."""
    if hasattr(app.state, "warm_pool") and app.state.warm_pool:
        WARM_POOL_AVAILABLE.set(app.state.warm_pool.queue.qsize())
    if hasattr(app.state, "lease_manager") and app.state.lease_manager:
        active_leases = await app.state.lease_manager.list_active()
        ACTIVE_SESSIONS.set(len(active_leases))
        active_locks = await app.state.lease_manager.count_active_locks()
        WORKSPACE_LOCKS_ACTIVE.set(active_locks)

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("sandbox_engine.main:app", host=settings.host, port=settings.port, reload=False)
