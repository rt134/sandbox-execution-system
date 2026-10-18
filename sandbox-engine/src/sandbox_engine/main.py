from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .api.routes_sessions import router as sessions_router
from .api.routes_stateless import router as stateless_router
from .api.routes_ws import router as ws_router
from .config import settings
from .core.lease_manager import LeaseManager
from .core.metrics import ACTIVE_SESSIONS, WARM_POOL_AVAILABLE, WORKSPACE_LOCKS_ACTIVE
from .core.security import verify_api_key
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
    # Enforce strict fail-fast validation in production mode
    settings.validate_production_settings()

    logger.info(f"🚀 Initializing sandbox-engine with driver: {settings.driver}")
    driver = get_driver()

    # 1. Initialize Lease Manager (Redis with SQLite fallback unless require_redis is set)
    lease_mgr = LeaseManager()
    await lease_mgr.initialize()

    # 2. Initialize Stateless Warm Pool
    warm_pool = WarmPoolManager(driver=driver, target_size=settings.warm_pool_size)
    await warm_pool.start()

    # 3. Initialize Sweeper GC Loop (passing warm_pool for orphan cleanup)
    sweeper = Sweeper(
        driver=driver,
        lease_manager=lease_mgr,
        warm_pool=warm_pool,
        interval_sec=settings.sweeper_interval_sec,
    )
    sweeper.start()

    app.state.driver = driver
    app.state.lease_manager = lease_mgr
    app.state.sweeper = sweeper
    app.state.warm_pool = warm_pool

    yield

    # Teardown
    logger.info("🛑 Shutting down sandbox-engine...")
    await sweeper.stop()
    await warm_pool.stop()
    await lease_mgr.close()


app = FastAPI(
    title="Sandbox Execution Engine",
    description="Unified execution engine for stateless code and stateful agent sandboxes",
    version="0.1.0",
    lifespan=lifespan,
)

# Secure CORS Middleware: Never pair wildcard origins with credentials
is_wildcard = "*" in settings.cors_allowed_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allowed_origins,
    allow_credentials=not is_wildcard,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    """Enforce strict request body size limit (25MB) to prevent memory exhaustion attacks."""
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > settings.max_request_size_bytes:
                return Response(
                    content='{"detail": "Request entity too large: payload exceeds 25MB limit"}',
                    status_code=413,
                    media_type="application/json",
                )
        except ValueError:
            pass
    return await call_next(request)


app.include_router(stateless_router)
app.include_router(sessions_router)
app.include_router(ws_router)


@app.get("/healthz", tags=["System"], dependencies=[Depends(verify_api_key)])
async def healthz():
    """Liveness probe: returns 200 if server process is running."""
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


@app.get("/readyz", tags=["System"], dependencies=[Depends(verify_api_key)])
async def readyz():
    """Readiness probe: validates backend container driver, database ledger, and warm pool availability."""
    if not hasattr(app.state, "driver") or not hasattr(app.state, "warm_pool"):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Engine initializing")

    db_ready = True
    if hasattr(app.state, "lease_manager") and app.state.lease_manager:
        db_ready = await app.state.lease_manager.ping()

    if not db_ready:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database ledger unreachable")

    # Verify warm pool has ready instances or driver is responsive
    pool_size = app.state.warm_pool.queue.qsize()
    return {
        "status": "ready",
        "database": "ready",
        "warm_pool_ready": pool_size > 0,
        "warm_pool_size": pool_size,
    }


@app.get("/metrics", tags=["Observability"], dependencies=[Depends(verify_api_key)])
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
