from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from .api.routes_sessions import router as sessions_router
from .api.routes_stateless import router as stateless_router
from .api.routes_ws import router as ws_router
from .config import settings
from .core.admission import acquire_in_flight, release_in_flight
from .core.lease_manager import LeaseManager
from .core.rate_limiter import rate_limiter
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

    # Wire distributed rate limiter
    if hasattr(lease_mgr.backend, "_pool") and lease_mgr.backend._pool:
        rate_limiter.set_pool(lease_mgr.backend._pool)
        await rate_limiter.ensure_pg_schema()
    elif hasattr(lease_mgr.backend, "redis") and lease_mgr.backend.redis:
        rate_limiter.set_redis(lease_mgr.backend.redis)

    if settings.enable_prometheus_metrics:
        from prometheus_client import start_http_server

        start_http_server(settings.metrics_port)

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


_is_prod = settings.environment.lower() in ("production", "prod")
app = FastAPI(
    title="Sandbox Execution Engine",
    description="Unified execution engine for stateless code and stateful agent sandboxes",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None if _is_prod else "/docs",
    openapi_url=None if _is_prod else "/openapi.json",
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
async def admission_and_size_limits(request: Request, call_next):
    if request.url.path.startswith("/v1/"):
        await acquire_in_flight()
        try:
            return await _limit_request_size(request, call_next)
        finally:
            await release_in_flight()
    return await _limit_request_size(request, call_next)


async def _limit_request_size(request: Request, call_next):
    """Enforce strict request body size limit (25MB) including chunked streams to prevent memory exhaustion attacks."""
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
    else:
        # Protect against chunked streaming memory bombs where Content-Length is omitted
        received_bytes = 0
        original_receive = request._receive

        async def limited_receive():
            nonlocal received_bytes
            msg = await original_receive()
            if msg.get("type") == "http.request":
                body_chunk = msg.get("body", b"")
                received_bytes += len(body_chunk)
                if received_bytes > settings.max_request_size_bytes:
                    return Response(
                        content='{"detail":"Request entity too large: streaming payload exceeds 25MB limit"}',
                        status_code=413,
                        media_type="application/json",
                    )
            return msg

        request._receive = limited_receive

    return await call_next(request)


app.include_router(stateless_router)
app.include_router(sessions_router)
app.include_router(ws_router)


@app.get("/livez", tags=["System"])
async def livez():
    """Kubernetes liveness probe: returns 200 if server process is running without tenant auth."""
    return {"status": "alive"}


@app.get("/healthz", tags=["System"])
async def healthz():
    """System health check: returns component status without requiring tenant auth."""
    driver_name = getattr(app.state, "driver", None).__class__.__name__ if hasattr(app.state, "driver") else "unknown"
    pool_size = app.state.warm_pool.queue.qsize() if hasattr(app.state, "warm_pool") else 0
    return {
        "status": "healthy",
        "driver": driver_name,
        "configured_driver": settings.driver,
        "warm_pool_available": pool_size,
    }


@app.get("/readyz", tags=["System"])
async def readyz():
    """Readiness probe: validates backend container driver, database ledger, and warm pool availability."""
    if not hasattr(app.state, "driver") or not hasattr(app.state, "warm_pool"):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Engine initializing")

    db_ready = True
    if hasattr(app.state, "lease_manager") and app.state.lease_manager:
        db_ready = await app.state.lease_manager.ping()

    if not db_ready:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database ledger unreachable")

    pool_size = app.state.warm_pool.queue.qsize()
    warm_ready = pool_size > 0 or settings.driver == "k8s"
    if not warm_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Warm pool empty and no sandboxes ready",
        )
    return {
        "status": "ready",
        "database": "ready",
        "warm_pool_ready": warm_ready,
        "warm_pool_size": pool_size,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("sandbox_engine.main:app", host=settings.host, port=settings.port, reload=False)
