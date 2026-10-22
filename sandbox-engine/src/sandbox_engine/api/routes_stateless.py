from __future__ import annotations

import json
import logging
import time
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from ..config import settings
from ..core.admission import try_acquire_tenant_slot, release_tenant_slot
from ..core.http_client import request_with_byte_cap
from ..core.metrics import STATELESS_DURATION_SECONDS, STATELESS_RUNS_TOTAL
from ..core.security import verify_api_key
from ..core.warm_pool import WarmPoolManager
from ..models import StatelessRunRequest, StatelessRunResponse

logger = logging.getLogger("sandbox.stateless")
router = APIRouter(prefix="/v1/stateless", tags=["Stateless Execution"])


ALLOWED_LANGUAGES = {"python", "javascript", "typescript", "bash", "sh", "go", "rust", "node"}


def get_warm_pool(request: Request) -> WarmPoolManager:
    return request.app.state.warm_pool


@router.post("/run", response_model=StatelessRunResponse)
async def run_stateless_code(
    payload: StatelessRunRequest,
    tenant_id: str = Depends(verify_api_key),
    warm_pool: WarmPoolManager = Depends(get_warm_pool),
) -> StatelessRunResponse:
    """
    Execute stateless code with sub-50ms SLA using the pre-warmed container pool.
    """
    # Enforce maximum timeout limit to prevent worker starvation
    if payload.timeout > settings.max_stateless_timeout:
        raise HTTPException(
            status_code=400,
            detail=f"Requested timeout ({payload.timeout}s) exceeds maximum allowed limit ({settings.max_stateless_timeout}s)",
        )

    # Resolves H3: Enforce per-tenant concurrency limit on stateless runs
    if not await try_acquire_tenant_slot(tenant_id, max_slots=settings.max_stateless_per_tenant):
        raise HTTPException(
            status_code=429,
            detail=f"Tenant '{tenant_id}' reached maximum concurrent stateless executions ({settings.max_stateless_per_tenant})",
            headers={"Retry-After": "2"},
        )

    start_time = time.perf_counter()
    status = "error"

    # Normalize language Prometheus label to prevent unbounded metric cardinality
    raw_lang = (payload.language or "").lower().strip()
    metric_lang = raw_lang if raw_lang in ALLOWED_LANGUAGES else "other"

    try:
        # 1. Grab a pre-warmed sandbox (<1ms)
        try:
            instance = await warm_pool.acquire(timeout=settings.warm_pool_acquire_timeout)
        except HTTPException as e:
            STATELESS_RUNS_TOTAL.labels(language=metric_lang, status="pool_exhausted").inc()
            raise e
        except Exception as e:
            STATELESS_RUNS_TOTAL.labels(language=metric_lang, status="pool_exhausted").inc()
            raise HTTPException(
                status_code=503,
                detail=f"No available warm sandbox: {e}",
                headers={"Retry-After": "2"},
            )
        # 2. Direct HTTP dispatch to shellctl inside container with internal auth
        timeout_client = httpx.Timeout(payload.timeout + 2.0, connect=1.0)
        headers = {}
        if instance.auth_token:
            headers["Authorization"] = f"Bearer {instance.auth_token}"

        body, status_code, _ = await request_with_byte_cap(
            "POST",
            f"{instance.endpoint}/execute",
            json={
                "language": payload.language,
                "code": payload.code,
                "inputs": payload.inputs,
                "timeout": payload.timeout,
            },
            headers=headers,
            timeout_seconds=payload.timeout + 2.0,
        )

        if status_code != 200:
            status = "failed"
            raise HTTPException(status_code=status_code, detail="In-sandbox execution failed")

        data = json.loads(body)
        duration_sec = time.perf_counter() - start_time
        total_duration_ms = duration_sec * 1000

        exit_code = data.get("exit_code", 0)
        status = "success" if exit_code == 0 else "error"

        STATELESS_RUNS_TOTAL.labels(language=metric_lang, status=status).inc()
        STATELESS_DURATION_SECONDS.labels(language=metric_lang).observe(duration_sec)

        # Resolves M10: Structured audit logging with tenant attribution for stateless executions
        logger.info(
            f"[AUDIT] stateless_run tenant={tenant_id} lang={metric_lang} "
            f"code_len={len(payload.code)} status={status} exit_code={exit_code} "
            f"duration_ms={total_duration_ms:.2f} sandbox_id={instance.sandbox_id}"
        )

        return StatelessRunResponse(
            output=data.get("output"),
            logs=data.get("logs", ""),
            error=data.get("error"),
            exit_code=exit_code,
            duration_ms=total_duration_ms,
            sandbox_id=instance.sandbox_id,
        )

    except httpx.TimeoutException:
        status = "timeout"
        STATELESS_RUNS_TOTAL.labels(language=metric_lang, status="timeout").inc()
        logger.warning(f"[AUDIT] stateless_run_timeout tenant={tenant_id} lang={metric_lang}")
        raise HTTPException(status_code=504, detail="Execution timed out in sandbox")
    except httpx.RequestError as e:
        status = "comm_error"
        STATELESS_RUNS_TOTAL.labels(language=metric_lang, status="comm_error").inc()
        logger.error(f"[AUDIT] stateless_run_comm_error tenant={tenant_id} lang={metric_lang} err={e}")
        raise HTTPException(status_code=502, detail=f"Communication error with sandbox: {e}")
    finally:
        # 3. Recycle consumed container asynchronously (never blocks caller response)
        if "instance" in locals():
            warm_pool.trigger_recycle(instance)
        await release_tenant_slot(tenant_id)
