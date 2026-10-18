from __future__ import annotations

import time
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from ..core.metrics import STATELESS_DURATION_SECONDS, STATELESS_RUNS_TOTAL
from ..core.security import verify_api_key
from ..core.warm_pool import WarmPoolManager
from ..models import StatelessRunRequest, StatelessRunResponse

router = APIRouter(prefix="/v1/stateless", tags=["Stateless Execution"])


def get_warm_pool(request: Request) -> WarmPoolManager:
    return request.app.state.warm_pool


@router.post("/run", response_model=StatelessRunResponse, dependencies=[Depends(verify_api_key)])
async def run_stateless_code(
    payload: StatelessRunRequest,
    warm_pool: WarmPoolManager = Depends(get_warm_pool),
) -> StatelessRunResponse:
    """
    Execute stateless code with sub-50ms SLA using the pre-warmed container pool.
    """
    start_time = time.perf_counter()
    status = "error"

    # 1. Grab a pre-warmed sandbox (<1ms)
    try:
        instance = await warm_pool.acquire(timeout=5.0)
    except Exception as e:
        STATELESS_RUNS_TOTAL.labels(language=payload.language, status="pool_exhausted").inc()
        raise HTTPException(status_code=503, detail=f"No available warm sandbox: {e}")

    try:
        # 2. Direct HTTP dispatch to shellctl inside container with internal auth
        timeout_client = httpx.Timeout(payload.timeout + 2.0, connect=1.0)
        headers = {}
        if instance.auth_token:
            headers["Authorization"] = f"Bearer {instance.auth_token}"

        async with httpx.AsyncClient(timeout=timeout_client, headers=headers) as client:
            resp = await client.post(
                f"{instance.endpoint}/execute",
                json={
                    "language": payload.language,
                    "code": payload.code,
                    "inputs": payload.inputs,
                    "timeout": payload.timeout,
                },
            )


        if resp.status_code != 200:
            status = "failed"
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"In-sandbox daemon returned {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        duration_sec = time.perf_counter() - start_time
        total_duration_ms = duration_sec * 1000

        exit_code = data.get("exit_code", 0)
        status = "success" if exit_code == 0 else "error"

        STATELESS_RUNS_TOTAL.labels(language=payload.language, status=status).inc()
        STATELESS_DURATION_SECONDS.labels(language=payload.language).observe(duration_sec)

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
        STATELESS_RUNS_TOTAL.labels(language=payload.language, status="timeout").inc()
        raise HTTPException(status_code=504, detail="Execution timed out in sandbox")
    except httpx.RequestError as e:
        status = "comm_error"
        STATELESS_RUNS_TOTAL.labels(language=payload.language, status="comm_error").inc()
        raise HTTPException(status_code=502, detail=f"Communication error with sandbox: {e}")
    finally:
        # 3. Recycle consumed container asynchronously (never blocks caller response)
        warm_pool.trigger_recycle(instance)
