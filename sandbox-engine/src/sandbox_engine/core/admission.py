from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Dict

from ..config import settings

_in_flight = 0
_lock = asyncio.Lock()
_tenant_in_flight: Dict[str, int] = defaultdict(int)


async def try_acquire_in_flight() -> bool:
    """
    Acquires a global admission slot non-blockingly.
    Returns True if acquired, or False if server is at capacity.
    """
    global _in_flight
    async with _lock:
        if _in_flight >= settings.max_in_flight_requests:
            return False
        _in_flight += 1
        return True


async def release_in_flight() -> None:
    """Releases a global admission slot."""
    global _in_flight
    async with _lock:
        _in_flight = max(0, _in_flight - 1)


async def acquire_in_flight() -> None:
    """
    Legacy helper: raises HTTPException(503) if server at capacity.
    For middleware use, prefer try_acquire_in_flight() and return JSONResponse directly.
    """
    if not await try_acquire_in_flight():
        from fastapi import HTTPException

        raise HTTPException(
            status_code=503,
            detail="Server at capacity",
            headers={"Retry-After": "5"},
        )


async def try_acquire_tenant_slot(tenant_id: str, max_slots: int = 10) -> bool:
    """
    Acquires a per-tenant concurrency slot for stateless execution.
    Prevents a single tenant from exhausting all worker capacity.
    """
    async with _lock:
        if _tenant_in_flight[tenant_id] >= max_slots:
            return False
        _tenant_in_flight[tenant_id] += 1
        return True


async def release_tenant_slot(tenant_id: str) -> None:
    """Releases a per-tenant concurrency slot."""
    async with _lock:
        current = _tenant_in_flight.get(tenant_id, 0)
        if current <= 1:
            _tenant_in_flight.pop(tenant_id, None)
        else:
            _tenant_in_flight[tenant_id] = current - 1
