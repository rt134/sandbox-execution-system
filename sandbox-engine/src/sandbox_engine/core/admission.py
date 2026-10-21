from __future__ import annotations

import asyncio

from ..config import settings

_in_flight = 0
_lock = asyncio.Lock()


async def acquire_in_flight() -> None:
    global _in_flight
    async with _lock:
        if _in_flight >= settings.max_in_flight_requests:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=503,
                detail="Server at capacity",
                headers={"Retry-After": "5"},
            )
        _in_flight += 1


async def release_in_flight() -> None:
    global _in_flight
    async with _lock:
        _in_flight = max(0, _in_flight - 1)
