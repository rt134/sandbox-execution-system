from __future__ import annotations

import asyncio
from typing import AsyncIterator, Dict, Optional, Tuple

import httpx
from fastapi import HTTPException

from ..config import settings

DEFAULT_LIMITS = httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=30.0)


async def request_with_byte_cap(
    method: str,
    url: str,
    *,
    json: Optional[dict] = None,
    params: Optional[dict] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 30.0,
    max_bytes: Optional[int] = None,
) -> Tuple[bytes, int, Dict[str, str]]:
    cap = max_bytes if max_bytes is not None else settings.max_response_bytes
    headers = headers or {}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds), limits=DEFAULT_LIMITS) as client:
            async with client.stream(method, url, json=json, params=params, headers=headers) as resp:
                chunks: list[bytes] = []
                total = 0
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
                    if total > cap:
                        raise HTTPException(status_code=413, detail="Response exceeds maximum allowed size")
                    chunks.append(chunk)
                body = b"".join(chunks)
                return body, resp.status_code, dict(resp.headers)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Gateway Timeout: sandbox request timed out")


async def stream_response_bytes(
    method: str,
    url: str,
    *,
    json: Optional[dict] = None,
    params: Optional[dict] = None,
    headers: Optional[Dict[str, str]] = None,
    max_bytes: Optional[int] = None,
) -> AsyncIterator[bytes]:
    cap = max_bytes if max_bytes is not None else settings.max_stream_response_bytes
    total = 0
    async with httpx.AsyncClient(timeout=None, limits=DEFAULT_LIMITS) as client:
        async with client.stream(method, url, json=json, params=params, headers=headers) as resp:
            async for chunk in resp.aiter_bytes():
                total += len(chunk)
                if total > cap:
                    raise HTTPException(status_code=413, detail="Stream exceeds maximum allowed size")
                yield chunk
