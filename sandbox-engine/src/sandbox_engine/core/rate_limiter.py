from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from typing import Dict, List, Optional
from fastapi import HTTPException, status
from ..config import settings
from .metrics import RATE_LIMITER_FALLBACK_TOTAL

logger = logging.getLogger("sandbox.rate_limiter")


class RateLimiter:
    """
    Distributed, tenant-aware rate limiter.
    - Runs strictly AFTER authentication using the verified tenant identity.
    - Unauthenticated requests are rejected by auth and NEVER consume tenant quota.
    - Uses Redis when available for multi-replica synchronization with automatic TTL eviction.
    - Falls back to an in-memory sliding window bounded by an OrderedDict with LRU eviction
      and maximum capacity to prevent memory exhaustion attacks.
    """

    def __init__(self, redis_client=None, pool=None, max_tracked_tenants: int = 5000):
        self.redis = redis_client
        self.pool = pool
        self.max_tracked_tenants = max_tracked_tenants
        self._history: OrderedDict[str, List[float]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._pg_schema_ready = False

    def set_redis(self, redis_client):
        self.redis = redis_client

    def set_pool(self, pool):
        self.pool = pool

    async def ensure_pg_schema(self) -> None:
        if not self.pool or self._pg_schema_ready:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tenant_rate_limits (
                    tenant_id VARCHAR(64) NOT NULL,
                    window_minute BIGINT NOT NULL,
                    request_count INT DEFAULT 1,
                    PRIMARY KEY (tenant_id, window_minute)
                )
                """
            )
            await conn.execute(
                "DELETE FROM tenant_rate_limits WHERE window_minute < $1",
                int(time.time() // 60) - 5,
            )
        self._pg_schema_ready = True

    async def check_rate_limit(self, tenant_id: str, limit: Optional[int] = None) -> None:
        """
        Records a request for the verified tenant_id and raises HTTP 429 if limit is exceeded.
        """
        max_requests = limit if limit is not None else settings.rate_limit_requests_per_minute

        # 1. Distributed Redis Implementation (if available)
        if self.redis:
            try:
                now_minute = int(time.time() // 60)
                redis_key = f"ratelimit:{tenant_id}:{now_minute}"
                count = await self.redis.incr(redis_key)
                if count == 1:
                    await self.redis.expire(redis_key, 65)
                if count > max_requests:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Too Many Requests: rate limit exceeded for tenant",
                        headers={"Retry-After": "60"},
                    )
                return
            except HTTPException:
                raise
            except Exception as e:
                # Resolves L2: log warning and emit metric when backend fails over to memory
                logger.warning(f"Rate limiter Redis backend failed for tenant '{tenant_id}': {e}. Falling back.")
                RATE_LIMITER_FALLBACK_TOTAL.inc()

        # 2. Distributed PostgreSQL Implementation (multi-replica synchronized)
        if self.pool:
            try:
                await self.ensure_pg_schema()
                now_minute = int(time.time() // 60)
                async with self.pool.acquire() as conn:
                    count = await conn.fetchval(
                        """
                        INSERT INTO tenant_rate_limits (tenant_id, window_minute, request_count)
                        VALUES ($1, $2, 1)
                        ON CONFLICT (tenant_id, window_minute)
                        DO UPDATE SET request_count = tenant_rate_limits.request_count + 1
                        RETURNING request_count
                        """,
                        tenant_id,
                        now_minute,
                    )
                    if count and count > max_requests:
                        raise HTTPException(
                            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                            detail="Too Many Requests: rate limit exceeded for tenant",
                            headers={"Retry-After": "60"},
                        )
                    return
            except HTTPException:
                raise
            except Exception as e:
                # Resolves L2: log warning and emit metric when backend fails over to memory
                logger.warning(f"Rate limiter PostgreSQL backend failed for tenant '{tenant_id}': {e}. Falling back.")
                RATE_LIMITER_FALLBACK_TOTAL.inc()

        # 3. Local Bounded Sliding Window Implementation (fallback)
        now = time.time()
        cutoff = now - 60.0

        async with self._lock:
            # If tenant already tracked, move to end (MRU)
            if tenant_id in self._history:
                self._history.move_to_end(tenant_id)
            else:
                # Bounded memory: If at capacity, evict oldest / expired entries
                if len(self._history) >= self.max_tracked_tenants:
                    # Prune expired entries first
                    expired_keys = [
                        t for t, timestamps in self._history.items()
                        if not timestamps or timestamps[-1] <= cutoff
                    ]
                    for ek in expired_keys:
                        self._history.pop(ek, None)

                    # If still at capacity, pop the least recently used item (LRU)
                    while len(self._history) >= self.max_tracked_tenants:
                        self._history.popitem(last=False)

            timestamps = self._history.setdefault(tenant_id, [])
            # Filter timestamps to current 60s window
            active = [ts for ts in timestamps if ts > cutoff]
            self._history[tenant_id] = active

            if len(active) >= max_requests:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Too Many Requests: rate limit exceeded for tenant",
                    headers={"Retry-After": "60"},
                )

            active.append(now)


# Global singleton instance
rate_limiter = RateLimiter()
