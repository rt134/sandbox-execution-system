from __future__ import annotations

import time
from typing import Optional

from ..config import settings
from ..models import LeaseRecord


def clamp_session_ttl(record: LeaseRecord, ttl_seconds: Optional[int]) -> int:
    """Clamp heartbeat/acquire TTL to max_session_ttl and absolute max_session_lifetime."""
    ttl = ttl_seconds if ttl_seconds is not None else settings.default_session_ttl
    ttl = min(int(ttl), settings.max_session_ttl)
    absolute_cap = record.created_at + settings.max_session_lifetime
    remaining = absolute_cap - time.time()
    if remaining <= 0:
        return 1
    ttl = min(ttl, int(remaining))
    return max(1, ttl)


def clamp_new_session_ttl(ttl_seconds: Optional[int]) -> int:
    ttl = ttl_seconds if ttl_seconds is not None else settings.default_session_ttl
    ttl = min(int(ttl), settings.max_session_ttl)
    return max(1, ttl)
