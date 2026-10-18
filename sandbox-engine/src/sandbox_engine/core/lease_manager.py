from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from ..config import settings
from ..models import LeaseRecord

logger = logging.getLogger("sandbox.lease_manager")


class LeaseBackend(ABC):
    @abstractmethod
    async def create_or_renew_lease(
        self, session_id: str, workspace_id: str, sandbox_id: str, endpoint: str, ttl_seconds: int
    ) -> LeaseRecord:
        pass

    @abstractmethod
    async def heartbeat(self, session_id: str, ttl_seconds: int) -> Optional[LeaseRecord]:
        pass

    @abstractmethod
    async def get_lease(self, session_id: str) -> Optional[LeaseRecord]:
        pass

    @abstractmethod
    async def release_lease(self, session_id: str) -> bool:
        pass

    @abstractmethod
    async def get_expired_leases(self) -> List[LeaseRecord]:
        pass

    @abstractmethod
    async def mark_expired(self, session_id: str) -> None:
        pass

    @abstractmethod
    async def list_active_leases(self) -> List[LeaseRecord]:
        pass

    # Concurrency Lock APIs
    @abstractmethod
    async def acquire_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        """
        Atomically acquire or refresh workspace lock.
        Returns: (success, locked_by_session_id, expires_at)
        """
        pass

    @abstractmethod
    async def release_workspace_lock(self, workspace_id: str, session_id: str) -> bool:
        pass

    @abstractmethod
    async def refresh_workspace_lock(self, workspace_id: str, session_id: str, ttl_seconds: int) -> bool:
        pass

    @abstractmethod
    async def get_workspace_lock(self, workspace_id: str) -> Optional[Tuple[str, float]]:
        pass

    @abstractmethod
    async def cleanup_expired_workspace_locks(self) -> int:
        pass

    @abstractmethod
    async def count_active_locks(self) -> int:
        pass


class SQLiteLeaseBackend(LeaseBackend):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leases (
                    session_id TEXT PRIMARY KEY,
                    workspace_id TEXT,
                    sandbox_id TEXT,
                    endpoint TEXT,
                    created_at REAL,
                    expires_at REAL,
                    status TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workspace_locks (
                    workspace_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            conn.commit()

    async def create_or_renew_lease(
        self, session_id: str, workspace_id: str, sandbox_id: str, endpoint: str, ttl_seconds: int
    ) -> LeaseRecord:
        async with self._lock:
            now = time.time()
            expires_at = now + ttl_seconds
            record = LeaseRecord(
                session_id=session_id,
                workspace_id=workspace_id,
                sandbox_id=sandbox_id,
                endpoint=endpoint,
                created_at=now,
                expires_at=expires_at,
                status="active",
            )
            await asyncio.to_thread(self._sync_save, record)
            return record

    def _sync_save(self, record: LeaseRecord):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO leases (session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.session_id,
                    record.workspace_id,
                    record.sandbox_id,
                    record.endpoint,
                    record.created_at,
                    record.expires_at,
                    record.status,
                ),
            )
            conn.commit()

    async def heartbeat(self, session_id: str, ttl_seconds: int) -> Optional[LeaseRecord]:
        async with self._lock:
            record = await self.get_lease(session_id)
            if not record or record.status != "active":
                return None
            record.expires_at = time.time() + ttl_seconds
            await asyncio.to_thread(self._sync_save, record)
            return record

    async def get_lease(self, session_id: str) -> Optional[LeaseRecord]:
        return await asyncio.to_thread(self._sync_get, session_id)

    def _sync_get(self, session_id: str) -> Optional[LeaseRecord]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status FROM leases WHERE session_id = ?",
                (session_id,),
            )
            row = cursor.fetchone()
            if row:
                return LeaseRecord(
                    session_id=row[0],
                    workspace_id=row[1],
                    sandbox_id=row[2],
                    endpoint=row[3],
                    created_at=row[4],
                    expires_at=row[5],
                    status=row[6],
                )
        return None

    async def release_lease(self, session_id: str) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._sync_update_status, session_id, "released")

    async def mark_expired(self, session_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._sync_update_status, session_id, "expired")

    def _sync_update_status(self, session_id: str, status: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE leases SET status = ? WHERE session_id = ?",
                (status, session_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    async def get_expired_leases(self) -> List[LeaseRecord]:
        return await asyncio.to_thread(self._sync_get_expired)

    def _sync_get_expired(self) -> List[LeaseRecord]:
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status FROM leases WHERE status = 'active' AND expires_at < ?",
                (now,),
            )
            rows = cursor.fetchall()
            return [
                LeaseRecord(
                    session_id=r[0],
                    workspace_id=r[1],
                    sandbox_id=r[2],
                    endpoint=r[3],
                    created_at=r[4],
                    expires_at=r[5],
                    status=r[6],
                )
                for r in rows
            ]

    async def list_active_leases(self) -> List[LeaseRecord]:
        return await asyncio.to_thread(self._sync_list_active)

    def _sync_list_active(self) -> List[LeaseRecord]:
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status FROM leases WHERE status = 'active' AND expires_at >= ?",
                (now,),
            )
            rows = cursor.fetchall()
            return [
                LeaseRecord(
                    session_id=r[0],
                    workspace_id=r[1],
                    sandbox_id=r[2],
                    endpoint=r[3],
                    created_at=r[4],
                    expires_at=r[5],
                    status=r[6],
                )
                for r in rows
            ]

    # Concurrency Lock Implementations
    async def acquire_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        async with self._lock:
            return await asyncio.to_thread(self._sync_acquire_lock, workspace_id, session_id, ttl_seconds)

    def _sync_acquire_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        now = time.time()
        new_expires_at = now + ttl_seconds
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, expires_at FROM workspace_locks WHERE workspace_id = ?",
                (workspace_id,),
            )
            row = cursor.fetchone()
            if row:
                holder, exp = row[0], float(row[1])
                if exp > now and holder != session_id:
                    # Still held by another active session
                    return False, holder, exp

            # Lock is either free, expired, or held by same session -> acquire/renew
            conn.execute(
                "INSERT OR REPLACE INTO workspace_locks (workspace_id, session_id, expires_at) VALUES (?, ?, ?)",
                (workspace_id, session_id, new_expires_at),
            )
            conn.commit()
            return True, session_id, new_expires_at

    async def release_workspace_lock(self, workspace_id: str, session_id: str) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._sync_release_lock, workspace_id, session_id)

    def _sync_release_lock(self, workspace_id: str, session_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM workspace_locks WHERE workspace_id = ? AND session_id = ?",
                (workspace_id, session_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    async def refresh_workspace_lock(self, workspace_id: str, session_id: str, ttl_seconds: int) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._sync_refresh_lock, workspace_id, session_id, ttl_seconds)

    def _sync_refresh_lock(self, workspace_id: str, session_id: str, ttl_seconds: int) -> bool:
        now = time.time()
        new_exp = now + ttl_seconds
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE workspace_locks SET expires_at = ? WHERE workspace_id = ? AND session_id = ?",
                (new_exp, workspace_id, session_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    async def get_workspace_lock(self, workspace_id: str) -> Optional[Tuple[str, float]]:
        return await asyncio.to_thread(self._sync_get_lock, workspace_id)

    def _sync_get_lock(self, workspace_id: str) -> Optional[Tuple[str, float]]:
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, expires_at FROM workspace_locks WHERE workspace_id = ? AND expires_at > ?",
                (workspace_id, now),
            )
            row = cursor.fetchone()
            if row:
                return row[0], float(row[1])
        return None

    async def cleanup_expired_workspace_locks(self) -> int:
        return await asyncio.to_thread(self._sync_cleanup_locks)

    def _sync_cleanup_locks(self) -> int:
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM workspace_locks WHERE expires_at <= ?",
                (now,),
            )
            conn.commit()
            return cursor.rowcount

    async def count_active_locks(self) -> int:
        return await asyncio.to_thread(self._sync_count_active_locks)

    def _sync_count_active_locks(self) -> int:
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM workspace_locks WHERE expires_at > ?",
                (now,),
            )
            row = cursor.fetchone()
            return row[0] if row else 0


class RedisLeaseBackend(LeaseBackend):
    def __init__(self, redis_client):
        self.redis = redis_client

    async def create_or_renew_lease(
        self, session_id: str, workspace_id: str, sandbox_id: str, endpoint: str, ttl_seconds: int
    ) -> LeaseRecord:
        now = time.time()
        expires_at = now + ttl_seconds
        record = LeaseRecord(
            session_id=session_id,
            workspace_id=workspace_id,
            sandbox_id=sandbox_id,
            endpoint=endpoint,
            created_at=now,
            expires_at=expires_at,
            status="active",
        )
        data = record.model_dump_json()
        await self.redis.set(f"lease:{session_id}", data, ex=ttl_seconds)
        await self.redis.sadd("leases:active", session_id)
        return record

    async def heartbeat(self, session_id: str, ttl_seconds: int) -> Optional[LeaseRecord]:
        record = await self.get_lease(session_id)
        if not record or record.status != "active":
            return None
        record.expires_at = time.time() + ttl_seconds
        await self.redis.set(f"lease:{session_id}", record.model_dump_json(), ex=ttl_seconds)
        return record

    async def get_lease(self, session_id: str) -> Optional[LeaseRecord]:
        raw = await self.redis.get(f"lease:{session_id}")
        if not raw:
            return None
        return LeaseRecord.model_validate_json(raw)

    async def release_lease(self, session_id: str) -> bool:
        record = await self.get_lease(session_id)
        if not record:
            return False
        record.status = "released"
        await self.redis.delete(f"lease:{session_id}")
        await self.redis.srem("leases:active", session_id)
        return True

    async def get_expired_leases(self) -> List[LeaseRecord]:
        expired = []
        active_ids = await self.redis.smembers("leases:active")
        for sid_bytes in active_ids:
            sid = sid_bytes.decode("utf-8") if isinstance(sid_bytes, bytes) else str(sid_bytes)
            exists = await self.redis.exists(f"lease:{sid}")
            if not exists:
                expired.append(
                    LeaseRecord(
                        session_id=sid,
                        workspace_id="",
                        sandbox_id="",
                        endpoint="",
                        created_at=0,
                        expires_at=0,
                        status="expired",
                    )
                )
        return expired

    async def mark_expired(self, session_id: str) -> None:
        await self.redis.srem("leases:active", session_id)

    async def list_active_leases(self) -> List[LeaseRecord]:
        active_ids = await self.redis.smembers("leases:active")
        leases = []
        for sid_bytes in active_ids:
            sid = sid_bytes.decode("utf-8") if isinstance(sid_bytes, bytes) else str(sid_bytes)
            lease = await self.get_lease(sid)
            if lease:
                leases.append(lease)
        return leases

    async def acquire_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        key = f"lock:workspace:{workspace_id}"
        now = time.time()
        existing = await self.redis.get(key)
        if existing:
            holder = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing)
            ttl_rem = await self.redis.ttl(key)
            exp = now + max(0, ttl_rem)
            if holder == session_id:
                await self.redis.expire(key, ttl_seconds)
                return True, session_id, now + ttl_seconds
            else:
                return False, holder, exp

        ok = await self.redis.set(key, session_id, nx=True, ex=ttl_seconds)
        if ok:
            await self.redis.sadd("workspace_locks:active", workspace_id)
            return True, session_id, now + ttl_seconds

        existing = await self.redis.get(key)
        holder = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing) if existing else "unknown"
        ttl_rem = await self.redis.ttl(key)
        return False, holder, now + max(0, ttl_rem)

    async def release_workspace_lock(self, workspace_id: str, session_id: str) -> bool:
        key = f"lock:workspace:{workspace_id}"
        existing = await self.redis.get(key)
        if not existing:
            await self.redis.srem("workspace_locks:active", workspace_id)
            return False
        holder = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing)
        if holder == session_id:
            await self.redis.delete(key)
            await self.redis.srem("workspace_locks:active", workspace_id)
            return True
        return False

    async def refresh_workspace_lock(self, workspace_id: str, session_id: str, ttl_seconds: int) -> bool:
        key = f"lock:workspace:{workspace_id}"
        existing = await self.redis.get(key)
        if not existing:
            return False
        holder = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing)
        if holder == session_id:
            await self.redis.expire(key, ttl_seconds)
            return True
        return False

    async def get_workspace_lock(self, workspace_id: str) -> Optional[Tuple[str, float]]:
        key = f"lock:workspace:{workspace_id}"
        existing = await self.redis.get(key)
        if not existing:
            return None
        holder = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing)
        ttl_rem = await self.redis.ttl(key)
        return holder, time.time() + max(0, ttl_rem)

    async def cleanup_expired_workspace_locks(self) -> int:
        active_ws = await self.redis.smembers("workspace_locks:active")
        cleaned = 0
        for ws_bytes in active_ws:
            ws = ws_bytes.decode("utf-8") if isinstance(ws_bytes, bytes) else str(ws_bytes)
            if not await self.redis.exists(f"lock:workspace:{ws}"):
                await self.redis.srem("workspace_locks:active", ws)
                cleaned += 1
        return cleaned

    async def count_active_locks(self) -> int:
        await self.cleanup_expired_workspace_locks()
        return await self.redis.scard("workspace_locks:active")


class LeaseManager:
    """
    Manages session leases and workspace concurrency locks with automatic fallback
    to SQLite when Redis is unavailable.
    """

    def __init__(self, backend: Optional[LeaseBackend] = None):
        self._backend = backend

    async def initialize(self):
        if self._backend is not None:
            return

        if settings.redis_url:
            try:
                import redis.asyncio as aioredis
                client = aioredis.from_url(settings.redis_url, socket_connect_timeout=1.0)
                await client.ping()
                logger.info(f"Connected to Redis for session leases at {settings.redis_url}")
                self._backend = RedisLeaseBackend(client)
                return
            except Exception as e:
                logger.warning(f"Redis unavailable ({e}); falling back to SQLite ledger at {settings.sqlite_path}")

        self._backend = SQLiteLeaseBackend(settings.sqlite_path)

    async def create_or_renew(
        self, session_id: str, workspace_id: str, sandbox_id: str, endpoint: str, ttl_seconds: Optional[int] = None
    ) -> LeaseRecord:
        ttl = ttl_seconds if ttl_seconds is not None else settings.default_session_ttl
        return await self._backend.create_or_renew_lease(session_id, workspace_id, sandbox_id, endpoint, ttl)

    async def heartbeat(self, session_id: str, ttl_seconds: Optional[int] = None) -> Optional[LeaseRecord]:
        ttl = ttl_seconds if ttl_seconds is not None else settings.default_session_ttl
        return await self._backend.heartbeat(session_id, ttl)

    async def get(self, session_id: str) -> Optional[LeaseRecord]:
        return await self._backend.get_lease(session_id)

    async def release(self, session_id: str) -> bool:
        return await self._backend.release_lease(session_id)

    async def get_expired(self) -> List[LeaseRecord]:
        return await self._backend.get_expired_leases()

    async def mark_expired(self, session_id: str) -> None:
        await self._backend.mark_expired(session_id)

    async def list_active(self) -> List[LeaseRecord]:
        return await self._backend.list_active_leases()

    # Workspace Lock Operations
    async def acquire_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: Optional[int] = None
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        ttl = ttl_seconds if ttl_seconds is not None else settings.default_session_ttl
        return await self._backend.acquire_workspace_lock(workspace_id, session_id, ttl)

    async def release_workspace_lock(self, workspace_id: str, session_id: str) -> bool:
        return await self._backend.release_workspace_lock(workspace_id, session_id)

    async def refresh_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: Optional[int] = None
    ) -> bool:
        ttl = ttl_seconds if ttl_seconds is not None else settings.default_session_ttl
        return await self._backend.refresh_workspace_lock(workspace_id, session_id, ttl)

    async def get_workspace_lock(self, workspace_id: str) -> Optional[Tuple[str, float]]:
        return await self._backend.get_workspace_lock(workspace_id)

    async def cleanup_expired_workspace_locks(self) -> int:
        return await self._backend.cleanup_expired_workspace_locks()

    async def count_active_locks(self) -> int:
        return await self._backend.count_active_locks()
