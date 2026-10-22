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
from .security import decrypt_token, encrypt_token

logger = logging.getLogger("sandbox.lease_manager")


class LeaseBackend(ABC):
    @abstractmethod
    async def create_or_renew_lease(
        self,
        session_id: str,
        workspace_id: str,
        sandbox_id: str,
        endpoint: str,
        ttl_seconds: int,
        auth_token: Optional[str] = None,
        tenant_id: str = "default",
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
    async def claim_expired_leases(self, cutoff_time: float) -> List[LeaseRecord]:
        pass

    @abstractmethod
    async def mark_expired(self, session_id: str, from_status: Optional[str] = None) -> bool:
        pass

    @abstractmethod
    async def list_active_leases(self) -> List[LeaseRecord]:
        pass

    @abstractmethod
    async def log_audit_event(
        self,
        session_id: str,
        event_type: str = "custom",
        details: Optional[dict] = None,
        workspace_id: str = "",
    ):
        pass

    # Concurrency Lock APIs (strictly scoped to tenant_id, workspace_id)
    @abstractmethod
    async def acquire_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        """
        Atomically acquire or refresh workspace lock scoped to (tenant_id, workspace_id).
        Returns: (success, locked_by_session_id, expires_at)
        """
        pass

    @abstractmethod
    async def release_workspace_lock(self, workspace_id: str, session_id: str, tenant_id: str = "default") -> bool:
        pass

    @abstractmethod
    async def refresh_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> bool:
        pass

    @abstractmethod
    async def get_workspace_lock(self, workspace_id: str, tenant_id: str = "default") -> Optional[Tuple[str, float]]:
        pass

    @abstractmethod
    async def cleanup_expired_workspace_locks(self) -> int:
        pass

    @abstractmethod
    async def count_active_locks(self) -> int:
        pass

    @abstractmethod
    async def claim_workspace_ownership(self, workspace_id: str, tenant_id: str = "default") -> Tuple[bool, str]:
        """
        Registers or verifies tenant ownership of a workspace_id scoped to tenant_id.
        Returns: (is_owned_by_tenant, actual_owner_tenant_id)
        """
        pass

    @abstractmethod
    async def reserve_session_slot(
        self, tenant_id: str, max_concurrent: int, session_id: Optional[str] = None, workspace_id: Optional[str] = None
    ) -> bool:
        """
        Atomically checks and reserves a session slot under tenant quota to prevent check-then-act race conditions.
        """
        pass

    @abstractmethod
    async def count_active_sessions_for_tenant(self, tenant_id: str) -> int:
        pass

    @abstractmethod
    async def register_engine_heartbeat(self, engine_id: str) -> None:
        pass

    @abstractmethod
    async def get_dead_engines(self, max_idle_sec: float = 60.0) -> List[str]:
        pass

    @abstractmethod
    async def delete_dead_engine(self, engine_id: str) -> None:
        pass

    @abstractmethod
    async def prune_database_records(self, retention_seconds: float = 86400.0 * 7) -> None:
        pass

    @abstractmethod
    async def try_acquire_sweeper_leader(self, engine_id: str, ttl_seconds: float = 15.0) -> bool:
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
                    status TEXT,
                    auth_token TEXT,
                    tenant_id TEXT DEFAULT 'default'
                )
                """
            )
            try:
                conn.execute("ALTER TABLE leases ADD COLUMN auth_token TEXT")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE leases ADD COLUMN tenant_id TEXT DEFAULT 'default'")
            except Exception:
                pass

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workspace_locks (
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    workspace_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (tenant_id, workspace_id)
                )
                """
            )
            try:
                conn.execute("ALTER TABLE workspace_locks ADD COLUMN tenant_id TEXT DEFAULT 'default'")
            except Exception:
                pass

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workspaces (
                    tenant_id TEXT NOT NULL,
                    workspace_id TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (tenant_id, workspace_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS engine_instances (
                    engine_id TEXT PRIMARY KEY,
                    last_heartbeat REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sweeper_leader (
                    lock_id INTEGER PRIMARY KEY DEFAULT 1,
                    leader_id TEXT NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            conn.commit()

    async def create_or_renew_lease(
        self,
        session_id: str,
        workspace_id: str,
        sandbox_id: str,
        endpoint: str,
        ttl_seconds: int,
        auth_token: Optional[str] = None,
        tenant_id: str = "default",
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
                auth_token=auth_token,
                tenant_id=tenant_id,
            )
            await asyncio.to_thread(self._sync_save, record)
            return record

    def _sync_save(self, record: LeaseRecord):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO leases (session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.session_id,
                    record.workspace_id,
                    record.sandbox_id,
                    record.endpoint,
                    record.created_at,
                    record.expires_at,
                    record.status,
                    record.auth_token,
                    getattr(record, "tenant_id", "default"),
                ),
            )
            conn.commit()

    async def claim_expired_leases(self, cutoff_time: float) -> List[LeaseRecord]:
        async with self._lock:
            return await asyncio.to_thread(self._sync_claim_expired_leases, cutoff_time)

    def _sync_claim_expired_leases(self, cutoff_time: float) -> List[LeaseRecord]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id FROM leases WHERE status = 'active' AND expires_at < ?",
                (cutoff_time,),
            )
            rows = cursor.fetchall()
            if rows:
                conn.execute(
                    "UPDATE leases SET status = 'reclaiming' WHERE status = 'active' AND expires_at < ?",
                    (cutoff_time,),
                )
                conn.commit()
            return [
                LeaseRecord(
                    session_id=r[0],
                    workspace_id=r[1],
                    sandbox_id=r[2],
                    endpoint=r[3],
                    created_at=r[4],
                    expires_at=r[5],
                    status="reclaiming",
                    auth_token=r[7] if len(r) > 7 else None,
                    tenant_id=r[8] if len(r) > 8 and r[8] else "default",
                )
                for r in rows
            ]

    async def log_audit_event(
        self,
        session_id: str,
        event_type: str = "custom",
        details: Optional[dict] = None,
        workspace_id: str = "",
    ):
        await asyncio.to_thread(self._sync_log_audit_event, session_id, event_type, details or {}, workspace_id)

    def _sync_log_audit_event(self, session_id: str, event_type: str, details: dict, workspace_id: str):
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS session_audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    workspace_id TEXT,
                    event_type TEXT NOT NULL,
                    details TEXT,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO session_audit_logs (session_id, workspace_id, event_type, details, created_at) VALUES (?, ?, ?, ?, ?)",
                (session_id, workspace_id, event_type, json.dumps(details), now),
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
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id FROM leases WHERE session_id = ?",
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
                    auth_token=row[7] if len(row) > 7 else None,
                    tenant_id=row[8] if len(row) > 8 and row[8] else "default",
                )
        return None


    async def release_lease(self, session_id: str) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._sync_update_status, session_id, "released")

    async def mark_expired(self, session_id: str, from_status: Optional[str] = None) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._sync_mark_expired, session_id, from_status)

    def _sync_mark_expired(self, session_id: str, from_status: Optional[str] = None) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            # Resolves N3: If from_status is specified ('reclaiming' or 'active'), only match that status.
            # If not specified, match any active or reclaiming lease to mark expired.
            if from_status:
                cursor = conn.execute(
                    "UPDATE leases SET status = 'expired' WHERE session_id = ? AND status = ?",
                    (session_id, from_status),
                )
            else:
                cursor = conn.execute(
                    "UPDATE leases SET status = 'expired' WHERE session_id = ? AND status IN ('active', 'reclaiming')",
                    (session_id,),
                )
            conn.commit()
            return cursor.rowcount > 0

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
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id FROM leases WHERE status = 'active' AND expires_at < ?",
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
                    auth_token=r[7] if len(r) > 7 else None,
                    tenant_id=r[8] if len(r) > 8 and r[8] else "default",
                )
                for r in rows
            ]

    async def list_active_leases(self) -> List[LeaseRecord]:
        return await asyncio.to_thread(self._sync_list_active)

    def _sync_list_active(self) -> List[LeaseRecord]:
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id FROM leases WHERE status = 'active' AND expires_at >= ?",
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
                    auth_token=r[7] if len(r) > 7 else None,
                    tenant_id=r[8] if len(r) > 8 and r[8] else "default",
                )
                for r in rows
            ]

    # Concurrency Lock Implementations (scoped to tenant_id and workspace_id)
    async def acquire_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        async with self._lock:
            return await asyncio.to_thread(self._sync_acquire_lock, workspace_id, session_id, ttl_seconds, tenant_id)

    def _sync_acquire_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        now = time.time()
        new_expires_at = now + ttl_seconds
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, expires_at FROM workspace_locks WHERE tenant_id = ? AND workspace_id = ?",
                (tenant_id, workspace_id),
            )
            row = cursor.fetchone()
            if row:
                holder, exp = row[0], float(row[1])
                if exp > now and holder != session_id:
                    # Still held by another active session for this tenant
                    return False, holder, exp

            # Lock is either free, expired, or held by same session -> acquire/renew
            conn.execute(
                "INSERT OR REPLACE INTO workspace_locks (tenant_id, workspace_id, session_id, expires_at) VALUES (?, ?, ?, ?)",
                (tenant_id, workspace_id, session_id, new_expires_at),
            )
            conn.commit()
            return True, session_id, new_expires_at

    async def release_workspace_lock(self, workspace_id: str, session_id: str, tenant_id: str = "default") -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._sync_release_lock, workspace_id, session_id, tenant_id)

    def _sync_release_lock(self, workspace_id: str, session_id: str, tenant_id: str = "default") -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM workspace_locks WHERE tenant_id = ? AND workspace_id = ? AND session_id = ?",
                (tenant_id, workspace_id, session_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    async def refresh_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._sync_refresh_lock, workspace_id, session_id, ttl_seconds, tenant_id)

    def _sync_refresh_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> bool:
        now = time.time()
        new_exp = now + ttl_seconds
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE workspace_locks SET expires_at = ? WHERE tenant_id = ? AND workspace_id = ? AND session_id = ?",
                (new_exp, tenant_id, workspace_id, session_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    async def get_workspace_lock(self, workspace_id: str, tenant_id: str = "default") -> Optional[Tuple[str, float]]:
        return await asyncio.to_thread(self._sync_get_lock, workspace_id, tenant_id)

    def _sync_get_lock(self, workspace_id: str, tenant_id: str = "default") -> Optional[Tuple[str, float]]:
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT session_id, expires_at FROM workspace_locks WHERE tenant_id = ? AND workspace_id = ? AND expires_at > ?",
                (tenant_id, workspace_id, now),
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

    async def claim_workspace_ownership(self, workspace_id: str, tenant_id: str = "default") -> Tuple[bool, str]:
        return await asyncio.to_thread(self._sync_claim_workspace, workspace_id, tenant_id)

    async def count_tenant_workspaces(self, tenant_id: str) -> int:
        return await asyncio.to_thread(self._sync_count_tenant_workspaces, tenant_id)

    def _sync_count_tenant_workspaces(self, tenant_id: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM workspaces WHERE tenant_id = ?",
                (tenant_id,),
            ).fetchone()
            return int(row[0]) if row else 0

    async def tenant_has_workspace(self, tenant_id: str, workspace_id: str) -> bool:
        return await asyncio.to_thread(self._sync_tenant_has_workspace, tenant_id, workspace_id)

    def _sync_tenant_has_workspace(self, tenant_id: str, workspace_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT 1 FROM workspaces WHERE tenant_id = ? AND workspace_id = ?",
                (tenant_id, workspace_id),
            ).fetchone()
            return row is not None

    def _sync_claim_workspace(self, workspace_id: str, tenant_id: str = "default") -> Tuple[bool, str]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT tenant_id FROM workspaces WHERE tenant_id = ? AND workspace_id = ?",
                (tenant_id, workspace_id),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    "INSERT INTO workspaces (tenant_id, workspace_id, created_at) VALUES (?, ?, ?)",
                    (tenant_id, workspace_id, time.time()),
                )
                conn.commit()
            return (True, tenant_id)

    async def reserve_session_slot(
        self, tenant_id: str, max_concurrent: int, session_id: Optional[str] = None, workspace_id: Optional[str] = None
    ) -> bool:
        return await asyncio.to_thread(self._sync_reserve_session_slot, tenant_id, max_concurrent, session_id, workspace_id)

    def _sync_reserve_session_slot(
        self, tenant_id: str, max_concurrent: int, session_id: Optional[str] = None, workspace_id: Optional[str] = None
    ) -> bool:
        if max_concurrent <= 0:
            return True
        now = time.time()
        with sqlite3.connect(self.db_path, timeout=10.0) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                "SELECT count(*) FROM leases WHERE tenant_id = ? AND status = 'active' AND expires_at > ?",
                (tenant_id, now),
            )
            row = cursor.fetchone()
            active = int(row[0]) if row else 0
            if active >= max_concurrent:
                return False
            if session_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO leases (session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id) "
                    "VALUES (?, ?, 'pending', 'pending', ?, ?, 'active', '', ?)",
                    (session_id, workspace_id or "", now, now + 120, tenant_id),
                )
            conn.commit()
            return True

    async def count_active_sessions_for_tenant(self, tenant_id: str) -> int:
        return await asyncio.to_thread(self._sync_count_tenant_sessions, tenant_id)

    def _sync_count_tenant_sessions(self, tenant_id: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT count(*) FROM leases WHERE tenant_id = ? AND status = 'active'", (tenant_id,))
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    async def register_engine_heartbeat(self, engine_id: str) -> None:
        await asyncio.to_thread(self._sync_register_engine, engine_id)

    def _sync_register_engine(self, engine_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO engine_instances (engine_id, last_heartbeat) VALUES (?, ?)",
                (engine_id, time.time()),
            )
            conn.commit()

    async def get_dead_engines(self, max_idle_sec: float = 60.0) -> List[str]:
        return await asyncio.to_thread(self._sync_get_dead_engines, max_idle_sec)

    def _sync_get_dead_engines(self, max_idle_sec: float) -> List[str]:
        cutoff = time.time() - max_idle_sec
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT engine_id FROM engine_instances WHERE last_heartbeat < ?", (cutoff,))
            rows = cursor.fetchall()
            return [r[0] for r in rows]

    async def delete_dead_engine(self, engine_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._sync_delete_dead_engine, engine_id)

    def _sync_delete_dead_engine(self, engine_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM engine_instances WHERE engine_id = ?", (engine_id,))
            conn.commit()

    async def prune_database_records(self, retention_seconds: float = 86400.0 * 7) -> None:
        async with self._lock:
            await asyncio.to_thread(self._sync_prune_database_records, retention_seconds)

    def _sync_prune_database_records(self, retention_seconds: float):
        cutoff = time.time() - retention_seconds
        stale_engine_cutoff = time.time() - 86400.0
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM leases WHERE status IN ('expired', 'released') AND expires_at < ?", (cutoff,))
            conn.execute("DELETE FROM session_audit_logs WHERE created_at < ?", (cutoff,))
            conn.execute("DELETE FROM engine_instances WHERE last_heartbeat < ?", (stale_engine_cutoff,))
            conn.commit()

    async def try_acquire_sweeper_leader(self, engine_id: str, ttl_seconds: float = 15.0) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._sync_try_acquire_sweeper_leader, engine_id, ttl_seconds)

    def _sync_try_acquire_sweeper_leader(self, engine_id: str, ttl_seconds: float) -> bool:
        now = time.time()
        new_expires = now + ttl_seconds
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT leader_id, expires_at FROM sweeper_leader WHERE lock_id = 1")
            row = cursor.fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO sweeper_leader (lock_id, leader_id, expires_at) VALUES (1, ?, ?)",
                    (engine_id, new_expires),
                )
                conn.commit()
                return True
            curr_leader, curr_expires = row
            if curr_leader == engine_id or curr_expires < now:
                conn.execute(
                    "UPDATE sweeper_leader SET leader_id = ?, expires_at = ? WHERE lock_id = 1",
                    (engine_id, new_expires),
                )
                conn.commit()
                return True
            return False


class RedisLeaseBackend(LeaseBackend):
    def __init__(self, redis_client):
        self.redis = redis_client

    async def create_or_renew_lease(
        self,
        session_id: str,
        workspace_id: str,
        sandbox_id: str,
        endpoint: str,
        ttl_seconds: int,
        auth_token: Optional[str] = None,
        tenant_id: str = "default",
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
            auth_token=auth_token,
            tenant_id=tenant_id,
        )

        data = record.model_dump_json()
        await self.redis.set(f"lease:{session_id}", data, ex=ttl_seconds)
        await self.redis.sadd("leases:active", session_id)
        return record

    async def claim_expired_leases(self, cutoff_time: float) -> List[LeaseRecord]:
        expired = []
        active_ids = await self.redis.smembers("leases:active")
        for sid_bytes in active_ids:
            sid = sid_bytes.decode("utf-8") if isinstance(sid_bytes, bytes) else str(sid_bytes)
            lease = await self.get_lease(sid)
            if lease and lease.status == "active" and lease.expires_at < cutoff_time:
                lease.status = "reclaiming"
                await self.redis.set(f"lease:{sid}", lease.model_dump_json(), ex=60)
                expired.append(lease)
            elif not lease:
                expired.append(
                    LeaseRecord(
                        session_id=sid,
                        workspace_id="",
                        sandbox_id="",
                        endpoint="",
                        created_at=0,
                        expires_at=0,
                        status="reclaiming",
                    )
                )
        return expired

    async def log_audit_event(
        self,
        session_id: str,
        event_type: str = "custom",
        details: Optional[dict] = None,
        workspace_id: str = "",
    ):
        event = {
            "session_id": session_id,
            "workspace_id": workspace_id,
            "event_type": event_type,
            "details": details or {},
            "created_at": time.time(),
        }
        await self.redis.lpush(f"audit:{session_id}", json.dumps(event))
        await self.redis.ltrim(f"audit:{session_id}", 0, 99)

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

    async def mark_expired(self, session_id: str, from_status: Optional[str] = None) -> bool:
        await self.redis.srem("leases:active", session_id)
        raw = await self.redis.get(f"lease:{session_id}")
        if raw:
            try:
                data = json.loads(raw)
                curr_status = data.get("status")
                if from_status is None or curr_status == from_status or curr_status in ("active", "reclaiming"):
                    data["status"] = "expired"
                    await self.redis.set(f"lease:{session_id}", json.dumps(data))
                    return True
                return False
            except Exception:
                pass
        return True

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
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        key = f"lock:workspace:{tenant_id}:{workspace_id}"
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
            await self.redis.sadd("workspace_locks:active", f"{tenant_id}:{workspace_id}")
            return True, session_id, now + ttl_seconds

        existing = await self.redis.get(key)
        holder = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing) if existing else "unknown"
        ttl_rem = await self.redis.ttl(key)
        return False, holder, now + max(0, ttl_rem)

    async def release_workspace_lock(self, workspace_id: str, session_id: str, tenant_id: str = "default") -> bool:
        key = f"lock:workspace:{tenant_id}:{workspace_id}"
        existing = await self.redis.get(key)
        if not existing:
            await self.redis.srem("workspace_locks:active", f"{tenant_id}:{workspace_id}")
            return False
        holder = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing)
        if holder == session_id:
            await self.redis.delete(key)
            await self.redis.srem("workspace_locks:active", f"{tenant_id}:{workspace_id}")
            return True
        return False

    async def refresh_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> bool:
        key = f"lock:workspace:{tenant_id}:{workspace_id}"
        existing = await self.redis.get(key)
        if not existing:
            return False
        holder = existing.decode("utf-8") if isinstance(existing, bytes) else str(existing)
        if holder == session_id:
            await self.redis.expire(key, ttl_seconds)
            return True
        return False

    async def get_workspace_lock(self, workspace_id: str, tenant_id: str = "default") -> Optional[Tuple[str, float]]:
        key = f"lock:workspace:{tenant_id}:{workspace_id}"
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

    async def claim_workspace_ownership(self, workspace_id: str, tenant_id: str = "default") -> Tuple[bool, str]:
        return (True, tenant_id)

    async def reserve_session_slot(
        self, tenant_id: str, max_concurrent: int, session_id: Optional[str] = None, workspace_id: Optional[str] = None
    ) -> bool:
        if max_concurrent <= 0:
            return True
        active = await self.count_active_sessions_for_tenant(tenant_id)
        if active >= max_concurrent:
            return False
        if session_id:
            await self.create_or_renew_lease(
                session_id=session_id,
                workspace_id=workspace_id or "",
                sandbox_id="pending",
                endpoint="pending",
                ttl_seconds=120,
                tenant_id=tenant_id,
            )
        return True

    async def count_active_sessions_for_tenant(self, tenant_id: str) -> int:
        leases = await self.list_active_leases()
        return sum(1 for l in leases if getattr(l, "tenant_id", "default") == tenant_id and l.status == "active")

    async def register_engine_heartbeat(self, engine_id: str) -> None:
        now = time.time()
        await self.redis.set(f"engine:heartbeat:{engine_id}", str(now), ex=120)
        await self.redis.sadd("engines:active", engine_id)

    async def get_dead_engines(self, max_idle_sec: float = 60.0) -> List[str]:
        now = time.time()
        dead = []
        engine_ids = await self.redis.smembers("engines:active")
        for eid_bytes in engine_ids:
            eid = eid_bytes.decode("utf-8") if isinstance(eid_bytes, bytes) else str(eid_bytes)
            raw = await self.redis.get(f"engine:heartbeat:{eid}")
            if not raw or (now - float(raw.decode("utf-8") if isinstance(raw, bytes) else str(raw))) > max_idle_sec:
                dead.append(eid)
        return dead

    async def delete_dead_engine(self, engine_id: str) -> None:
        await self.redis.delete(f"engine:heartbeat:{engine_id}")
        await self.redis.srem("engines:active", engine_id)

    async def prune_database_records(self, retention_seconds: float = 86400.0 * 7) -> None:
        await self.cleanup_expired_workspace_locks()

    async def try_acquire_sweeper_leader(self, engine_id: str, ttl_seconds: float = 15.0) -> bool:
        key = "sweeper:leader"
        acquired = await self.redis.set(key, engine_id, nx=True, ex=int(ttl_seconds))
        if acquired:
            return True
        curr = await self.redis.get(key)
        curr_str = curr.decode("utf-8") if isinstance(curr, bytes) else str(curr) if curr else ""
        if curr_str == engine_id:
            await self.redis.expire(key, int(ttl_seconds))
            return True
        return False


class PostgresLeaseBackend(LeaseBackend):
    """
    Production-grade PostgreSQL backend for distributed lease management,
    row-level atomic workspace locking, and audit trail persistence.
    """

    def __init__(self, postgres_url: str, min_pool: int = 2, max_pool: int = 20):
        self.postgres_url = postgres_url
        self.min_pool = min_pool
        self.max_pool = max_pool
        self._pool = None

    async def initialize(self):
        import asyncpg

        self._pool = await asyncpg.create_pool(
            dsn=self.postgres_url,
            min_size=self.min_pool,
            max_size=self.max_pool,
            command_timeout=10.0,
        )
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leases (
                    session_id VARCHAR(128) PRIMARY KEY,
                    workspace_id VARCHAR(128) NOT NULL,
                    sandbox_id VARCHAR(128) NOT NULL,
                    endpoint VARCHAR(256) NOT NULL,
                    created_at DOUBLE PRECISION NOT NULL,
                    expires_at DOUBLE PRECISION NOT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'active',
                    auth_token TEXT,
                    tenant_id VARCHAR(128) DEFAULT 'default'
                );
                ALTER TABLE leases ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(128) DEFAULT 'default';
                CREATE INDEX IF NOT EXISTS idx_leases_workspace_id ON leases(workspace_id);
                CREATE INDEX IF NOT EXISTS idx_leases_status_expires ON leases(status, expires_at);
                CREATE INDEX IF NOT EXISTS idx_leases_tenant_id ON leases(tenant_id);

                CREATE TABLE IF NOT EXISTS workspace_locks (
                    tenant_id VARCHAR(128) NOT NULL DEFAULT 'default',
                    workspace_id VARCHAR(128) NOT NULL,
                    session_id VARCHAR(128) NOT NULL,
                    acquired_at DOUBLE PRECISION NOT NULL,
                    expires_at DOUBLE PRECISION NOT NULL,
                    PRIMARY KEY (tenant_id, workspace_id)
                );
                CREATE INDEX IF NOT EXISTS idx_workspace_locks_expires ON workspace_locks(expires_at);

                CREATE TABLE IF NOT EXISTS session_audit_logs (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(128) NOT NULL,
                    workspace_id VARCHAR(128) NOT NULL,
                    event_type VARCHAR(64) NOT NULL,
                    details TEXT DEFAULT '{}',
                    created_at DOUBLE PRECISION NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_session ON session_audit_logs(session_id);

                CREATE TABLE IF NOT EXISTS workspaces (
                    tenant_id VARCHAR(128) NOT NULL,
                    workspace_id VARCHAR(128) NOT NULL,
                    created_at DOUBLE PRECISION NOT NULL,
                    PRIMARY KEY (tenant_id, workspace_id)
                );
                CREATE INDEX IF NOT EXISTS idx_workspaces_tenant ON workspaces(tenant_id);

                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'workspace_locks' AND column_name = 'workspace_id') THEN
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'workspace_locks' AND column_name = 'tenant_id') THEN
                            ALTER TABLE workspace_locks ADD COLUMN tenant_id VARCHAR(128) NOT NULL DEFAULT 'default';
                        END IF;
                        BEGIN
                            ALTER TABLE workspace_locks DROP CONSTRAINT IF EXISTS workspace_locks_pkey;
                            ALTER TABLE workspace_locks ADD PRIMARY KEY (tenant_id, workspace_id);
                        EXCEPTION WHEN OTHERS THEN
                            NULL;
                        END;
                    END IF;
                    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'workspaces' AND column_name = 'workspace_id') THEN
                        BEGIN
                            ALTER TABLE workspaces DROP CONSTRAINT IF EXISTS workspaces_pkey;
                            ALTER TABLE workspaces ADD PRIMARY KEY (tenant_id, workspace_id);
                        EXCEPTION WHEN OTHERS THEN
                            NULL;
                        END;
                    END IF;
                END $$;

                CREATE TABLE IF NOT EXISTS engine_instances (
                    engine_id VARCHAR(128) PRIMARY KEY,
                    last_heartbeat DOUBLE PRECISION NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_engine_heartbeat ON engine_instances(last_heartbeat);

                CREATE TABLE IF NOT EXISTS sweeper_leader (
                    lock_id INT PRIMARY KEY DEFAULT 1,
                    leader_id VARCHAR(128) NOT NULL,
                    expires_at DOUBLE PRECISION NOT NULL
                );
                """
            )

    async def ping(self) -> bool:
        if not self._pool:
            return False
        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                return True
        except Exception:
            return False

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def create_or_renew_lease(
        self,
        session_id: str,
        workspace_id: str,
        sandbox_id: str,
        endpoint: str,
        ttl_seconds: int,
        auth_token: Optional[str] = None,
        tenant_id: str = "default",
    ) -> LeaseRecord:
        now = time.time()
        expires_at = now + ttl_seconds
        encrypted_token = encrypt_token(auth_token)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO leases (session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id)
                VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, $8)
                ON CONFLICT (session_id) DO UPDATE SET
                    sandbox_id = EXCLUDED.sandbox_id,
                    workspace_id = EXCLUDED.workspace_id,
                    expires_at = EXCLUDED.expires_at,
                    status = 'active',
                    endpoint = EXCLUDED.endpoint,
                    auth_token = COALESCE(EXCLUDED.auth_token, leases.auth_token),
                    tenant_id = EXCLUDED.tenant_id
                RETURNING session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id
                """,
                session_id,
                workspace_id,
                sandbox_id,
                endpoint,
                now,
                expires_at,
                encrypted_token,
                tenant_id,
            )
            await conn.execute(
                "INSERT INTO session_audit_logs (session_id, workspace_id, event_type, details, created_at) VALUES ($1, $2, 'lease_created', $3, $4)",
                session_id,
                workspace_id,
                json.dumps({"sandbox_id": sandbox_id, "endpoint": endpoint, "ttl": ttl_seconds, "tenant_id": tenant_id}),
                now,
            )
            return LeaseRecord(
                session_id=row["session_id"],
                workspace_id=row["workspace_id"],
                sandbox_id=row["sandbox_id"],
                endpoint=row["endpoint"],
                created_at=row["created_at"],
                expires_at=row["expires_at"],
                status=row["status"],
                auth_token=decrypt_token(row["auth_token"]),
                tenant_id=row.get("tenant_id") or "default",
            )

    async def heartbeat(self, session_id: str, ttl_seconds: int) -> Optional[LeaseRecord]:
        now = time.time()
        expires_at = now + ttl_seconds
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE leases
                SET expires_at = $2, status = 'active'
                WHERE session_id = $1 AND status = 'active'
                RETURNING session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id
                """,
                session_id,
                expires_at,
            )
            if not row:
                return None
            return LeaseRecord(
                session_id=row["session_id"],
                workspace_id=row["workspace_id"],
                sandbox_id=row["sandbox_id"],
                endpoint=row["endpoint"],
                created_at=row["created_at"],
                expires_at=row["expires_at"],
                status=row["status"],
                auth_token=decrypt_token(row["auth_token"]),
                tenant_id=row.get("tenant_id") or "default",
            )

    async def get_lease(self, session_id: str) -> Optional[LeaseRecord]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id FROM leases WHERE session_id = $1",
                session_id,
            )
            if not row:
                return None
            return LeaseRecord(
                session_id=row["session_id"],
                workspace_id=row["workspace_id"],
                sandbox_id=row["sandbox_id"],
                endpoint=row["endpoint"],
                created_at=row["created_at"],
                expires_at=row["expires_at"],
                status=row["status"],
                auth_token=decrypt_token(row["auth_token"]),
                tenant_id=row.get("tenant_id") or "default",
            )

    async def release_lease(self, session_id: str) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE leases SET status = 'released' WHERE session_id = $1 AND status = 'active' RETURNING workspace_id",
                session_id,
            )
            if row:
                ws_id = row["workspace_id"]
                await conn.execute("DELETE FROM workspace_locks WHERE workspace_id = $1 AND session_id = $2", ws_id, session_id)
                await conn.execute(
                    "INSERT INTO session_audit_logs (session_id, workspace_id, event_type, details, created_at) VALUES ($1, $2, 'lease_released', '{}', $3)",
                    session_id,
                    ws_id,
                    time.time(),
                )
                return True
            return False

    async def get_expired_leases(self) -> List[LeaseRecord]:
        now = time.time()
        return await self.claim_expired_leases(now)

    async def claim_expired_leases(self, cutoff_time: float) -> List[LeaseRecord]:
        """Atomically transitions active expired leases to 'reclaiming' to eliminate heartbeat race conditions."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                UPDATE leases
                SET status = 'reclaiming'
                WHERE status = 'active' AND expires_at < $1
                RETURNING session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id
                """,
                cutoff_time,
            )
            return [
                LeaseRecord(
                    session_id=r["session_id"],
                    workspace_id=r["workspace_id"],
                    sandbox_id=r["sandbox_id"],
                    endpoint=r["endpoint"],
                    created_at=r["created_at"],
                    expires_at=r["expires_at"],
                    status=r["status"],
                    auth_token=decrypt_token(r["auth_token"]),
                    tenant_id=r.get("tenant_id") or "default",
                )
                for r in rows
            ]

    async def mark_expired(self, session_id: str, from_status: Optional[str] = None) -> bool:
        async with self._pool.acquire() as conn:
            # Resolves N3: If from_status is specified ('reclaiming' or 'active'), only match that status.
            # If not specified, match any active or reclaiming lease to mark expired.
            if from_status:
                row = await conn.fetchrow(
                    "UPDATE leases SET status = 'expired' WHERE session_id = $1 AND status = $2 RETURNING workspace_id",
                    session_id,
                    from_status,
                )
            else:
                row = await conn.fetchrow(
                    "UPDATE leases SET status = 'expired' WHERE session_id = $1 AND status IN ('active', 'reclaiming') RETURNING workspace_id",
                    session_id,
                )
            if row:
                ws_id = row["workspace_id"]
                await conn.execute("DELETE FROM workspace_locks WHERE workspace_id = $1 AND session_id = $2", ws_id, session_id)
                await conn.execute(
                    "INSERT INTO session_audit_logs (session_id, workspace_id, event_type, details, created_at) VALUES ($1, $2, 'lease_expired', '{}', $3)",
                    session_id,
                    ws_id,
                    time.time(),
                )
                return True
            return False

    async def list_active_leases(self) -> List[LeaseRecord]:
        now = time.time()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id FROM leases WHERE status = 'active' AND expires_at >= $1",
                now,
            )
            return [
                LeaseRecord(
                    session_id=r["session_id"],
                    workspace_id=r["workspace_id"],
                    sandbox_id=r["sandbox_id"],
                    endpoint=r["endpoint"],
                    created_at=r["created_at"],
                    expires_at=r["expires_at"],
                    status=r["status"],
                    auth_token=decrypt_token(r["auth_token"]),
                    tenant_id=r.get("tenant_id") or "default",
                )
                for r in rows
            ]

    async def log_audit_event(
        self,
        session_id: str,
        event_type: str = "custom",
        details: Optional[dict] = None,
        workspace_id: str = "",
    ):
        now = time.time()
        async with self._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO session_audit_logs (session_id, workspace_id, event_type, details, created_at) VALUES ($1, $2, $3, $4, $5)",
                session_id,
                workspace_id or "",
                event_type,
                json.dumps(details or {}),
                now,
            )

    async def acquire_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        now = time.time()
        expires_at = now + ttl_seconds
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT session_id, expires_at FROM workspace_locks WHERE tenant_id = $1 AND workspace_id = $2 FOR UPDATE",
                    tenant_id,
                    workspace_id,
                )
                if row:
                    locked_by = row["session_id"]
                    current_expires = row["expires_at"]
                    if current_expires >= now and locked_by != session_id:
                        return False, locked_by, current_expires

                    await conn.execute(
                        "UPDATE workspace_locks SET session_id = $3, acquired_at = $4, expires_at = $5 WHERE tenant_id = $1 AND workspace_id = $2",
                        tenant_id,
                        workspace_id,
                        session_id,
                        now,
                        expires_at,
                    )
                    return True, session_id, expires_at
                else:
                    await conn.execute(
                        "INSERT INTO workspace_locks (tenant_id, workspace_id, session_id, acquired_at, expires_at) VALUES ($1, $2, $3, $4, $5)",
                        tenant_id,
                        workspace_id,
                        session_id,
                        now,
                        expires_at,
                    )
                    return True, session_id, expires_at

    async def release_workspace_lock(self, workspace_id: str, session_id: str, tenant_id: str = "default") -> bool:
        async with self._pool.acquire() as conn:
            res = await conn.execute(
                "DELETE FROM workspace_locks WHERE tenant_id = $1 AND workspace_id = $2 AND session_id = $3",
                tenant_id,
                workspace_id,
                session_id,
            )
            return res == "DELETE 1"

    async def refresh_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: int, tenant_id: str = "default"
    ) -> bool:
        now = time.time()
        expires_at = now + ttl_seconds
        async with self._pool.acquire() as conn:
            res = await conn.execute(
                "UPDATE workspace_locks SET expires_at = $4 WHERE tenant_id = $1 AND workspace_id = $2 AND session_id = $3 AND expires_at >= $5",
                tenant_id,
                workspace_id,
                session_id,
                expires_at,
                now,
            )
            return res == "UPDATE 1"

    async def get_workspace_lock(self, workspace_id: str, tenant_id: str = "default") -> Optional[Tuple[str, float]]:
        now = time.time()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT session_id, expires_at FROM workspace_locks WHERE tenant_id = $1 AND workspace_id = $2 AND expires_at >= $3",
                tenant_id,
                workspace_id,
                now,
            )
            if not row:
                return None
            return row["session_id"], row["expires_at"]

    async def cleanup_expired_workspace_locks(self) -> int:
        now = time.time()
        async with self._pool.acquire() as conn:
            res = await conn.execute("DELETE FROM workspace_locks WHERE expires_at < $1", now)
            parts = res.split()
            return int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

    async def count_active_locks(self) -> int:
        now = time.time()
        async with self._pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM workspace_locks WHERE expires_at >= $1", now)
            return int(count or 0)

    async def count_tenant_workspaces(self, tenant_id: str) -> int:
        async with self._pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM workspaces WHERE tenant_id = $1",
                tenant_id,
            )
            return int(count or 0)

    async def tenant_has_workspace(self, tenant_id: str, workspace_id: str) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchval(
                "SELECT 1 FROM workspaces WHERE tenant_id = $1 AND workspace_id = $2",
                tenant_id,
                workspace_id,
            )
            return row is not None

    async def claim_workspace_ownership(self, workspace_id: str, tenant_id: str = "default") -> Tuple[bool, str]:
        now = time.time()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO workspaces (tenant_id, workspace_id, created_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (tenant_id, workspace_id) DO NOTHING
                """,
                tenant_id,
                workspace_id,
                now,
            )
            return (True, tenant_id)

    async def reserve_session_slot(
        self, tenant_id: str, max_concurrent: int, session_id: Optional[str] = None, workspace_id: Optional[str] = None
    ) -> bool:
        if max_concurrent <= 0:
            return True
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Advisory transaction-level lock scoped to tenant_id hash
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext('tenant_quota:' || $1))", tenant_id)
                now = time.time()
                val = await conn.fetchval(
                    "SELECT count(*) FROM leases WHERE tenant_id = $1 AND status = 'active' AND expires_at > $2",
                    tenant_id,
                    now,
                )
                active = int(val or 0)
                if active >= max_concurrent:
                    return False
                if session_id:
                    await conn.execute(
                        """
                        INSERT INTO leases (session_id, workspace_id, sandbox_id, endpoint, created_at, expires_at, status, auth_token, tenant_id)
                        VALUES ($1, $2, 'pending', 'pending', $3, $4, 'active', '', $5)
                        ON CONFLICT (session_id) DO NOTHING
                        """,
                        session_id,
                        workspace_id or "",
                        now,
                        now + 120,
                        tenant_id,
                    )
                return True

    async def count_active_sessions_for_tenant(self, tenant_id: str) -> int:
        async with self._pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT count(*) FROM leases WHERE tenant_id = $1 AND status = 'active'",
                tenant_id,
            )
            return int(val or 0)

    async def register_engine_heartbeat(self, engine_id: str) -> None:
        now = time.time()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO engine_instances (engine_id, last_heartbeat) VALUES ($1, $2)
                ON CONFLICT (engine_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat
                """,
                engine_id,
                now,
            )

    async def get_dead_engines(self, max_idle_sec: float = 60.0) -> List[str]:
        cutoff = time.time() - max_idle_sec
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT engine_id FROM engine_instances WHERE last_heartbeat < $1",
                cutoff,
            )
            return [r["engine_id"] for r in rows]

    async def delete_dead_engine(self, engine_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM engine_instances WHERE engine_id = $1", engine_id)

    async def prune_database_records(self, retention_seconds: float = 86400.0 * 7) -> None:
        cutoff = time.time() - retention_seconds
        stale_engine_cutoff = time.time() - 86400.0
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM leases WHERE status IN ('expired', 'released') AND expires_at < $1", cutoff)
            await conn.execute("DELETE FROM session_audit_logs WHERE created_at < $1", cutoff)
            await conn.execute("DELETE FROM engine_instances WHERE last_heartbeat < $1", stale_engine_cutoff)

    async def try_acquire_sweeper_leader(self, engine_id: str, ttl_seconds: float = 15.0) -> bool:
        now = time.time()
        new_expires = now + ttl_seconds
        async with self._pool.acquire() as conn:
            row = await conn.fetchval(
                """
                INSERT INTO sweeper_leader (lock_id, leader_id, expires_at)
                VALUES (1, $1, $2)
                ON CONFLICT (lock_id) DO UPDATE
                SET leader_id = $1, expires_at = $2
                WHERE sweeper_leader.leader_id = $1 OR sweeper_leader.expires_at < $3
                RETURNING leader_id;
                """,
                engine_id, new_expires, now,
            )
            return bool(row == engine_id)


class LeaseManager:
    """
    Manages session leases and workspace concurrency locks across engines.
    Uses PostgreSQL as the primary persistent data source for all persistent state.
    """

    def __init__(self, backend: Optional[LeaseBackend] = None):
        self._backend = backend

    @property
    def backend(self) -> Optional[LeaseBackend]:
        return self._backend

    async def initialize(self):
        if self._backend is not None:
            return

        # 1. Primary Persistent Data Source: PostgreSQL
        try:
            pg_url = settings.get_effective_postgres_url()
        except ValueError as e:
            logger.error(f"PostgreSQL configuration error: {e}")
            raise

        if pg_url:
            try:
                backend = PostgresLeaseBackend(pg_url)
                await backend.initialize()
                masked_url = pg_url.split("@")[-1] if "@" in pg_url else pg_url
                logger.info(f"Connected to PostgreSQL persistent ledger at {masked_url}")
                self._backend = backend
                return
            except Exception as e:
                if settings.environment.lower() in ("production", "prod"):
                    raise RuntimeError(
                        f"Failed to connect to production PostgreSQL at {pg_url.split('@')[-1] if '@' in pg_url else 'configured host'} ({e}). "
                        "Refusing to start engine without primary persistent data source."
                    )
                logger.warning(
                    f"Local PostgreSQL unavailable ({e}). "
                    f"Falling back to SQLite ledger at {settings.sqlite_path} for offline development/testing."
                )

        # 2. Secondary Redis (if explicitly configured)
        if settings.redis_url:
            try:
                import redis.asyncio as aioredis
                client = aioredis.from_url(settings.redis_url, socket_connect_timeout=1.0)
                await client.ping()
                logger.info(f"Connected to Redis for session leases at {settings.redis_url}")
                self._backend = RedisLeaseBackend(client)
                return
            except Exception as e:
                if settings.require_redis:
                    raise RuntimeError(
                        f"Failed to connect to Redis at {settings.redis_url} ({e}) and require_redis is True. "
                        "Refusing to fall back to SQLite to prevent split-brain across multiple engine replicas."
                    )
                logger.warning(f"Redis unavailable ({e}); falling back to SQLite ledger at {settings.sqlite_path}")

        # 3. Development Fallback: SQLite
        self._backend = SQLiteLeaseBackend(settings.sqlite_path)

    async def ping(self) -> bool:
        if self._backend and hasattr(self._backend, "ping"):
            return await self._backend.ping()
        return True

    async def close(self):
        if self._backend and hasattr(self._backend, "close"):
            await self._backend.close()

    async def create_or_renew(
        self,
        session_id: str,
        workspace_id: str,
        sandbox_id: str,
        endpoint: str,
        ttl_seconds: Optional[int] = None,
        auth_token: Optional[str] = None,
        tenant_id: str = "default",
    ) -> LeaseRecord:
        from .ttl import clamp_new_session_ttl

        ttl = clamp_new_session_ttl(ttl_seconds)
        return await self._backend.create_or_renew_lease(
            session_id, workspace_id, sandbox_id, endpoint, ttl, auth_token=auth_token, tenant_id=tenant_id
        )

    async def heartbeat(self, session_id: str, ttl_seconds: Optional[int] = None) -> Optional[LeaseRecord]:
        from .ttl import clamp_session_ttl

        record = await self.get(session_id)
        if not record:
            return None
        ttl = clamp_session_ttl(record, ttl_seconds)
        return await self._backend.heartbeat(session_id, ttl)

    async def get(self, session_id: str) -> Optional[LeaseRecord]:
        return await self._backend.get_lease(session_id)

    async def release(self, session_id: str) -> bool:
        return await self._backend.release_lease(session_id)

    async def get_expired(self) -> List[LeaseRecord]:
        return await self._backend.get_expired_leases()

    async def claim_expired(self, cutoff_time: Optional[float] = None) -> List[LeaseRecord]:
        now = cutoff_time if cutoff_time is not None else time.time()
        if hasattr(self._backend, "claim_expired_leases"):
            return await self._backend.claim_expired_leases(now)
        return await self._backend.get_expired_leases()

    async def log_audit_event(
        self,
        session_id: str,
        event_type: str = "custom",
        details: Optional[dict] = None,
        workspace_id: str = "",
    ):
        if hasattr(self._backend, "log_audit_event"):
            await self._backend.log_audit_event(
                session_id=session_id,
                event_type=event_type,
                details=details,
                workspace_id=workspace_id,
            )

    async def mark_expired(self, session_id: str, from_status: Optional[str] = None) -> bool:
        return await self._backend.mark_expired(session_id, from_status=from_status)

    async def list_active(self) -> List[LeaseRecord]:
        return await self._backend.list_active_leases()

    # Workspace Lock Operations
    async def acquire_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: Optional[int] = None, tenant_id: str = "default"
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        ttl = ttl_seconds if ttl_seconds is not None else settings.default_session_ttl
        return await self._backend.acquire_workspace_lock(workspace_id, session_id, ttl, tenant_id=tenant_id)

    async def release_workspace_lock(
        self, workspace_id: str, session_id: str, tenant_id: str = "default"
    ) -> bool:
        return await self._backend.release_workspace_lock(workspace_id, session_id, tenant_id=tenant_id)

    async def refresh_workspace_lock(
        self, workspace_id: str, session_id: str, ttl_seconds: Optional[int] = None, tenant_id: str = "default"
    ) -> bool:
        ttl = ttl_seconds if ttl_seconds is not None else settings.default_session_ttl
        return await self._backend.refresh_workspace_lock(workspace_id, session_id, ttl, tenant_id=tenant_id)

    async def get_workspace_lock(
        self, workspace_id: str, tenant_id: str = "default"
    ) -> Optional[Tuple[str, float]]:
        return await self._backend.get_workspace_lock(workspace_id, tenant_id=tenant_id)

    async def cleanup_expired_workspace_locks(self) -> int:
        return await self._backend.cleanup_expired_workspace_locks()

    async def count_active_locks(self) -> int:
        return await self._backend.count_active_locks()

    async def claim_workspace_ownership(self, workspace_id: str, tenant_id: str = "default") -> Tuple[bool, str]:
        if hasattr(self._backend, "claim_workspace_ownership"):
            return await self._backend.claim_workspace_ownership(workspace_id, tenant_id=tenant_id)
        return (True, tenant_id)

    async def count_tenant_workspaces(self, tenant_id: str) -> int:
        if hasattr(self._backend, "count_tenant_workspaces"):
            return await self._backend.count_tenant_workspaces(tenant_id)
        return 0

    async def tenant_has_workspace(self, tenant_id: str, workspace_id: str) -> bool:
        if hasattr(self._backend, "tenant_has_workspace"):
            return await self._backend.tenant_has_workspace(tenant_id, workspace_id)
        return False

    async def reserve_session_slot(
        self, tenant_id: str, max_concurrent: int, session_id: Optional[str] = None, workspace_id: Optional[str] = None
    ) -> bool:
        if hasattr(self._backend, "reserve_session_slot"):
            return await self._backend.reserve_session_slot(
                tenant_id, max_concurrent, session_id=session_id, workspace_id=workspace_id
            )
        return True

    async def count_active_sessions_for_tenant(self, tenant_id: str) -> int:
        if hasattr(self._backend, "count_active_sessions_for_tenant"):
            return await self._backend.count_active_sessions_for_tenant(tenant_id)
        return 0

    async def register_engine_heartbeat(self, engine_id: str) -> None:
        if hasattr(self._backend, "register_engine_heartbeat"):
            await self._backend.register_engine_heartbeat(engine_id)

    async def get_dead_engines(self, max_idle_sec: float = 60.0) -> List[str]:
        if hasattr(self._backend, "get_dead_engines"):
            return await self._backend.get_dead_engines(max_idle_sec)
        return []

    async def delete_dead_engine(self, engine_id: str) -> None:
        if hasattr(self._backend, "delete_dead_engine"):
            await self._backend.delete_dead_engine(engine_id)

    async def prune_database_records(self, retention_seconds: float = 86400.0 * 7) -> None:
        if hasattr(self._backend, "prune_database_records"):
            await self._backend.prune_database_records(retention_seconds)

    async def try_acquire_sweeper_leader(self, engine_id: str, ttl_seconds: float = 15.0) -> bool:
        if hasattr(self._backend, "try_acquire_sweeper_leader"):
            return await self._backend.try_acquire_sweeper_leader(engine_id, ttl_seconds=ttl_seconds)
        return True
