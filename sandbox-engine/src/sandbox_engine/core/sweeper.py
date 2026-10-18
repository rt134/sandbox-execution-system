from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, TYPE_CHECKING

from ..config import settings
from ..drivers.base import ContainerDriver
from .lease_manager import LeaseManager
from .metrics import ACTIVE_SESSIONS, SWEEPER_RECLAIMS_TOTAL, WORKSPACE_LOCKS_ACTIVE

if TYPE_CHECKING:
    from .warm_pool import WarmPoolManager

logger = logging.getLogger("sandbox.sweeper")


class Sweeper:
    """
    Background garbage collection task.
    Reconciles expired leases, cleans up workspace locks, and reclaims leaked containers
    with an orphan grace period to prevent race conditions during container creation.
    """

    def __init__(
        self,
        driver: ContainerDriver,
        lease_manager: LeaseManager,
        warm_pool: Optional[WarmPoolManager] = None,
        interval_sec: Optional[float] = None,
    ):
        self.driver = driver
        self.lease_manager = lease_manager
        self.warm_pool = warm_pool
        self.interval_sec = interval_sec or settings.sweeper_interval_sec
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._sweeper_loop())
            logger.info(f"Sweeper started (interval: {self.interval_sec}s)")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            logger.info("Sweeper stopped.")

    async def _sweeper_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.interval_sec)
                await self.sweep_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in sweeper loop: {e}", exc_info=True)

    async def sweep_once(self):
        """Perform a single reconciliation sweep."""
        now = time.time()
        grace_period = settings.orphan_grace_period_sec

        # 1. Clean up expired leases (atomic claim to avoid killing containers that just renewed)
        expired_leases = await self.lease_manager.claim_expired(cutoff_time=now)
        for lease in expired_leases:
            logger.info(f"🧹 [Sweeper] Lease expired for session '{lease.session_id}'. Reclaiming container '{lease.sandbox_id}'...")
            try:
                if lease.sandbox_id:
                    await self.driver.destroy_sandbox(lease.sandbox_id)
                SWEEPER_RECLAIMS_TOTAL.labels(reason="expired_lease").inc()
            except Exception as e:
                logger.error(f"Failed to destroy sandbox {lease.sandbox_id}: {e}")
            finally:
                if lease.workspace_id:
                    await self.lease_manager.release_workspace_lock(lease.workspace_id, lease.session_id)
                await self.lease_manager.mark_expired(lease.session_id)

        # 2. Reconcile any orphaned workspace locks
        await self.lease_manager.cleanup_expired_workspace_locks()

        # 3. Orphan stateful container garbage collection
        try:
            active_leases = await self.lease_manager.list_active()
            active_sandbox_ids = {l.sandbox_id for l in active_leases if l.sandbox_id}

            running_sandboxes = await self.driver.list_sandboxes(labels={"sandbox.type": "stateful"})
            for sbx in running_sandboxes:
                if sbx.sandbox_id not in active_sandbox_ids:
                    # Prevent race condition: do not reap containers in their creation/handshake window
                    age = now - sbx.created_at
                    if age < grace_period:
                        continue

                    logger.warning(f"🧹 [Sweeper] Found orphan stateful container '{sbx.sandbox_id}' (age: {age:.1f}s). Destroying...")
                    try:
                        await self.driver.destroy_sandbox(sbx.sandbox_id)
                        SWEEPER_RECLAIMS_TOTAL.labels(reason="orphan_container").inc()
                    except Exception as e:
                        logger.error(f"Failed to destroy orphan container {sbx.sandbox_id}: {e}")

            # 4. Orphan warm pool container garbage collection (reclaimed if this engine replica crashed)
            if self.warm_pool is not None:
                tracked_pool_ids = self.warm_pool.get_active_ids()
                # Multi-replica safe: only inspect warm containers created by this replica's engine_id!
                running_warm = await self.driver.list_sandboxes(
                    labels={"sandbox.pool": "warm", "sandbox.engine_id": self.warm_pool.engine_id}
                )
                for sbx in running_warm:
                    if sbx.sandbox_id not in tracked_pool_ids:
                        age = now - sbx.created_at
                        if age < grace_period:
                            continue
                        logger.warning(f"🧹 [Sweeper] Found orphan warm container '{sbx.sandbox_id}' for engine '{self.warm_pool.engine_id}' (age: {age:.1f}s). Destroying...")
                        try:
                            await self.driver.destroy_sandbox(sbx.sandbox_id)
                            SWEEPER_RECLAIMS_TOTAL.labels(reason="orphan_warm_container").inc()
                        except Exception as e:
                            logger.error(f"Failed to destroy orphan warm sandbox {sbx.sandbox_id}: {e}")

            # Update Prometheus gauges
            ACTIVE_SESSIONS.set(len(active_leases))
            active_locks = await self.lease_manager.count_active_locks()
            WORKSPACE_LOCKS_ACTIVE.set(active_locks)

        except Exception as e:
            logger.error(f"Error checking orphan sandboxes: {e}")
