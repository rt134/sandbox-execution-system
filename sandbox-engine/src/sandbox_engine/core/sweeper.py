from __future__ import annotations

import asyncio
import logging
from typing import Optional

from ..config import settings
from ..drivers.base import ContainerDriver
from .lease_manager import LeaseManager

logger = logging.getLogger("sandbox.sweeper")


class Sweeper:
    """
    Background garbage collection task.
    Reconciles expired leases and leaked containers every N seconds.
    """

    def __init__(
        self,
        driver: ContainerDriver,
        lease_manager: LeaseManager,
        interval_sec: Optional[float] = None,
    ):
        self.driver = driver
        self.lease_manager = lease_manager
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
        # 1. Clean up expired leases
        expired_leases = await self.lease_manager.get_expired()
        for lease in expired_leases:
            logger.info(f"🧹 [Sweeper] Lease expired for session '{lease.session_id}'. Reclaiming container '{lease.sandbox_id}'...")
            try:
                if lease.sandbox_id:
                    await self.driver.destroy_sandbox(lease.sandbox_id)
            except Exception as e:
                logger.error(f"Failed to destroy sandbox {lease.sandbox_id}: {e}")
            finally:
                await self.lease_manager.mark_expired(lease.session_id)

        # 2. Orphan container garbage collection
        try:
            active_leases = await self.lease_manager.list_active()
            active_sandbox_ids = {l.sandbox_id for l in active_leases if l.sandbox_id}

            # Query all stateful sandboxes managed by driver
            running_sandboxes = await self.driver.list_sandboxes(labels={"sandbox.type": "stateful"})
            for sbx in running_sandboxes:
                if sbx.sandbox_id not in active_sandbox_ids:
                    logger.warning(f"🧹 [Sweeper] Found orphan stateful container '{sbx.sandbox_id}'. Destroying...")
                    await self.driver.destroy_sandbox(sbx.sandbox_id)
        except Exception as e:
            logger.error(f"Error checking orphan sandboxes: {e}")
