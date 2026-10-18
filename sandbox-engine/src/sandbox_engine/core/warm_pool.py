from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Optional, Set
import httpx

from ..config import settings
from ..core.security import generate_daemon_token
from ..drivers.base import ContainerDriver
from ..models import SandboxInstance, SandboxSpec
from .metrics import WARM_POOL_ACQUISITIONS_TOTAL, WARM_POOL_AVAILABLE

logger = logging.getLogger("sandbox.warm_pool")


class WarmPoolManager:
    """
    Manages a pool of pre-warmed, idle sandboxes for sub-50ms execution.
    Implements a 'replenish-on-consume' pattern with bounded concurrency semaphore.
    """

    def __init__(self, driver: ContainerDriver, target_size: Optional[int] = None):
        self.driver = driver
        self.target_size = target_size if target_size is not None else settings.warm_pool_size
        self.queue: asyncio.Queue[SandboxInstance] = asyncio.Queue()
        self._active_spawns: int = 0
        self._running: bool = False
        self._all_pool_ids: Set[str] = set()
        # Cap concurrent Docker/K8s launches to avoid socket / API server starvation
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_spawns)

    def get_active_ids(self) -> Set[str]:
        """Returns set of all active sandbox IDs tracked by this warm pool."""
        return set(self._all_pool_ids)

    async def start(self):
        """Initialize the warm pool up to target size."""
        self._running = True
        logger.info(f"Starting Warm Pool with target size {self.target_size} using {settings.driver} driver...")
        spawn_tasks = [self._spawn_warm_instance() for _ in range(self.target_size)]
        await asyncio.gather(*spawn_tasks, return_exceptions=True)
        WARM_POOL_AVAILABLE.set(self.queue.qsize())
        logger.info(f"Warm Pool ready! Available instances in queue: {self.queue.qsize()}")

    async def acquire(self, timeout: float = 5.0) -> SandboxInstance:
        """
        Pop an idle, pre-warmed sandbox from the queue.
        Takes <1ms when a warm instance is available.
        """
        try:
            instance = await asyncio.wait_for(self.queue.get(), timeout=timeout)
            logger.debug(f"Acquired warm sandbox {instance.sandbox_id}. Queue remaining: {self.queue.qsize()}")
            WARM_POOL_ACQUISITIONS_TOTAL.labels(result="hit").inc()
            WARM_POOL_AVAILABLE.set(self.queue.qsize())
            # Immediately trigger replenish in the background
            self.trigger_replenish()
            return instance
        except asyncio.TimeoutError:
            logger.warning("Warm pool queue empty! Spawning on-demand sandbox...")
            WARM_POOL_ACQUISITIONS_TOTAL.labels(result="miss").inc()
            instance = await self._create_one_instance()
            self.trigger_replenish()
            return instance

    def trigger_replenish(self):
        """Fire-and-forget task to restore pool size."""
        if not self._running:
            return
        needed = self.target_size - (self.queue.qsize() + self._active_spawns)
        for _ in range(max(0, needed)):
            asyncio.create_task(self._spawn_warm_instance())

    def trigger_recycle(self, instance: SandboxInstance):
        """Asynchronously destroy a consumed sandbox in the background."""
        asyncio.create_task(self._recycle_worker(instance))

    async def _recycle_worker(self, instance: SandboxInstance):
        try:
            self._all_pool_ids.discard(instance.sandbox_id)
            await self.driver.destroy_sandbox(instance.sandbox_id)
            logger.debug(f"Recycled consumed sandbox {instance.sandbox_id}")
        except Exception as e:
            logger.error(f"Failed to recycle sandbox {instance.sandbox_id}: {e}")

    async def _spawn_warm_instance(self):
        if not self._running:
            return
        self._active_spawns += 1
        try:
            instance = await self._create_one_instance()
            await self.queue.put(instance)
            WARM_POOL_AVAILABLE.set(self.queue.qsize())
            logger.debug(f"Replenished warm sandbox {instance.sandbox_id}. Queue size now: {self.queue.qsize()}")
        except Exception as e:
            logger.error(f"Error spawning warm sandbox: {e}")
        finally:
            self._active_spawns = max(0, self._active_spawns - 1)

    async def _create_one_instance(self) -> SandboxInstance:
        async with self._semaphore:
            auth_token = generate_daemon_token()
            spec = SandboxSpec(
                spec_id=f"warm-{uuid.uuid4().hex[:8]}",
                sandbox_type="stateless",
                image=settings.image,
                labels={"sandbox.pool": "warm"},
                pids_limit=settings.pids_limit,
                disk_quota_mb=settings.default_workspace_disk_quota_mb,
                mem_limit=settings.default_mem_limit,
                cpu_limit=settings.default_cpu_limit,
                auth_token=auth_token,
            )
            instance = await self.driver.create_sandbox(spec)
            self._all_pool_ids.add(instance.sandbox_id)

            # Verify readiness with auth token
            ready = await self.driver.healthcheck(instance.endpoint, auth_token=auth_token)
            if not ready:
                await self.driver.destroy_sandbox(instance.sandbox_id)
                self._all_pool_ids.discard(instance.sandbox_id)
                raise RuntimeError(f"Sandbox {instance.sandbox_id} failed readiness healthcheck")

            return instance

    async def stop(self):
        """Drain the pool and destroy all remaining idle containers."""
        self._running = False
        logger.info("Stopping warm pool and cleaning up containers...")
        while not self.queue.empty():
            try:
                instance = self.queue.get_nowait()
                await self.driver.destroy_sandbox(instance.sandbox_id)
            except Exception:
                pass
        WARM_POOL_AVAILABLE.set(0)

        remaining_ids = list(self._all_pool_ids)
        for s_id in remaining_ids:
            try:
                await self.driver.destroy_sandbox(s_id)
            except Exception:
                pass
        self._all_pool_ids.clear()
        logger.info("Warm pool stopped.")
