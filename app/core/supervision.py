import asyncio
import tracemalloc
from datetime import datetime, timezone
from typing import Optional
from app.core.logger import get_logger

logger = get_logger(__name__)


class SystemSupervisor:
    """
    Low-overhead runtime supervisor.
    Tracks memory allocations, event loop lag, and orphaned async tasks.
    Provides a continuous heartbeat for observability.
    """

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._startup_time = datetime.now(timezone.utc)

    async def start(self) -> None:
        if self._running:
            return
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="system-supervisor")
        logger.info("System supervisor started")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        logger.info("System supervisor stopped cleanly")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(60.0)
                await self._emit_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("System supervisor error", exc_info=e)
                await asyncio.sleep(5.0)

    async def _emit_heartbeat(self) -> None:
        # 1. Measure Event Loop Lag
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        await asyncio.sleep(0)
        lag_ms = (loop.time() - t0) * 1000.0

        # 2. Track Async Tasks (Orphan / Leak detection)
        tasks = asyncio.all_tasks(loop)
        task_count = len(tasks)
        task_names = {}
        for t in tasks:
            name = t.get_name()
            task_names[name] = task_names.get(name, 0) + 1

        # 3. Memory Leak Detection Hook
        memory_kb = 0.0
        if tracemalloc.is_tracing():
            current, peak = tracemalloc.get_traced_memory()
            memory_kb = current / 1024.0

        uptime_sec = (datetime.now(timezone.utc) - self._startup_time).total_seconds()

        logger.info(
            "System Heartbeat",
            extra={
                "ctx_uptime_seconds": round(uptime_sec, 2),
                "ctx_event_loop_lag_ms": round(lag_ms, 2),
                "ctx_async_task_count": task_count,
                "ctx_memory_usage_kb": round(memory_kb, 2),
                "ctx_tasks_by_name": task_names,
            }
        )