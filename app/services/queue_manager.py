"""
QueueManager — thread-safe media group coalescing.

Design: Version-counter approach to replace lock+cancel (which causes
deadlocks under rapid album delivery):

  * Each group gets a monotonically increasing version number.
  * The flush task captures its version at creation time.
  * When the task wakes up, it checks if its version is still current.
  * If a newer message arrived, the version has advanced and the task
    exits silently — no cancellation, no lock contention.
  * Only the task whose version matches the current one flushes.
  * A per-group asyncio.Lock serialises the final pop+insert, but is
    never held across an await that can be cancelled.

FIX HR-2: Added shutdown() method that forcibly flushes all in-progress
groups so that albums buffered at shutdown time are not silently lost.
"""

import asyncio
from typing import Dict, List, Optional

from pyrogram.types import Message
from loguru import logger

from app.database.models import QueueItem
from app.database.repositories import queue_repo

_MEDIA_GROUP_FLUSH_DELAY: int = 3  # seconds


class _GroupState:
    __slots__ = ("messages", "version", "lock")

    def __init__(self) -> None:
        self.messages: List[Message] = []
        self.version: int = 0
        self.lock: asyncio.Lock = asyncio.Lock()


class QueueManager:
    def __init__(self) -> None:
        self._groups: Dict[str, _GroupState] = {}
        # Track spawned flush tasks so shutdown() can await them
        self._flush_tasks: Dict[str, asyncio.Task] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def add_message_to_queue(self, message: Message) -> None:
        if message.media_group_id:
            await self._handle_media_group(message)
        else:
            await self._handle_single(message)

    async def shutdown(self) -> None:
        """
        Immediately flush all buffered media groups.

        Called during graceful shutdown so that albums that arrived within
        the last MEDIA_GROUP_FLUSH_DELAY seconds are not lost.
        """
        if not self._groups:
            logger.debug("QueueManager shutdown: no pending groups.")
            return

        logger.info(
            f"QueueManager shutdown: force-flushing {len(self._groups)} "
            f"pending album group(s)…"
        )

        # Snapshot group IDs; new messages won't arrive after shutdown starts
        gids = list(self._groups.keys())
        for gid in gids:
            if gid not in self._groups:
                continue
            state = self._groups[gid]
            async with state.lock:
                if gid not in self._groups:
                    continue  # already flushed by a concurrent task
                messages = list(state.messages)
                del self._groups[gid]

            if not messages:
                continue

            messages.sort(key=lambda m: m.id)
            message_ids = [m.id for m in messages]
            first_id = message_ids[0]
            item = QueueItem(
                message_id=first_id,
                media_group_id=gid,
                message_ids=message_ids,
            )
            result = await queue_repo.add_to_queue(item)
            if result is None:
                logger.debug(
                    f"Shutdown flush: group {gid} already in queue (skipped)."
                )
            else:
                logger.info(
                    f"Shutdown flush: queued group {gid} "
                    f"({len(message_ids)} messages)."
                )

        # Cancel any remaining sleep tasks (they would flush groups that no
        # longer exist in self._groups, so they exit safely on wakeup)
        for task in list(self._flush_tasks.values()):
            if not task.done():
                task.cancel()
        self._flush_tasks.clear()

        logger.info("QueueManager shutdown complete.")

    # ------------------------------------------------------------------
    # Single message
    # ------------------------------------------------------------------

    async def _handle_single(self, message: Message) -> None:
        logger.info(f"Queueing single message: {message.id}")
        item = QueueItem(message_id=message.id)
        await queue_repo.add_to_queue(item)

    # ------------------------------------------------------------------
    # Media group
    # ------------------------------------------------------------------

    async def _handle_media_group(self, message: Message) -> None:
        gid = message.media_group_id

        if gid not in self._groups:
            self._groups[gid] = _GroupState()

        state = self._groups[gid]

        async with state.lock:
            state.messages.append(message)
            state.version += 1
            my_version = state.version
            logger.debug(
                f"Buffered msg {message.id} → group {gid} "
                f"(version={my_version}, total={len(state.messages)})"
            )

        task = asyncio.create_task(
            self._flush_group(gid, my_version),
            name=f"flush_{gid}_v{my_version}",
        )
        self._flush_tasks[f"{gid}_v{my_version}"] = task
        task.add_done_callback(
            lambda t: self._flush_tasks.pop(f"{gid}_v{my_version}", None)
        )

    async def _flush_group(self, gid: str, my_version: int) -> None:
        try:
            await asyncio.sleep(_MEDIA_GROUP_FLUSH_DELAY)
        except asyncio.CancelledError:
            # Cancelled during shutdown — the shutdown() method will handle
            # any remaining groups directly.
            return

        if gid not in self._groups:
            return  # already flushed and cleaned up

        state = self._groups[gid]

        async with state.lock:
            if state.version != my_version:
                logger.debug(
                    f"Flush v{my_version} for group {gid} superseded by "
                    f"v{state.version}. Exiting."
                )
                return

            messages = list(state.messages)
            del self._groups[gid]

        if not messages:
            logger.warning(f"Flush v{my_version} for group {gid}: buffer empty.")
            return

        messages.sort(key=lambda m: m.id)
        message_ids = [m.id for m in messages]
        first_id = message_ids[0]

        logger.info(
            f"Flushing media group {gid}: {len(message_ids)} messages, "
            f"IDs {message_ids[0]}–{message_ids[-1]}"
        )

        item = QueueItem(
            message_id=first_id,
            media_group_id=gid,
            message_ids=message_ids,
        )
        result = await queue_repo.add_to_queue(item)
        if result is None:
            logger.debug(f"Media group {gid} already in queue (deduplicated).")


queue_manager = QueueManager()
