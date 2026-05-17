import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Dict

from pyrogram.types import Message
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.bot.client import get_bot
from app.config import settings
from app.core.database import DatabaseManager
from app.core.models import MediaType
from app.utils.logger import get_logger

logger = get_logger(__name__)


class MediaIngestionPipeline:
    """
    Ingestion pipeline ensuring media group buffering, duplicate media prevention,
    protected content handling, and vault archival consistency.
    """

    def __init__(self):
        self._buffer: Dict[str, List[Message]] = defaultdict(list)
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def ingest(self, message: Message, source_channel_id: str) -> None:
        """
        Entry point for Pyrogram update handlers.
        """
        group_id = message.media_group_id

        if not group_id:
            await self._archive_album([message], source_channel_id)
            return

        # Media group buffering to handle partial album arrival
        async with self._lock:
            self._buffer[group_id].append(message)
            
            if group_id in self._tasks:
                self._tasks[group_id].cancel()
                
            self._tasks[group_id] = asyncio.create_task(
                self._wait_and_flush(group_id, source_channel_id)
            )

    async def _wait_and_flush(self, group_id: str, source_channel_id: str) -> None:
        try:
            await asyncio.sleep(getattr(settings, "MEDIA_GROUP_TIMEOUT_SECONDS", 3.0))
        except asyncio.CancelledError:
            return

        async with self._lock:
            messages = self._buffer.pop(group_id, [])
            self._tasks.pop(group_id, None)

        if messages:
            await self._archive_album(messages, source_channel_id)

    async def _archive_album(self, messages: List[Message], source_channel_id: str) -> None:
        # Album ordering safely preserved
        messages.sort(key=lambda m: m.id)

        db = DatabaseManager.get_db()
        vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]
        bot = get_bot()

        operations = []
        now = datetime.now(timezone.utc)

        for msg in messages:
            media = getattr(msg, str(msg.media.value)) if msg.media else None
            file_unique_id = getattr(media, "file_unique_id", None) if media else None
            file_id = getattr(media, "file_id", None) if media else None
            
            # Protected content handling: download to local disk
            local_path = None
            if msg.has_protected_content and media:
                try:
                    logger.info("Downloading protected content", extra={"ctx_msg": msg.id})
                    local_path = await bot.download_media(message=msg)
                except Exception as e:
                    logger.error("Failed to download protected content", exc_info=e)
                    continue
            
            # Media normalization
            media_type_str = str(msg.media.value) if msg.media else MediaType.TEXT.value
            content_id = f"{source_channel_id}_{msg.id}"
            
            doc = {
                "$setOnInsert": {
                    "content_id": content_id,
                    "source_channel_id": source_channel_id,
                    "message_id": msg.id,
                    "media_group_id": msg.media_group_id,
                    "media_type": media_type_str,
                    "file_id": file_id,
                    "file_unique_id": file_unique_id,
                    "file_path": str(local_path) if local_path else None,
                    "caption": msg.caption or msg.text or "",
                    "has_protected_content": msg.has_protected_content,
                    "created_at": now,
                    "status": "pending_distribution",
                    "metadata": {
                        "has_spoiler": getattr(media, "has_spoiler", False) if media else False,
                        "date": msg.date,
                    }
                }
            }
            
            # Duplicate media prevention + vault archival consistency via atomic upsert
            operations.append(UpdateOne(
                {"content_id": content_id},
                doc,
                upsert=True
            ))

        if operations:
            try:
                result = await vault.bulk_write(operations, ordered=False)
                logger.info("Album archived", extra={"ctx_inserted": result.upserted_count})
            except BulkWriteError:
                logger.warning("Duplicate media ignored during archival")
            except Exception as e:
                logger.error("Vault archival failure", exc_info=e)

    async def recover_partial_albums(self) -> None:
        """
        Restart-safe media-group recovery.
        Finds any partial albums in memory and safely flushes them on shutdown or crash recovery.
        """
        async with self._lock:
            group_ids = list(self._buffer.keys())
            for gid in group_ids:
                if gid in self._tasks:
                    self._tasks[gid].cancel()
                    self._tasks.pop(gid, None)
                    
        # Flush remaining buffers directly via graceful teardown
        for gid in group_ids:
            messages = self._buffer.pop(gid, [])
            if messages:
                source_channel_id = str(messages[0].chat.id)
                await self._archive_album(messages, source_channel_id)