"""
app/bot/delivery.py

Vault-First delivery pipeline.
Resolves vault references from multiple schema versions for backwards compatibility.
"""

from __future__ import annotations

import asyncio
from typing import List, Tuple

from pyrogram.client import Client
from pyrogram.errors import FloodWait, RPCError

from app.bot.client import get_bot
from app.config.settings import settings
from app.core.exceptions import (
    FloodWaitError,
    DispatcherError,
    PermanentDeliveryError,
    VaultReferenceMissingError,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


# Maps source_channel_id string labels → actual Telegram channel IDs
# Add any other labels your job creation code uses here
_CHANNEL_LABEL_MAP: dict[str, str] = {
    "submission_premium": str(settings.PREMIUM_CHANNEL_ID),
    "submission_main":    str(settings.VAULT_CHANNEL_ID),
    "submission_nsfw":    str(settings.NSFW_GROUP_ID),
}


def _resolve_vault_ref(job: dict) -> Tuple[str, int]:
    """
    Resolve vault (source chat_id, message_id) from a job document.

    Handles three schema versions:
      1. New schema:    vault_chat_id + vault_message_id  (direct fields)
      2. Legacy schema: source_channel_id label + metadata.message_id
      3. Encoded:       content_id = '{chat_id}_{msg_id}_{type}'
    """
    job_id = str(job.get("_id", "unknown"))

    # --- Schema v2: direct vault fields (new jobs) ---
    vault_chat_id = job.get("vault_chat_id")
    vault_message_id = job.get("vault_message_id")
    if vault_chat_id and vault_message_id:
        return str(vault_chat_id), int(vault_message_id)

    # --- Schema v1: label + metadata.message_id (existing jobs) ---
    source_label = job.get("source_channel_id")
    meta_msg_id = (job.get("metadata") or {}).get("message_id")

    if source_label and meta_msg_id:
        resolved_channel = _CHANNEL_LABEL_MAP.get(source_label)
        if resolved_channel:
            return resolved_channel, int(meta_msg_id)
        # Label unknown — log and fall through to content_id parse
        logger.warning(
            "Unknown source_channel_id label, attempting content_id parse",
            extra={"ctx_job_id": job_id, "ctx_label": source_label},
        )

    # --- Schema v0: parse content_id = '{chat_id}_{msg_id}_{type}' ---
    content_id = job.get("content_id", "")
    if content_id:
        # content_id example: '-1003958888080_10_premium'
        # Split on first two underscores only — chat_id may be negative
        parts = content_id.split("_", 2)
        if len(parts) >= 2:
            try:
                parsed_chat_id = parts[0]
                parsed_msg_id = int(parts[1])
                if parsed_chat_id.lstrip("-").isdigit():
                    logger.info(
                        "Resolved vault ref from content_id",
                        extra={
                            "ctx_job_id": job_id,
                            "ctx_chat_id": parsed_chat_id,
                            "ctx_msg_id": parsed_msg_id,
                        },
                    )
                    return parsed_chat_id, parsed_msg_id
            except (ValueError, IndexError):
                pass

    raise VaultReferenceMissingError(
        f"Job {job_id} has no resolvable vault reference. "
        f"Fields present: source_channel_id={job.get('source_channel_id')!r}, "
        f"metadata.message_id={meta_msg_id!r}, content_id={content_id!r}"
    )


async def execute_telegram_delivery(job_docs: List[dict], target_id: str) -> None:
    """
    Delivery entry point for the DistributionEngine.
    Strictly vault-first: all sends use copy_message / copy_media_group.
    """
    bot = get_bot()
    if not job_docs:
        return

    sorted_jobs = sorted(
        job_docs, key=lambda x: x.get("album_sequence_index") or 0
    )

    try:
        if len(sorted_jobs) == 1:
            await _send_single(bot, sorted_jobs[0], target_id)
        else:
            await _send_album(bot, sorted_jobs, target_id)

    except FloodWait as e:
        raise FloodWaitError(seconds=int(e.value))
    except RPCError as e:
        logger.error(
            "Telegram RPC error during delivery",
            extra={"ctx_target": target_id, "ctx_error": str(e)},
        )
        raise DispatcherError(f"RPC error: {e}")
    except (PermanentDeliveryError, VaultReferenceMissingError):
        raise
    except Exception as e:
        logger.error(
            "Unexpected delivery failure",
            extra={"ctx_target": target_id, "ctx_error": str(e)},
            exc_info=True,
        )
        raise DispatcherError(f"Unexpected error: {e}")


async def _send_single(bot: Client, job: dict, target_id: str) -> None:
    """Send a single message via copy_message from vault."""
    from_chat_id, message_id = _resolve_vault_ref(job)

    await bot.copy_message(
        chat_id=target_id,
        from_chat_id=from_chat_id,
        message_id=message_id,
        caption=job.get("caption") or None,
    )
    logger.info(
        "Single message delivered",
        extra={
            "ctx_target": target_id,
            "ctx_from_chat": from_chat_id,
            "ctx_msg_id": message_id,
        },
    )


async def _send_album(bot: Client, sorted_jobs: List[dict], target_id: str) -> None:
    """
    Send an album via copy_media_group, with sequential copy_message fallback.
    All items must resolve vault references.
    """
    primary_job = sorted_jobs[0]
    from_chat_id, first_msg_id = _resolve_vault_ref(primary_job)

    # Primary: copy_media_group (single API call, preserves grouping)
    try:
        await bot.copy_media_group(
            chat_id=target_id,
            from_chat_id=from_chat_id,
            message_id=first_msg_id,
        )
        logger.info(
            "Album delivered via copy_media_group",
            extra={"ctx_target": target_id, "ctx_from_chat": from_chat_id},
        )
        return
    except (RPCError, Exception) as e:
        logger.warning(
            "copy_media_group failed, falling back to sequential copy_message",
            extra={"ctx_target": target_id, "ctx_error": str(e)},
        )

    # Fallback: sequential copy_message per item
    for i, job in enumerate(sorted_jobs):
        item_chat_id, item_msg_id = _resolve_vault_ref(job)
        await bot.copy_message(
            chat_id=target_id,
            from_chat_id=item_chat_id,
            message_id=item_msg_id,
            caption=job.get("caption") if i == 0 else None,
        )

    logger.info(
        "Album delivered via sequential fallback",
        extra={"ctx_target": target_id, "ctx_count": len(sorted_jobs)},
    )