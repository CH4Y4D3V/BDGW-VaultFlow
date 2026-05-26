"""
app/bot/delivery.py

Strict Vault-First delivery pipeline.
ALL delivery MUST use copy_message() or copy_media_group() from vault channel ONLY.
NO fallbacks to file_id or local paths.
"""

from __future__ import annotations

import asyncio
from typing import List

from pyrogram.client import Client
from pyrogram.errors import FloodWait, RPCError

from app.bot.client import get_bot
from app.core.exceptions import (
    FloodWaitError, 
    DispatcherError, 
    PermanentDeliveryError, 
    APIDegradationError,
    VaultReferenceMissingError
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_telegram_delivery(job_docs: List[dict], target_id: str) -> None:
    """
    Delivery pipeline for the DistributionEngine.
    Strictly uses vault copies.
    """
    bot = get_bot()
    if not job_docs:
        return

    # Sort by sequence index to ensure deterministic ordering
    sorted_jobs = sorted(job_docs, key=lambda x: x.get("album_sequence_index") or 0)

    try:
        if len(sorted_jobs) == 1:
            await _send_single(bot, sorted_jobs[0], target_id)
        else:
            await _send_album(bot, sorted_jobs, target_id)
            
    except FloodWait as e:
        raise FloodWaitError(seconds=int(e.value))
    except RPCError as e:
        logger.error("Telegram RPC error during delivery", extra={"ctx_target": target_id, "ctx_error": str(e)})
        raise DispatcherError(f"RPC error: {e}")
    except Exception as e:
        if isinstance(e, (PermanentDeliveryError, VaultReferenceMissingError)):
            raise
        logger.error("Unexpected delivery failure", extra={"ctx_target": target_id, "ctx_error": str(e)}, exc_info=True)
        raise DispatcherError(f"Unexpected error: {e}")


async def _send_single(bot: Client, job: dict, target_id: str) -> None:
    """Send a single message using copy_message from vault."""
    vault_chat_id = job.get("vault_chat_id")
    vault_message_id = job.get("vault_message_id")
    
    if not vault_chat_id or not vault_message_id:
        raise VaultReferenceMissingError(f"Job {job.get('_id')} missing vault references")

    await bot.copy_message(
        chat_id=target_id,
        from_chat_id=vault_chat_id,
        message_id=vault_message_id,
        caption=job.get("caption")
    )


async def _send_album(bot: Client, sorted_jobs: List[dict], target_id: str) -> None:
    """
    Send an album using copy_media_group from vault.
    Falls back to sequential copy_message on failure.
    """
    primary_job = sorted_jobs[0]
    vault_chat_id = primary_job.get("vault_chat_id")
    first_vault_id = primary_job.get("vault_message_id")

    if not vault_chat_id or not first_vault_id:
        raise VaultReferenceMissingError("Album primary job missing vault references")

    # 1. Primary: copy_media_group
    try:
        await bot.copy_media_group(
            chat_id=target_id,
            from_chat_id=vault_chat_id,
            message_id=first_vault_id
        )
        return
    except (RPCError, Exception) as e:
        logger.warning(
            "copy_media_group failed, attempting sequential fallback",
            extra={"ctx_target": target_id, "ctx_error": str(e)}
        )
        
    # 2. Fallback: Sequential copy_message
    # This preserves album grouping semantics and ordering while avoiding InputMedia reconstruction.
    batch_id = f"fallback_{primary_job.get('_id')}_{target_id}"
    
    for i, job in enumerate(sorted_jobs):
        v_chat_id = job.get("vault_chat_id")
        v_msg_id = job.get("vault_message_id")
        
        if not v_chat_id or not v_msg_id:
            raise VaultReferenceMissingError(f"Album item {i} missing vault references")

        await bot.copy_message(
            chat_id=target_id,
            from_chat_id=v_chat_id,
            message_id=v_msg_id,
            caption=job.get("caption") if i == 0 else None
        )
        
    logger.info("Album delivered via sequential fallback", extra={"ctx_target": target_id, "ctx_batch_id": batch_id})