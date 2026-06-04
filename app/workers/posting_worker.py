"""
Posting worker — dequeues one item per scheduler tick and sends it.

Vault Replay Mode
-----------------
When the pending queue is empty and all real content has been sent, the
worker enters Vault Replay Mode.  It cycles through every item in the vault
(queue documents with status='sent' or status='failed') in a randomised,
non-repeating order.  Once every vault item has been replayed once, a new
shuffled cycle begins.  This continues indefinitely until new real content
arrives in the source channel.

Preemption:
  Real content (status='pending') is always checked FIRST at the top of
  every tick.  The moment a new message is queued by message_handlers.py,
  the next scheduler tick will pick it up and send it before any replay
  item — no special signal or interrupt needed.

Replay state is persisted in MongoDB (vault_replay_state collection) so
it survives restarts.  The daily limit and FloodWait handling apply
identically to both normal sends and replay sends.

FIX HR-1: Daily counter reset uses configured TIMEZONE, not UTC.
FIX HR-2: Atomic daily limit check-and-increment via try_increment_daily_sent_count.
FIX-PEER: PeerIdInvalid re-queued up to MAX_PEER_RETRIES, then marked failed.
"""

import asyncio
import random
from datetime import datetime
from typing import Optional

import pytz
from bson import ObjectId
from loguru import logger
from pyrogram.errors import FloodWait, PeerIdInvalid

from app.database.models import QueueItem, SentLog
from app.database.repositories import (
    queue_repo,
    sent_log_repo,
    state_repo,
    vault_replay_repo,
)
from app.services.telegram_sender import TelegramSender
from app.utils.config import settings

# Maximum seconds to sleep on FloodWait before re-queuing.
_MAX_FLOODWAIT_SLEEP = 300  # 5 minutes

# After this many consecutive PeerIdInvalid failures on the SAME item,
# mark it failed to unblock the queue.
_MAX_PEER_RETRIES = 20


def _today_str() -> str:
    """Return today's date string in the configured timezone, e.g. '2024-01-15'."""
    tz = pytz.timezone(settings.TIMEZONE)
    return datetime.now(tz=tz).date().isoformat()


# ── Daily counter reset helper ────────────────────────────────────────────────

async def _maybe_reset_daily_counter(today_str: str) -> None:
    """Reset the daily counter if the date has rolled over."""
    state = await state_repo.get_state()
    if state is None or state.last_reset_date != today_str:
        logger.info(f"New day ({today_str}). Resetting daily sent counter.")
        await state_repo.reset_daily_counter(today_str)


# ── Normal send helpers ───────────────────────────────────────────────────────

async def _send_item(
    sender: TelegramSender,
    item: QueueItem,
    today_str: str,
) -> None:
    """
    Attempt to send a real pending item.  Updates queue status and daily
    counter.  Re-queues on FloodWait / PeerIdInvalid.  Marks failed on
    permanent errors.
    """
    allowed = await state_repo.try_increment_daily_sent_count(
        today_str, settings.DAILY_LIMIT
    )
    if not allowed:
        await queue_repo.update_item_status(item.id, "pending")
        logger.info(
            f"Daily limit reached ({settings.DAILY_LIMIT}). "
            f"Item {item.id} re-queued as pending."
        )
        return

    try:
        success = await sender.send_item(item)

        if success:
            await queue_repo.update_item_status(item.id, "sent")
            logger.info(f"Item {item.id} sent successfully.")
        else:
            await state_repo.decrement_daily_sent_count()
            await queue_repo.update_item_status(
                item.id, "failed", "send_item() returned False"
            )
            logger.warning(
                f"Item {item.id} marked as failed (send_item returned False)."
            )

    except FloodWait as exc:
        wait_seconds = exc.value
        sleep_for = min(wait_seconds, _MAX_FLOODWAIT_SLEEP)
        logger.warning(
            f"FloodWait {wait_seconds}s for item {item.id}. "
            f"Sleeping {sleep_for}s then re-queuing as pending."
        )
        await state_repo.decrement_daily_sent_count()
        await asyncio.sleep(sleep_for)
        await queue_repo.update_item_status(item.id, "pending")
        logger.info(f"Item {item.id} re-queued as pending after FloodWait.")

    except PeerIdInvalid as exc:
        await state_repo.decrement_daily_sent_count()
        retry_count = getattr(item, "retry_count", 0) or 0
        if retry_count >= _MAX_PEER_RETRIES:
            logger.error(
                f"PeerIdInvalid for item {item.id} exceeded max retries "
                f"({_MAX_PEER_RETRIES}). Marking as failed. Error: {exc}"
            )
            await queue_repo.update_item_status(
                item.id,
                "failed",
                f"PeerIdInvalid after {_MAX_PEER_RETRIES} retries: {exc}",
            )
        else:
            logger.warning(
                f"PeerIdInvalid for item {item.id} "
                f"(retry {retry_count + 1}/{_MAX_PEER_RETRIES}): {exc}. "
                "Re-queuing as pending."
            )
            await queue_repo.update_item_status(item.id, "pending")

    except Exception as exc:
        err_str = str(exc).lower()
        if "peer id invalid" in err_str:
            await state_repo.decrement_daily_sent_count()
            retry_count = getattr(item, "retry_count", 0) or 0
            if retry_count >= _MAX_PEER_RETRIES:
                logger.error(
                    f"PeerIdInvalid (via Exception) for item {item.id} exceeded "
                    f"max retries ({_MAX_PEER_RETRIES}). Marking as failed. Error: {exc}"
                )
                await queue_repo.update_item_status(
                    item.id,
                    "failed",
                    f"PeerIdInvalid after {_MAX_PEER_RETRIES} retries: {exc}",
                )
            else:
                logger.warning(
                    f"PeerIdInvalid (via Exception) for item {item.id} "
                    f"(retry {retry_count + 1}/{_MAX_PEER_RETRIES}): {exc}. "
                    "Re-queuing as pending."
                )
                await queue_repo.update_item_status(item.id, "pending")
            return

        await state_repo.decrement_daily_sent_count()
        logger.error(
            f"Unexpected error processing item {item.id}: {exc}", exc_info=True
        )
        await queue_repo.update_item_status(item.id, "failed", str(exc))


# ── Vault replay helpers ──────────────────────────────────────────────────────

async def _ensure_replay_cycle() -> bool:
    """
    Ensure a valid replay cycle is loaded in vault_replay_state.

    - If remaining_ids is non-empty: cycle already active, nothing to do.
    - If remaining_ids is empty (cycle exhausted or first time): fetch all
      vault ids, shuffle, and start a new cycle.

    Returns True if a cycle is ready (vault is non-empty), False if vault
    is completely empty and there is nothing to replay.
    """
    state = await vault_replay_repo.get_state()
    remaining = (state or {}).get("remaining_ids", [])

    if remaining:
        # Active cycle with items left — nothing to initialise.
        return True

    # Cycle exhausted or first entry — build a new one.
    vault_ids: list[ObjectId] = await queue_repo.get_vault_item_ids()

    if not vault_ids:
        logger.info("[REPLAY] Vault is empty. Nothing to replay.")
        return False

    random.shuffle(vault_ids)
    current_cycle = (state or {}).get("cycle_number", 0)
    new_cycle = current_cycle + 1

    await vault_replay_repo.reset_cycle(
        shuffled_ids=vault_ids,
        cycle_number=new_cycle,
    )

    logger.info(
        f"[REPLAY] Starting new cycle #{new_cycle} with {len(vault_ids)} items."
    )
    return True


async def _send_replay_item(sender: TelegramSender, today_str: str) -> None:
    """
    Pop one item from the replay cycle and send it.

    The vault document is fetched by _id.  A transient QueueItem is built
    from its fields and passed directly to TelegramSender.send_item() —
    the vault document's status is NEVER modified; it stays 'sent'.

    FloodWait: the popped id is pushed back to the front of remaining_ids
    so it is retried on the next tick.
    """
    cycle_ready = await _ensure_replay_cycle()
    if not cycle_ready:
        return

    # Atomic pop — returns None if remaining_ids just became empty (race-safe).
    oid: Optional[ObjectId] = await vault_replay_repo.pop_next_replay_id()
    if oid is None:
        # Another concurrent tick drained the list — will be refilled next tick.
        logger.debug("[REPLAY] pop_next_replay_id returned None (race). Skipping tick.")
        return

    # Fetch the original vault document to reconstruct a sendable QueueItem.
    vault_doc = await queue_repo.get_item_by_id(oid)
    if vault_doc is None:
        logger.warning(
            f"[REPLAY] Vault document {oid} not found (deleted?). Skipping."
        )
        return

    remaining = await vault_replay_repo.count_remaining()
    cycle_num = await vault_replay_repo.get_current_cycle_number()

    logger.info(
        f"[REPLAY] Cycle {cycle_num}: sending vault item "
        f"message_id={vault_doc.message_id} ({remaining} remaining in cycle)."
    )

    # Build a fresh QueueItem without a persistent _id so it doesn't
    # collide with the real queue documents.
    replay_item = QueueItem(
        message_id=vault_doc.message_id,
        media_group_id=vault_doc.media_group_id,
        message_ids=vault_doc.message_ids,
        status="pending",
    )

    # Daily limit check — replay sends count toward the limit.
    allowed = await state_repo.try_increment_daily_sent_count(
        today_str, settings.DAILY_LIMIT
    )
    if not allowed:
        # Push the id back so it is the next one sent tomorrow.
        await vault_replay_repo.push_back_id(oid)
        logger.info(
            f"[REPLAY] Daily limit reached ({settings.DAILY_LIMIT}). "
            f"Replay item message_id={vault_doc.message_id} deferred to next day."
        )
        return

    try:
        success = await sender.send_item(replay_item)

        if success:
            logger.info(
                f"[REPLAY] Cycle {cycle_num}: vault item "
                f"message_id={vault_doc.message_id} sent successfully."
            )
            if remaining == 0:
                logger.info(
                    f"[REPLAY] Cycle {cycle_num} complete. "
                    "Starting next cycle on the following tick."
                )
        else:
            await state_repo.decrement_daily_sent_count()
            logger.warning(
                f"[REPLAY] send_item returned False for vault item "
                f"message_id={vault_doc.message_id}. Skipping (not re-queued)."
            )

    except FloodWait as exc:
        wait_seconds = exc.value
        sleep_for = min(wait_seconds, _MAX_FLOODWAIT_SLEEP)
        logger.warning(
            f"[REPLAY] FloodWait {wait_seconds}s for vault item "
            f"message_id={vault_doc.message_id}. "
            f"Sleeping {sleep_for}s then pushing id back to replay queue."
        )
        await state_repo.decrement_daily_sent_count()
        await asyncio.sleep(sleep_for)
        await vault_replay_repo.push_back_id(oid)
        logger.info(
            f"[REPLAY] Vault item message_id={vault_doc.message_id} "
            "pushed back to front of replay queue."
        )

    except PeerIdInvalid as exc:
        await state_repo.decrement_daily_sent_count()
        logger.warning(
            f"[REPLAY] PeerIdInvalid for vault item "
            f"message_id={vault_doc.message_id}: {exc}. Skipping this item."
        )
        # Do not push back — peer issues on replay items should not loop.

    except Exception as exc:
        err_str = str(exc).lower()
        await state_repo.decrement_daily_sent_count()

        if "peer id invalid" in err_str:
            logger.warning(
                f"[REPLAY] PeerIdInvalid (via Exception) for vault item "
                f"message_id={vault_doc.message_id}: {exc}. Skipping."
            )
        else:
            logger.error(
                f"[REPLAY] Unexpected error sending vault item "
                f"message_id={vault_doc.message_id}: {exc}",
                exc_info=True,
            )


# ── Main scheduled job ────────────────────────────────────────────────────────

async def posting_job(sender: TelegramSender) -> None:
    """
    Scheduled job.  Fires every SEND_INTERVAL_SECONDS.

    Priority order:
      1. Real pending content  → send immediately, skip replay logic.
      2. Vault replay          → only reached when queue is genuinely empty.
    """
    logger.debug("Posting worker tick started.")

    today_str = _today_str()
    await _maybe_reset_daily_counter(today_str)

    # ── Priority 1: real pending content ─────────────────────────────────────
    item = await queue_repo.get_next_pending_item()

    if item is not None:
        # Deactivate replay mode in state (fire-and-forget, non-blocking).
        replay_state = await vault_replay_repo.get_state()
        if replay_state and replay_state.get("active"):
            await vault_replay_repo.set_active(False)
            logger.info(
                "[REPLAY] Exiting replay mode — new real content detected."
            )

        logger.info(
            f"Dequeued real item: source_message_id={item.message_id} id={item.id}"
        )
        await _send_item(sender, item, today_str)
        logger.debug("Posting worker tick complete.")
        return

    # ── Priority 2: vault replay ──────────────────────────────────────────────
    vault_ids = await queue_repo.get_vault_item_ids()

    if not vault_ids:
        logger.debug("Queue empty. Vault empty. Nothing to post.")
        logger.debug("Posting worker tick complete.")
        return

    logger.info(
        f"[REPLAY] Queue empty. Vault has {len(vault_ids)} items. "
        "Entering replay mode."
    )
    await vault_replay_repo.set_active(True)
    await _send_replay_item(sender, today_str)

    logger.debug("Posting worker tick complete.")
