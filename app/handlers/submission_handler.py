# app/handlers/submission_handler.py
"""
Handles all user-submitted content (images, videos, text) in private chat.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict

from pyrogram import Client, ContinuePropagation, StopPropagation, filters
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.moderation.verification_hub import forward_to_verification
from app.services.submission_service import SubmissionService
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── In-process album buffer (replaces the missing app.storage module) ─────────
# keyed by media_group_id → list of Messages
_album_buffer: dict[str, list[Message]] = defaultdict(list)
_album_tasks: dict[str, asyncio.Task] = {}
_album_lock = asyncio.Lock()


@Client.on_message(
    (filters.media | filters.text)
    & filters.private
    & ~filters.command(["start", "rules", "mystatus", "ping", "help", "takedown", "cancel", "become_creator"])
    & ~filters.bot,
    group=2,
)
async def handle_submission(client: Client, message: Message) -> None:
    """
    Main entry point for all user submissions.
    Buffers album messages, then forwards to the verification hub.
    """
    if not message.from_user:
        # No user context — nothing to route. Raise StopPropagation not return:
        # a bare return() leaves group dispatch open, which lets group=3
        # (support handler) run and create a spurious support session.
        raise StopPropagation

    user_id = message.from_user.id

    # ── Gate 1: takedown FSM ────────────────────────────────────────────────
    # If the user is mid-takedown-flow, let takedown_handler claim this message.
    try:
        from app.handlers.takedown_handler import _get_fsm, STATE_IDLE
        state, _ = await _get_fsm(user_id)
        if state != STATE_IDLE:
            raise ContinuePropagation
    except ContinuePropagation:
        raise
    except Exception as fsm_err:
        # FSM lookup error — proceed with submission rather than silently
        # blocking the user.
        logger.warning(
            "submission_takedown_fsm_check_failed — proceeding",
            extra={"ctx_user_id": user_id, "ctx_error": str(fsm_err)},
        )

    # ── Gate 2: active support session ──────────────────────────────────────
    # Yield ONLY for genuinely active (admin-accepted) sessions. Stale PENDING
    # sessions (never accepted) must NOT permanently block content submission —
    # they live forever per spec (the 5-min unattended timer only notifies, it
    # does not close or expire the session).
    #
    # FIX: previously checked "opened_at" but route_to_support_topic inserts
    # the doc with field "created_at". The field name mismatch meant the
    # PENDING-within-10-min gate ALWAYS missed, so any user who ever sent an
    # unhandled message (creating a stale PENDING session) would have ALL their
    # subsequent content permanently routed to support instead of the mod queue.
    # Fixed: use "created_at" to match the actual insert field.
    try:
        from datetime import datetime, timezone, timedelta
        db_check = DatabaseManager.get_db()
        active_session = await db_check["support_sessions"].find_one(
            {"user_id": user_id, "status": "ACTIVE"},
        )
        if active_session:
            raise ContinuePropagation

        recent_cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)
        recent_pending = await db_check["support_sessions"].find_one(
            {
                "user_id": user_id,
                "status": "PENDING",
                "created_at": {"$gte": recent_cutoff},  # FIX: was "opened_at"
            },
        )
        if recent_pending:
            raise ContinuePropagation
    except ContinuePropagation:
        raise
    except Exception as support_check_err:
        logger.warning(
            "submission_support_session_check_failed — proceeding with submission",
            extra={"ctx_user_id": user_id, "ctx_error": str(support_check_err)},
        )

    # ── Consent / creator verification ──────────────────────────────────────
    db = DatabaseManager.get_db()
    service = SubmissionService(db)

    try:
        user_has_consent = await service.has_consent(user_id)
    except Exception as consent_err:
        logger.error(
            "submission_consent_check_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(consent_err)},
        )
        await message.reply_text(
            "⚠️ There was a temporary error checking your creator status. "
            "Please try again in a moment."
        )
        raise StopPropagation

    if not user_has_consent:
        # Not a verified creator — show consent form and stop. Never yield to
        # support: a non-creator sending media should see the attestation prompt,
        # not "Your message has been sent to the support team".
        from app.handlers.creator_onboarding import _send_onboarding_prompt
        await _send_onboarding_prompt(client, message)
        raise StopPropagation

    # ── Verified creator — process submission ────────────────────────────────
    # Wrap in try/except so that ANY exception inside _process_submission or
    # _handle_album_message still raises StopPropagation afterward.
    #
    # ROOT-CAUSE FIX: previously the raise StopPropagation was on the line
    # *after* await _process_submission(). If _process_submission raised an
    # unhandled exception (topic creation failure, DB error, FloodWait, etc.)
    # that line was never reached. Pyrogram caught the exception, logged it,
    # and passed the message to group=3 (support handler). The verified
    # creator's content ended up as a support ticket instead of the mod queue.
    try:
        if message.media_group_id:
            await _handle_album_message(client, message, user_id)
        else:
            await _process_submission(client, user_id, [message])
    except Exception as proc_err:
        logger.error(
            "submission_process_failed — notifying user",
            extra={"ctx_user_id": user_id, "ctx_error": str(proc_err)},
            exc_info=True,
        )
        try:
            await message.reply_text(
                "⚠️ Your submission could not be processed right now. "
                "Please try again in a moment."
            )
        except Exception:
            pass
    raise StopPropagation  # always — a verified creator's message is NEVER support


async def _handle_album_message(client: Client, message: Message, user_id: int) -> None:
    """Buffer album frames and flush after ALBUM_COLLECTION_SECONDS."""
    gid = message.media_group_id

    async with _album_lock:
        is_new = gid not in _album_buffer
        _album_buffer[gid].append(message)

        # Cancel any existing flush task and reschedule
        existing = _album_tasks.get(gid)
        if existing and not existing.done():
            existing.cancel()

        task = asyncio.create_task(
            _flush_album(client, gid, user_id),
            name=f"album-flush-{gid}",
        )
        _album_tasks[gid] = task


async def _flush_album(client: Client, gid: str, user_id: int) -> None:
    """Wait for album collection window, then process all buffered frames."""
    try:
        await asyncio.sleep(getattr(settings, "ALBUM_COLLECTION_SECONDS", 3.0))
    except asyncio.CancelledError:
        return

    async with _album_lock:
        messages = _album_buffer.pop(gid, [])
        _album_tasks.pop(gid, None)

    if not messages:
        return

    messages.sort(key=lambda m: m.id)
    await _process_submission(client, user_id, messages)


async def _process_submission(client: Client, user_id: int, messages: list[Message]) -> None:
    """
    Core submission processing:
    1. Get/create user hub topic
    2. Forward content to hub + post moderation card (Approve NSFW /
       Approve Premium / Reject) via verification_hub.forward_to_verification
    3. Register pending submission (in-memory registry consumed by
       callback_handler.handle_moderation_callback, + DB persistence)
    """
    db = DatabaseManager.get_db()
    service = SubmissionService(db)

    # ── Get user's hub topic ──────────────────────────────────────────────────
    topic_manager = get_topic_manager()
    first = messages[0]
    user = first.from_user

    topic_id = await topic_manager.get_or_create_user_topic(
        client,
        user_id,
        full_name=user.full_name if user else str(user_id),
        username=user.username if user else None,
    )

    if not topic_id:
        logger.error("submission_aborted_no_topic", extra={"ctx_user_id": user_id})
        try:
            await first.reply_text(
                "Sorry, there was an issue preparing your submission. Please try again in a moment."
            )
        except Exception:
            pass
        return

    # ── Forward to hub + post moderation card (with Approve/Reject buttons) ───
    # NOTE: previously this did its own ad-hoc client.forward_messages() +
    # plain client.send_message(card) with NO reply_markup -- admins had no
    # way to approve/reject. forward_to_verification() is the complete,
    # already-built pipeline that posts the card with mod_app_nsfw /
    # mod_app_prem / mod_reject buttons wired to handle_moderation_callback.
    delivered = await forward_to_verification(client, messages, user_id, topic_id)
    if not delivered:
        logger.error(
            "submission_forward_failed",
            extra={"ctx_user_id": user_id, "ctx_topic_id": topic_id},
        )
        try:
            await first.reply_text("Your submission could not be processed. Please try again.")
        except Exception:
            pass
        return

    # ── Persist pending record (in-memory registry + DB for restart recovery) ─
    try:
        await service.create_pending_submission(
            user_id=user_id,
            messages=messages,
            hub_topic_id=topic_id,
            hub_card_message_id=0,
            hub_forwarded_ids=delivered,  # FIX: hub copies never auto-deleted
        )
    except Exception as e:
        logger.error(
            "submission_persist_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    # ── Acknowledge user ──────────────────────────────────────────────────────
    try:
        await messages[-1].reply_text("✅ Your submission has been received and is pending review.")
    except Exception:
        pass