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
        return

    user_id = message.from_user.id

    # FIX: if the user is mid-takedown-flow (STATE_AWAITING_ID/REASON/LINK),
    # let handle_takedown_fsm process this message instead of treating it as
    # a content submission (relevant for verified creators using /takedown).
    from app.handlers.takedown_handler import _get_fsm, STATE_IDLE
    state, _ = await _get_fsm(user_id)
    if state != STATE_IDLE:
        raise ContinuePropagation

    # FIX: if the user has an open support conversation, yield so
    # support_handler's catch-all (group=3) can route this message
    # into their hub topic instead. But only block for RECENT sessions
    # to avoid permanently locking out users with stale PENDING sessions
    # that were never accepted by an admin (PENDING sessions live forever
    # per the spec — the 5-minute unattended timer only sends a notification,
    # it does not close or expire the session).
    #
    # Rules:
    #   ACTIVE  (admin accepted)   → always yield to support
    #   PENDING created < 10 min   → yield (covers /help immediate follow-up)
    #   PENDING created ≥ 10 min   → do NOT yield (stale, don't block submission)
    try:
        from datetime import datetime, timezone, timedelta
        db_check = DatabaseManager.get_db()
        # Check for an ACTIVE session first (most common case, fast indexed lookup)
        active_session = await db_check["support_sessions"].find_one(
            {"user_id": user_id, "status": "ACTIVE"},
        )
        if active_session:
            raise ContinuePropagation

        # Check for a RECENT pending session (within 10 minutes)
        recent_cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)
        recent_pending = await db_check["support_sessions"].find_one(
            {
                "user_id": user_id,
                "status": "PENDING",
                "opened_at": {"$gte": recent_cutoff},
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
        # DB hiccup — proceed with submission (don't silently swallow all
        # content from this user by yielding to support on every DB error)

    # ── Check consent first ───────────────────────────────────────────────────
    db = DatabaseManager.get_db()
    service = SubmissionService(db)

    try:
        user_has_consent = await service.has_consent(user_id)
    except Exception as consent_err:
        # Any DB/network error during consent check must NOT silently route to
        # support. Log it and tell the user to try again.
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
        # User has not completed creator onboarding (no creator_profile +
        # consent_record in DB). Show the consent attestation prompt directly.
        #
        # PREVIOUS BUG: this raised ContinuePropagation with a comment saying
        # "handled by creator_onboarding.py". But creator_onboarding.py has NO
        # handler for general media/text messages — only for the /become_creator
        # command and consent callbacks. ContinuePropagation therefore fell
        # straight through to group=3 (support_handler.handle_private_message),
        # which routed the message to the user's hub support topic with text
        # "Your message has been sent to the support team". Every non-creator
        # media send created a spurious support session instead of showing the
        # consent form. Fixed: call _send_onboarding_prompt() here and stop.
        from app.handlers.creator_onboarding import _send_onboarding_prompt
        await _send_onboarding_prompt(client, message)
        raise StopPropagation

    # ── Media group (album) buffering ─────────────────────────────────────────
    if message.media_group_id:
        await _handle_album_message(client, message, user_id)
        raise StopPropagation  # prevent support handler from also firing

    # ── Single message ────────────────────────────────────────────────────────
    await _process_submission(client, user_id, [message])
    raise StopPropagation  # prevent support handler from also firing


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