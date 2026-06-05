from __future__ import annotations

"""
submission_handler.py — User content submission flow.

Handles:
  - Channel membership gate (Section 10.1)
  - Consent / terms acceptance gate (Section 10.1)
  - Daily submission cap
  - Album buffering and batching (Section 10.4)
  - Vault staging, topic routing, and pending registration
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.core.redis_client import get_redis
from app.moderation.verification_hub import forward_to_verification
from app.services.submission_service import register_pending
from app.services.subscription_service import SubscriptionService
from app.models.subscription import Plan
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Album collector ───────────────────────────────────────────────────────────
# WeakValueDictionary is NOT used here (intentionally).  Weak refs to
# asyncio.Lock objects get GC'd between assignment and `async with lock`,
# silently losing the lock.  Use plain dicts with explicit cleanup instead.

_album_cache: dict[str, list[Message]] = {}
_album_locks: dict[str, asyncio.Lock] = {}
_album_tasks: dict[str, asyncio.Task] = {}
_ALBUM_WAIT_SECONDS = 2.0

_MAX_RETRIES = 3
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER

# Cached bot username to avoid repeated get_me() calls.
_bot_username: Optional[str] = None


# ── Internal utilities ────────────────────────────────────────────────────────

async def _safe_reply(
    message: Message, text: str, reply_markup=None
) -> Optional[Message]:
    """
    Reply to a message with HTML parse mode.
    Swallows all exceptions — used only for user-facing feedback where
    failure is non-fatal.
    """
    try:
        return await message.reply_text(
            text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.warning(
            "_safe_reply: failed to send reply",
            extra={"ctx_error": str(e)},
        )
        return None


async def _send_hub_message(
    client: Client,
    chat_id: int,
    text: str,
    thread_id: Optional[int] = None,
) -> Optional[Message]:
    """
    Send a message to the hub group with FloodWait handling and exponential backoff.

    Returns the sent Message on success, None on permanent failure.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            kwargs = {"parse_mode": ParseMode.HTML}
            if thread_id:
                kwargs["message_thread_id"] = thread_id
            return await client.send_message(
                chat_id=chat_id,
                text=text,
                **kwargs,
            )
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            logger.info(
                "_send_hub_message: FloodWait",
                extra={"ctx_chat_id": chat_id, "ctx_wait": wait},
            )
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "_send_hub_message: RPCError",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(
                "_send_hub_message: unexpected error",
                extra={"ctx_chat_id": chat_id, "ctx_error": str(e)},
            )
            return None
    return None


async def _get_bot_username(client: Client) -> str:
    """
    Return the bot's username, cached after the first successful fetch.
    Falls back to an empty string on error.
    """
    global _bot_username
    if _bot_username:
        return _bot_username
    try:
        me = await client.get_me()
        _bot_username = me.username or ""
    except Exception as e:
        logger.warning(
            "_get_bot_username: get_me() failed",
            extra={"ctx_error": str(e)},
        )
        _bot_username = ""
    return _bot_username


# ── Membership gate ───────────────────────────────────────────────────────────

async def _verify_channel_membership(client: Client, user_id: int) -> bool:
    """
    Verify that a user is an active member of VAULT_CHANNEL_ID (Section 10.1).

    Returns True if the user is a member or if no channel is configured.
    Returns False on any access-denial error; logs and returns False on unexpected errors.
    """
    channel_id = settings.VAULT_CHANNEL_ID
    if not channel_id:
        return True  # No gate configured — allow all

    try:
        member = await client.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status not in (
            ChatMemberStatus.LEFT,
            ChatMemberStatus.BANNED,
        )
    except UserNotParticipant:
        return False
    except RPCError as e:
        logger.warning(
            "_verify_channel_membership: RPC error",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        return False
    except Exception as e:
        logger.error(
            "_verify_channel_membership: unexpected error",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        return False


# ── Menu callbacks ────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:submit$"))
async def handle_submit_menu(client: Client, callback: CallbackQuery) -> None:
    """Handle the 'Submit Content' menu button — show submission instructions."""
    await callback.answer()
    await callback.message.edit_text(
        "📨 <b>Submit Content</b>\n\n"
        "Send your photo, video, album, or any file now.\n\n"
        "<i>By sending content, you confirm you hold rights to share it.</i>",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("← Back", callback_data="menu:home")]]
        ),
        parse_mode=ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^menu:anonymous$"))
async def handle_anonymous_toggle(client: Client, callback: CallbackQuery) -> None:
    """Toggle anonymous submission mode for the user (stored in Redis, 30-day TTL)."""
    user_id = callback.from_user.id
    redis = get_redis()
    key = f"user:anon:{user_id}"

    is_anon = await redis.exists(key)
    if is_anon:
        await redis.delete(key)
        await callback.answer("Anonymous mode: OFF", show_alert=True)
    else:
        await redis.set(key, "1", ex=86400 * 30)
        await callback.answer("Anonymous mode: ON", show_alert=True)


# ── Consent callback ──────────────────────────────────────────────────────────
# NOTE: This handler uses `consent:agree` (no user_id suffix) for the
# submission flow.  The creator onboarding handler uses `consent:agree:{user_id}`.
# The regex patterns are distinct — both can coexist safely.

@Client.on_callback_query(filters.regex(r"^consent:agree$"))
async def handle_consent_agree_submission(client: Client, callback: CallbackQuery) -> None:
    """
    Record the user's consent to submission terms in the consent_records collection.

    Uses upsert so repeated clicks are idempotent.
    """
    user_id = callback.from_user.id
    db = DatabaseManager.get_db()
    now = datetime.now(timezone.utc)

    try:
        await db["consent_records"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "user_id": user_id,
                    "is_active": True,
                    "agreed_at": now,
                    "record_type": "attestation",
                    "attestation_version": "v1.0",
                }
            },
            upsert=True,
        )
    except Exception as e:
        logger.error(
            "handle_consent_agree_submission: DB write failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        await callback.answer("⚠️ Could not record consent. Please try again.", show_alert=True)
        return

    await callback.answer("Thank you! You can now submit content.", show_alert=True)
    try:
        await callback.message.edit_text(
            "✅ <b>Terms Agreed</b>\n\nYou can now send photos and videos directly to this chat.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("← Back", callback_data="menu:home")]]
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


# ── Main media submission handler ─────────────────────────────────────────────

@Client.on_message(
    filters.private
    & (
        filters.photo
        | filters.video
        | filters.document
        | filters.animation
        | filters.audio
        | filters.voice
        | filters.video_note
    )
)
async def handle_media_submission(client: Client, message: Message) -> None:
    """
    Entry point for all private media messages.

    Gate sequence (each gate returns early on failure):
      1. Payment flow exclusion.
      2. Channel membership check (Section 10.1).
      3. Consent / terms acceptance (Section 10.1).
      4. Daily submission cap.

    After gates pass:
      - Single media: forwarded directly to _finalize_submission.
      - Album (media_group_id): buffered per-group; _process_album fires after
        _ALBUM_WAIT_SECONDS to collect all parts before submitting.
    """
    if not message.from_user:
        return

    user_id = message.from_user.id

    logger.info(
        "submission_received",
        extra={
            "ctx_user_id": user_id,
            "ctx_msg_id": message.id,
            "ctx_media_group_id": message.media_group_id,
            "ctx_media_type": message.media.value if message.media else "unknown",
        },
    )

    redis = get_redis()

    # Gate 1: Skip if user has an active payment session
    if await redis.exists(f"pay_session:{user_id}"):
        return

    # Gate 2: Channel membership (Section 10.1)
    is_member = await _verify_channel_membership(client, user_id)
    if not is_member:
        # Build join link — use bot username as final fallback to avoid
        # calling get_me() on every non-member message.
        channel_link = f"https://t.me/{await _get_bot_username(client)}"
        try:
            chat = await client.get_chat(settings.VAULT_CHANNEL_ID)
            if getattr(chat, "invite_link", None):
                channel_link = chat.invite_link
            elif getattr(chat, "username", None):
                channel_link = f"https://t.me/{chat.username}"
        except Exception as e:
            logger.warning(
                "handle_media_submission: could not fetch channel link",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        await _safe_reply(
            message,
            "⚠️ <b>Channel Membership Required</b>\n\n"
            "You must join our main channel before you can submit content.\n\n"
            f'<a href="{channel_link}">👉 Join Here</a>\n\n'
            "After joining, send your content again.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("← Back", callback_data="menu:home")]]
            ),
        )
        return

    # Gate 3: Consent / terms acceptance (Section 10.1)
    db = DatabaseManager.get_db()
    try:
        consent = await db["consent_records"].find_one(
            {"user_id": user_id, "is_active": True}
        )
    except Exception as e:
        logger.error(
            "handle_media_submission: consent_records lookup failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        await _safe_reply(message, "⚠️ An error occurred. Please try again.")
        return

    if not consent:
        await _safe_reply(
            message,
            "⚠️ <b>Terms Acceptance Required</b>\n\n"
            "To submit content, you must agree to our terms and conditions.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ I Agree", callback_data="consent:agree")],
                    [InlineKeyboardButton("❌ Decline", callback_data="menu:home")],
                ]
            ),
        )
        return

    # Gate 4: Daily submission cap
    sub_service = SubscriptionService()
    try:
        plan = await sub_service.get_effective_plan(user_id)
    except Exception as e:
        logger.warning(
            "handle_media_submission: plan lookup failed — defaulting to FREE",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        plan = Plan.FREE

    daily_cap = 50 if plan != Plan.FREE else 5
    cap_key = f"cap:submit:{user_id}:{time.strftime('%Y-%m-%d')}"

    try:
        current_count = int(await redis.get(cap_key) or 0)
    except Exception as e:
        logger.warning(
            "handle_media_submission: cap_key read failed — allowing submission",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        current_count = 0

    if current_count >= daily_cap:
        await _safe_reply(
            message,
            f"🚫 <b>Daily Limit Reached</b>\n\n"
            f"You have reached your daily limit of {daily_cap} submissions.\n"
            "Premium users get higher limits.",
        )
        return

    # ── Album handling (Section 10.4) ──────────────────────────────────────────
    if message.media_group_id:
        group_id = message.media_group_id

        # Use plain dict for locks — WeakValueDictionary causes GC races.
        if group_id not in _album_locks:
            _album_locks[group_id] = asyncio.Lock()

        lock = _album_locks[group_id]

        async with lock:
            if group_id not in _album_cache:
                _album_cache[group_id] = []
                # Cancel any stale task for this group
                existing = _album_tasks.get(group_id)
                if existing and not existing.done():
                    existing.cancel()
                task = asyncio.create_task(
                    _process_album(client, group_id, user_id, cap_key),
                    name=f"album-{group_id}",
                )
                _album_tasks[group_id] = task

            _album_cache[group_id].append(message)
        return

    # ── Single media ────────────────────────────────────────────────────────────
    await _finalize_submission(client, [message], user_id, cap_key)


async def _process_album(
    client: Client, group_id: str, user_id: int, cap_key: str
) -> None:
    """
    Wait for the album collection window to close, then submit the buffered album.

    Albums are always submitted as a single logical unit (Section 10.4).
    """
    await asyncio.sleep(_ALBUM_WAIT_SECONDS)

    lock = _album_locks.get(group_id)
    if not lock:
        return

    async with lock:
        messages = _album_cache.pop(group_id, [])
        _album_tasks.pop(group_id, None)
        _album_locks.pop(group_id, None)  # Explicit cleanup

    if not messages:
        return

    messages.sort(key=lambda m: m.id)
    await _finalize_submission(client, messages, user_id, cap_key)


async def _finalize_submission(
    client: Client,
    messages: list[Message],
    user_id: int,
    cap_key: str,
) -> None:
    """
    Complete the submission pipeline:

      1. Increment daily cap counter (Redis).
      2. Archive content to vault as PENDING — write to MongoDB BEFORE any
         Telegram message is sent (restart-safe).
      3. Get-or-create the user's permanent content review topic in the hub.
      4. Forward content to the verification hub for moderation.
      5. Post a submission summary card to the user's hub topic.
      6. Register submission in the pending registry.
      7. Notify user of successful submission.

    NOTE on approve buttons:
      The dynamic "Approve → [Group Name]" buttons per spec §10.3 are built
      inside verification_hub.forward_to_verification using QUEUE_GROUPS from
      settings.  This file correctly delegates that responsibility — no
      hardcoded group names here.

    NOTE on duplicate prevention:
      Content hash deduplication (spec §10.3) is the responsibility of
      archive_to_vault / submission_service.  If archive_to_vault raises on
      duplicate, the exception is caught here and the user is notified.
    """
    first_msg = messages[0]

    try:
        redis = get_redis()

        # Step 1: Increment daily cap
        try:
            await redis.incr(cap_key)
            await redis.expire(cap_key, 86400)
        except Exception as e:
            logger.warning(
                "_finalize_submission: cap increment failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        # Step 2: Archive to vault as PENDING (MongoDB write — restart-safe)
        from app.moderation.moderation_actions import archive_to_vault
        from app.core.models import ModerationState

        # dest="nsfw" is the safe staging default.
        # The moderator's approval action overrides the final destination.
        # initial_status must be a string (.value) not the enum object.
        await archive_to_vault(
            client=client,
            messages=messages,
            dest="nsfw",
            submitter_user_id=user_id,
            initial_status=ModerationState.PENDING.value,
        )

        # Step 3: Get or create user's permanent review topic
        from app.services.topic_manager import get_topic_manager

        topic_manager = get_topic_manager()
        topic_id: Optional[int] = None
        try:
            topic_id = await topic_manager.get_or_create_user_topic(client, user_id)
        except Exception as e:
            logger.warning(
                "_finalize_submission: topic creation failed — using general hub",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        # Step 4: Forward to verification hub
        # Dynamic approve buttons ("Approve → [Group Name]" per §10.3) are
        # built inside forward_to_verification using QUEUE_GROUPS from settings.
        success = await forward_to_verification(
            client=client,
            messages=messages,
            submitter_user_id=user_id,
            topic_id=topic_id,
        )

        if success:
            # Step 5: Post submission summary card to user's hub topic
            try:
                media_count = len(messages)
                caption = first_msg.caption or first_msg.text or "—"
                if len(caption) > 100:
                    caption = caption[:97] + "..."

                await _send_hub_message(
                    client=client,
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    text=(
                        f"📥 <b>CONTENT SUBMITTED</b>\n\n"
                        f"<b>Media Count:</b> {media_count}\n"
                        f"<b>Caption:</b> {caption}"
                    ),
                    thread_id=topic_id,
                )
            except Exception as e:
                logger.warning(
                    "_finalize_submission: hub card post failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

            # Audit log (non-fatal)
            try:
                from app.services.audit_service import get_audit
                await get_audit().log(
                    action="CONTENT_SUBMITTED",
                    performed_by=user_id,
                    target_user_id=user_id,
                    details={"media_count": len(messages)},
                )
            except Exception as e:
                logger.warning(
                    "_finalize_submission: audit_log failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

            # Step 6: Register in pending registry (keyed by original first message ID)
            try:
                await register_pending(user_id, messages)
            except Exception as e:
                logger.warning(
                    "_finalize_submission: register_pending failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

            # Step 7: Notify user
            await _safe_reply(
                first_msg,
                "✅ <b>Content Submitted!</b>\n\n"
                "Our moderators will review it shortly. "
                "You'll be notified once a decision is made.",
            )
        else:
            await _safe_reply(
                first_msg,
                "❌ <b>Failed to submit content.</b>\n\nPlease try again later.",
            )

    except Exception as e:
        logger.exception(
            "_finalize_submission: unhandled exception",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        await _safe_reply(
            first_msg,
            "⚠️ An unexpected error occurred. Please try again.",
        )
