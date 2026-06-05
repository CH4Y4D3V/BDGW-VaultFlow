from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import UserNotParticipant, RPCError
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
# FIX GAP 5: WeakValueDictionary removed — weak refs to asyncio.Lock objects
# get GC'd between `_album_locks[group_id] = lock` and `async with lock`,
# losing the lock entirely. Use a plain dict with explicit cleanup instead.

_album_cache: dict[str, list[Message]] = {}
_album_locks: dict[str, asyncio.Lock] = {}
_album_tasks: dict[str, asyncio.Task] = {}
_ALBUM_WAIT_SECONDS = 2.0


async def _safe_reply(
    message: Message, text: str, reply_markup=None
) -> Optional[Message]:
    try:
        return await message.reply_text(
            text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
        )
    except Exception:
        return None


# ── Membership gate ───────────────────────────────────────────────────────────

async def _verify_channel_membership(
    client: Client, user_id: int
) -> bool:
    """
    FIX GAP 5: Channel membership gate — user must be member of VAULT_CHANNEL_ID
    (main channel) before they can submit content (Section 10.1).
    Returns True if member, False otherwise.
    """
    channel_id = settings.VAULT_CHANNEL_ID
    if not channel_id:
        return True  # No gate configured

    try:
        member = await client.get_chat_member(
            chat_id=channel_id, user_id=user_id
        )
        return member.status not in (
            ChatMemberStatus.LEFT,
            ChatMemberStatus.BANNED,
        )
    except UserNotParticipant:
        return False
    except RPCError as e:
        logger.warning(
            "Membership check RPC error",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        return False
    except Exception as e:
        logger.error(
            "Membership check unexpected error",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        return False


# ── Menu callbacks ────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:submit$"))
async def handle_submit_menu(client: Client, callback: CallbackQuery) -> None:
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
# FIX GAP 5: This handler uses `consent:agree` (no user_id) for the submission
# flow. The creator onboarding handler uses `consent:agree:{user_id}`.
# Both can coexist — the regex patterns are different.

@Client.on_callback_query(filters.regex(r"^consent:agree$"))
async def handle_consent_agree_submission(client: Client, callback: CallbackQuery) -> None:
    """Consent agreement for content submission flow."""
    user_id = callback.from_user.id
    db = DatabaseManager.get_db()
    now = datetime.now(timezone.utc)

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

    # Skip if user is in payment flow
    redis = get_redis()
    if await redis.exists(f"pay_session:{user_id}"):
        return

    # ── Gate 1: Channel membership (Section 10.1) ─────────────────────────────
    is_member = await _verify_channel_membership(client, user_id)
    if not is_member:
        bot_username = (await client.get_me()).username
        channel_link = f"https://t.me/{bot_username}"
        try:
            chat = await client.get_chat(settings.VAULT_CHANNEL_ID)
            if hasattr(chat, "invite_link") and chat.invite_link:
                channel_link = chat.invite_link
            elif hasattr(chat, "username") and chat.username:
                channel_link = f"https://t.me/{chat.username}"
        except Exception:
            pass

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

    # ── Gate 2: Consent / Terms acceptance (Section 10.1) ────────────────────
    db = DatabaseManager.get_db()
    consent = await db["consent_records"].find_one(
        {"user_id": user_id, "is_active": True}
    )
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

    # ── Gate 3: Daily submission cap ──────────────────────────────────────────
    sub_service = SubscriptionService()
    plan = await sub_service.get_effective_plan(user_id)
    daily_cap = 50 if plan != Plan.FREE else 5

    cap_key = f"cap:submit:{user_id}:{time.strftime('%Y-%m-%d')}"
    current_count = int(await redis.get(cap_key) or 0)

    if current_count >= daily_cap:
        await _safe_reply(
            message,
            f"🚫 <b>Daily Limit Reached</b>\n\n"
            f"You have reached your daily limit of {daily_cap} submissions.\n"
            f"Premium users get higher limits.",
        )
        return

    # ── Album handling ─────────────────────────────────────────────────────────
    if message.media_group_id:
        group_id = message.media_group_id

        # FIX GAP 5: Use plain dict lock — no WeakValueDictionary
        if group_id not in _album_locks:
            _album_locks[group_id] = asyncio.Lock()

        lock = _album_locks[group_id]

        async with lock:
            if group_id not in _album_cache:
                _album_cache[group_id] = []
                # Cancel existing task if any
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

    # ── Single media ───────────────────────────────────────────────────────────
    await _finalize_submission(client, [message], user_id, cap_key)


async def _process_album(
    client: Client, group_id: str, user_id: int, cap_key: str
) -> None:
    """Wait for album collection window, then submit."""
    await asyncio.sleep(_ALBUM_WAIT_SECONDS)

    lock = _album_locks.get(group_id)
    if not lock:
        return

    async with lock:
        messages = _album_cache.pop(group_id, [])
        _album_tasks.pop(group_id, None)
        _album_locks.pop(group_id, None)  # Clean up lock

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
    Complete the submission flow:
    1. Increment daily cap
    2. Archive to vault as PENDING (restart-safe — content preserved if bot restarts)
    3. Create/get user's content review topic in verification hub
    4. Forward to verification hub for moderation
    5. Register in pending registry
    6. Notify user

    FIX: archive_to_vault called with dest="nsfw" as safe staging default.
    The actual destination is overridden by the moderator's approval action.
    initial_status uses .value to pass a string as expected by the function.
    """
    first_msg = messages[0]

    try:
        # Increment daily cap counter
        redis = get_redis()
        await redis.incr(cap_key)
        await redis.expire(cap_key, 86400)

        # Archive to vault immediately as PENDING staging
        from app.moderation.moderation_actions import archive_to_vault
        from app.core.models import ModerationState

        # FIX BUG A: dest="nsfw" — valid staging destination, overridden on approve/queue
        # FIX BUG B: initial_status=ModerationState.PENDING.value — pass string not enum
        await archive_to_vault(
            client=client,
            messages=messages,
            dest="nsfw",
            submitter_user_id=user_id,
            initial_status=ModerationState.PENDING.value,
        )

        # Get or create content review topic for this user
        from app.services.topic_manager import get_topic_manager

        topic_manager = get_topic_manager()
        topic_id = None
        try:
            topic_id = await topic_manager.get_or_create_user_topic(
                client, user_id
            )
        except Exception as e:
            logger.warning(
                "Could not get user topic — forwarding to general hub",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        # Forward to verification hub
        success = await forward_to_verification(
            client=client,
            messages=messages,
            submitter_user_id=user_id,
            topic_id=topic_id,
        )

        if success:
            # FIX GAP 5: pending registry keyed by ORIGINAL first message ID
            await register_pending(user_id, messages)
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
            "Submission finalization failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        try:
            await _safe_reply(
                first_msg,
                "⚠️ An unexpected error occurred. Please try again.",
            )
        except Exception:
            pass