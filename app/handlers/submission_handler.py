from __future__ import annotations

"""
submission_handler.py — User content submission flow.

Handles:
  - Channel membership gate (Section 10.1) — ID sourced from hub_config
  - Terms acceptance gate (Section 10.1) — users.terms_accepted field (Section 25A.1)
  - Daily submission cap (ENV-controlled, not hardcoded)
  - Album buffering and batching (Section 10.4)
  - Per-item SHA-256 content hashing and combined album hash (Section 10.4)
  - Duplicate detection via content_fingerprints under Redis distributed lock (Section 10.5)
  - content_submissions written to MongoDB BEFORE any Telegram action (restart-safe)
  - Submission card posted to user's permanent topic with spec-compliant format (Section 10.3)
  - Moderation buttons: Approve NSFW | Approve Premium | Reject (Section 10.3)
  - Dual audit log: MongoDB audit_logs + Admin Logs topic (Section 22 / Section 9.4)
  - All user routing through user_topics_repo.get_or_create() (Section 9.2)
  - All hub IDs from hub_config, never hardcoded (Section 25 / Section 25A.19)
"""

import asyncio
import hashlib
import time
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
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
from app.core.hub_config import get_hub_config
from app.core.redis_client import get_redis
from app.moderation.verification_hub import forward_to_verification
from app.services.topic_manager import get_topic_manager
from app.services.audit_service import get_audit
from app.services.submission_service import register_pending
from app.services.subscription_service import SubscriptionService
from app.models.subscription import Plan
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Module-level singletons ───────────────────────────────────────────────────
# SubscriptionService instantiated once — not per-request.
_sub_service = SubscriptionService()

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
    failure is non-fatal. Never raises.
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
    reply_markup=None,
) -> Optional[Message]:
    """
    Send a message to the hub group with explicit FloodWait handling and
    exponential backoff on RPCError (Section 24).

    FloodWait is caught on every attempt and slept explicitly — it does NOT
    consume a retry slot. The attempt counter only increments on RPCError.
    Returns the sent Message on success, None after _MAX_RETRIES RPCErrors.
    """
    attempt = 0
    while attempt < _MAX_RETRIES:
        try:
            kwargs: dict = {"parse_mode": ParseMode.HTML}
            if thread_id:
                kwargs["message_thread_id"] = thread_id
            if reply_markup:
                kwargs["reply_markup"] = reply_markup
            return await client.send_message(
                chat_id=chat_id,
                text=text,
                **kwargs,
            )
        except FloodWait as e:
            # FloodWait: sleep and retry without consuming an attempt slot.
            wait = int(e.value) + _FLOOD_BUFFER
            logger.info(
                "_send_hub_message: FloodWait — sleeping",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)
            # Do not increment attempt — FloodWait is not a permanent failure
        except RPCError as e:
            attempt += 1
            logger.warning(
                "_send_hub_message: RPCError",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt,
                },
            )
            if attempt >= _MAX_RETRIES:
                return None
            await asyncio.sleep(2 ** (attempt - 1))
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

    Falls back to an empty string on error. Used only as a last-resort
    fallback when hub_config has no resolvable channel link.
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


# ── Content hashing ───────────────────────────────────────────────────────────

def _hash_file_id(file_id: str) -> str:
    """
    Compute the SHA-256 hash of a single Telegram file_id string.

    This is the per-item hash referenced in Section 10.4 and stored
    in the content_fingerprints collection (Section 25A.10).
    """
    return hashlib.sha256(file_id.encode()).hexdigest()


def _compute_album_hash(individual_hashes: list[str]) -> str:
    """
    Compute the combined album hash from individual media hashes in order.

    Per Section 10.4: "Album hash = hash of all individual media hashes
    combined in order."

    The '|' separator prevents hash-concatenation collisions — e.g. hashes
    ["AB", "C"] and ["A", "BC"] would produce the same raw concatenation
    without a separator but produce different results with one.

    Args:
        individual_hashes: Ordered list of per-item SHA-256 hex strings.
            Caller must ensure ordering is by ascending message ID.

    Returns:
        Single SHA-256 hex string representing the album fingerprint.
    """
    combined = "|".join(individual_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()


def _extract_file_id(message: Message) -> Optional[str]:
    """
    Extract the Telegram file_id from a media message.

    Handles all supported content types from Section 10.2:
    photo, video, document, animation (GIF), audio, voice, video_note.

    Returns None if the message contains no supported media.
    """
    if message.photo:
        return message.photo.file_id
    if message.video:
        return message.video.file_id
    if message.document:
        return message.document.file_id
    if message.animation:
        return message.animation.file_id
    if message.audio:
        return message.audio.file_id
    if message.voice:
        return message.voice.file_id
    if message.video_note:
        return message.video_note.file_id
    return None


def _detect_content_type(messages: list[Message]) -> str:
    """
    Detect the content_type label for a submission.

    For multi-message groups: always "album" (Section 10.4 — treated as
    one logical item). For single messages: returns the specific type
    matching the content_type values in content_submissions (Section 25A.9).
    """
    if len(messages) > 1:
        return "album"
    msg = messages[0]
    if msg.photo:
        return "photo"
    if msg.video:
        return "video"
    if msg.document:
        return "document"
    if msg.animation:
        return "gif"
    if msg.audio:
        return "audio"
    if msg.voice:
        return "voice"
    if msg.video_note:
        return "video_note"
    return "unknown"


# ── Membership gate ───────────────────────────────────────────────────────────

async def _verify_channel_membership(client: Client, user_id: int) -> bool:
    """
    Verify that a user is an active member of the main channel (Section 10.1).

    Channel ID is sourced from hub_config['main_channel_id'] (Section 25A.19).
    No hardcoded IDs. Returns True (gate open) when no channel is configured.

    FloodWait is handled with one explicit retry after sleeping.
    Returns False on any access-denial error.
    """
    hub_cfg = get_hub_config()
    channel_id = hub_cfg.get("main_channel_id")
    if not channel_id:
        logger.warning(
            "_verify_channel_membership: main_channel_id not in hub_config — gate skipped"
        )
        return True

    async def _check() -> bool:
        member = await client.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status not in (
            ChatMemberStatus.LEFT,
            ChatMemberStatus.BANNED,
        )

    try:
        return await _check()
    except UserNotParticipant:
        return False
    except FloodWait as e:
        wait = int(e.value) + _FLOOD_BUFFER
        logger.info(
            "_verify_channel_membership: FloodWait — retrying after sleep",
            extra={"ctx_user_id": user_id, "ctx_wait": wait},
        )
        await asyncio.sleep(wait)
        try:
            return await _check()
        except Exception as retry_err:
            logger.warning(
                "_verify_channel_membership: retry after FloodWait failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(retry_err)},
            )
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


# ── Duplicate detection (distributed lock) ───────────────────────────────────

async def _check_and_register_fingerprint(
    db,
    redis,
    media_hash: str,
    submission_id: ObjectId,
    user_id: int,
) -> bool:
    """
    Atomically check content_fingerprints for a duplicate and register if new.

    Uses a Redis distributed lock keyed on the media_hash to prevent TOCTOU
    race conditions under concurrent writes (Section 24 distributed locks,
    Section 10.5 duplicate prevention before any write).

    Lock timeout: 10 seconds. On lock contention, waits 750ms and re-checks
    DB existence — conservative treatment to avoid accepting a duplicate.

    Returns:
        True  — fingerprint is new; registration in content_fingerprints succeeded.
        False — fingerprint already exists (duplicate content).

    Raises:
        Exception on Redis or MongoDB failure — caller must catch and abort.
    """
    lock_key = f"lock:fingerprint:{media_hash}"
    lock_timeout_sec = 10

    acquired = await redis.set(lock_key, "1", nx=True, ex=lock_timeout_sec)
    if not acquired:
        logger.warning(
            "_check_and_register_fingerprint: lock contention — waiting and re-checking",
            extra={"ctx_hash": media_hash[:16], "ctx_user_id": user_id},
        )
        await asyncio.sleep(0.75)
        existing = await db["content_fingerprints"].find_one({"media_hash": media_hash})
        return existing is None

    try:
        existing = await db["content_fingerprints"].find_one({"media_hash": media_hash})
        if existing:
            return False

        now = datetime.now(timezone.utc)
        await db["content_fingerprints"].insert_one({
            "fingerprint_id": ObjectId(),
            "media_hash": media_hash,
            "submission_id": submission_id,
            "registered_at": now,
        })
        return True
    finally:
        # Always release — even on exception
        try:
            await redis.delete(lock_key)
        except Exception as e:
            logger.warning(
                "_check_and_register_fingerprint: lock release failed",
                extra={"ctx_error": str(e)},
            )


# ── Dual audit log writer ─────────────────────────────────────────────────────

async def _write_audit_and_admin_log(
    client: Client,
    action: str,
    target_user_id: int,
    detail: dict,
    admin_user_id: Optional[int] = None,
) -> None:
    """
    Write a dual audit entry: MongoDB audit_logs AND Admin Logs hub topic.

    Per Section 22: "All events must be written to two places simultaneously."
    Per Section 9.4: Admin Logs topic receives a structured entry card.

    Non-fatal — all exceptions caught and logged, never re-raised. A logging
    failure must never abort a submission pipeline.

    Args:
        client:          Pyrogram Client instance.
        action:          Action type string (e.g. "CONTENT_SUBMITTED").
        target_user_id:  The user this action concerns.
        detail:          Dict of action-specific data.
        admin_user_id:   Admin who performed the action (None = system-triggered).
    """
    now = datetime.now(timezone.utc)

    # Write 1: MongoDB audit_logs collection (Section 25A.17)
    try:
        audit = get_audit()
        await audit.log(
            action=action,
            performed_by=admin_user_id,
            target_user_id=target_user_id,
            details=detail,
        )
    except Exception as e:
        logger.warning(
            "_write_audit_and_admin_log: MongoDB write failed",
            extra={"ctx_action": action, "ctx_error": str(e)},
        )

    # Write 2: Admin Logs hub topic (Section 9.4)
    try:
        hub_cfg = get_hub_config()
        hub_id: Optional[int] = hub_cfg.get("hub_supergroup_id")
        logs_topic_id: Optional[int] = hub_cfg.get("admin_logs_topic_id")

        if not hub_id or not logs_topic_id:
            logger.warning(
                "_write_audit_and_admin_log: hub_id or logs_topic_id missing in hub_config",
                extra={"ctx_action": action},
            )
            return

        detail_lines = "\n".join(f"  {k}: {v}" for k, v in detail.items())
        admin_display = str(admin_user_id) if admin_user_id else "System"

        log_text = (
            f"<b>[{action}]</b>\n"
            f"Admin ID  : {admin_display}\n"
            f"Target ID : {target_user_id}\n"
            f"Time      : {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"Detail    :\n{detail_lines}"
        )
        await _send_hub_message(
            client=client,
            chat_id=hub_id,
            text=log_text,
            thread_id=logs_topic_id,
        )
    except Exception as e:
        logger.warning(
            "_write_audit_and_admin_log: Admin Logs topic write failed",
            extra={"ctx_action": action, "ctx_error": str(e)},
        )


# ── Menu callbacks ────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:submit$"))
async def handle_submit_menu(client: Client, callback: CallbackQuery) -> None:
    """
    Handle the 'Submit Content Anonymously' menu button (Section 6).

    Shows submission instructions. Gate enforcement (membership, consent)
    happens on actual media send so the user sees the instructions first.
    """
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


# NOTE: handle_anonymous_toggle has been permanently removed.
#
# Anonymous submission mode was explicitly removed from the spec.
# Section 10.3 states: "NOTE: Anonymous moderation no longer exists.
# Admin always sees: Full Name, Username, User ID."
#
# The callback_data pattern "menu:anonymous" is now dead. If an old client
# sends it, the default unhandled callback handler should answer and discard.


# ── Consent callback ──────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^consent:agree$"))
async def handle_consent_agree_submission(client: Client, callback: CallbackQuery) -> None:
    """
    Record the user's consent by setting users.terms_accepted = True.

    Uses the canonical users collection field (Section 25A.1: terms_accepted bool).
    The former consent_records collection is not in the spec schema — removed.
    Update is idempotent: safe to call multiple times.
    """
    user_id = callback.from_user.id
    db = DatabaseManager.get_db()
    now = datetime.now(timezone.utc)

    try:
        result = await db["users"].update_one(
            {"user_id": user_id},
            {"$set": {"terms_accepted": True, "terms_accepted_at": now}},
        )
        if result.matched_count == 0:
            logger.warning(
                "handle_consent_agree_submission: user document not found",
                extra={"ctx_user_id": user_id},
            )
    except Exception as e:
        logger.error(
            "handle_consent_agree_submission: users DB write failed",
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
    Entry point for all private media messages (Section 10.2).

    Gate sequence (each gate returns early on failure):
      1. Payment flow exclusion — skip silently if user has active payment session.
      2. Channel membership check (Section 10.1) — main_channel_id from hub_config.
      3. Terms acceptance check (Section 10.1) — users.terms_accepted field.
      4. Daily submission cap — limits from ENV settings, not hardcoded values.

    After all gates pass:
      - Single media: forwarded to _finalize_submission directly.
      - Album (media_group_id set): buffered per group_id; _process_album fires
        after _ALBUM_WAIT_SECONDS, sorts by message ID, then calls
        _finalize_submission with all parts as a single unit (Section 10.4).
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
    try:
        in_payment = await redis.exists(f"pay_session:{user_id}")
    except Exception as e:
        logger.warning(
            "handle_media_submission: Redis pay_session check failed — assuming no session",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        in_payment = False

    if in_payment:
        return

    # Gate 2: Channel membership — ID from hub_config, never from settings directly
    is_member = await _verify_channel_membership(client, user_id)
    if not is_member:
        hub_cfg = get_hub_config()
        channel_id = hub_cfg.get("main_channel_id")
        channel_link = ""

        if channel_id:
            try:
                chat = await client.get_chat(channel_id)
                if getattr(chat, "invite_link", None):
                    channel_link = chat.invite_link
                elif getattr(chat, "username", None):
                    channel_link = f"https://t.me/{chat.username}"
            except FloodWait as e:
                await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
            except Exception as e:
                logger.warning(
                    "handle_media_submission: could not fetch channel link",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

        if not channel_link:
            channel_link = f"https://t.me/{await _get_bot_username(client)}"

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

    # Gate 3: Terms acceptance — users.terms_accepted field (Section 25A.1)
    db = DatabaseManager.get_db()
    try:
        user_doc = await db["users"].find_one(
            {"user_id": user_id},
            {"terms_accepted": 1},
        )
        terms_accepted = bool(user_doc and user_doc.get("terms_accepted"))
    except Exception as e:
        logger.error(
            "handle_media_submission: users terms_accepted lookup failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        await _safe_reply(message, "⚠️ An error occurred. Please try again.")
        return

    if not terms_accepted:
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

    # Gate 4: Daily submission cap — limits sourced from ENV (not hardcoded)
    try:
        plan = await _sub_service.get_effective_plan(user_id)
    except Exception as e:
        logger.warning(
            "handle_media_submission: plan lookup failed — defaulting to FREE",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        plan = Plan.FREE

    daily_cap = (
        settings.PREMIUM_DAILY_SUBMISSION_LIMIT
        if plan != Plan.FREE
        else settings.FREE_DAILY_SUBMISSION_LIMIT
    )
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
            f"You have reached your daily submission limit of {daily_cap}.\n"
            "Premium users get higher limits.",
        )
        return

    # ── Album handling (Section 10.4) ──────────────────────────────────────────
    if message.media_group_id:
        group_id = message.media_group_id

        if group_id not in _album_locks:
            _album_locks[group_id] = asyncio.Lock()

        lock = _album_locks[group_id]

        async with lock:
            if group_id not in _album_cache:
                _album_cache[group_id] = []
                existing_task = _album_tasks.get(group_id)
                if existing_task and not existing_task.done():
                    existing_task.cancel()
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

    Albums are always submitted as a single logical unit (Section 10.4 —
    "Albums must never be split. Albums must never be partially approved.").

    Messages sorted by ascending ID before passing to _finalize_submission to
    guarantee deterministic ordering for the combined album hash computation.

    Lock cleanup order is intentional:
      1. Enter lock context.
      2. Pop cache + task refs while holding lock.
      3. Exit lock context (lock released).
      4. Pop lock ref AFTER context manager exits — never while holding it.
    """
    await asyncio.sleep(_ALBUM_WAIT_SECONDS)

    lock = _album_locks.get(group_id)
    if not lock:
        return

    async with lock:
        messages = _album_cache.pop(group_id, [])
        _album_tasks.pop(group_id, None)

    # Lock ref popped after context exit — not while the lock is held
    _album_locks.pop(group_id, None)

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
    Complete the submission pipeline — restart-safe, duplicate-safe, audit-complete.

    EXECUTION ORDER — MongoDB writes strictly precede all Telegram API calls:

      Step 1 : Compute per-item SHA-256 hashes + combined album hash (Section 10.4).
      Step 2 : Duplicate check against content_fingerprints under Redis lock.
               Duplicate → reject + dual-audit-log → return (Section 10.5).
      Step 3 : Insert content_submissions record (status=PENDING) to MongoDB.
               Fingerprint rollback on insert failure. Abort on failure.
      Step 4 : Increment daily cap counter in Redis (non-fatal).
      Step 5 : Resolve user's permanent topic via user_topics_repo.get_or_create().
               All routing uses the canonical repo (Section 9.2).
      Step 6 : Forward media to user's hub topic via forward_to_verification.
               FloodWait caught explicitly at this call site with one retry.
      Step 7 : Post spec-compliant admin card with moderation buttons (Section 10.3).
               Format: From (name + @username), User ID, Type, Hash, Time.
               Buttons: 🔞 Approve NSFW | ⭐ Approve Premium | ❌ Reject.
      Step 8 : Store hub card message_id back to submission record (non-fatal).
      Step 9 : Register in pending registry (non-fatal).
      Step 10: Dual audit log — MongoDB audit_logs + Admin Logs hub topic (Section 22).
      Step 11: Notify user of successful submission.

    Anonymous mode is removed — admin always sees full identity (Section 10.3).
    All hub IDs from hub_config — no hardcoded values (Section 25A.19).
    """
    first_msg = messages[0]

    try:
        db = DatabaseManager.get_db()
        redis = get_redis()
        hub_cfg = get_hub_config()
        hub_id: Optional[int] = hub_cfg.get("hub_supergroup_id")

        # ── Step 1: Compute per-item hashes + combined album hash ───────────────
        file_ids: list[str] = []
        individual_hashes: list[str] = []

        for msg in messages:
            fid = _extract_file_id(msg)
            if fid:
                file_ids.append(fid)
                individual_hashes.append(_hash_file_id(fid))
            else:
                logger.warning(
                    "_finalize_submission: could not extract file_id from message",
                    extra={"ctx_user_id": user_id, "ctx_msg_id": msg.id},
                )

        if not individual_hashes:
            logger.error(
                "_finalize_submission: no hashable media found — unsupported type",
                extra={"ctx_user_id": user_id},
            )
            await _safe_reply(
                first_msg,
                "⚠️ Unsupported media type. Please try a different file.",
            )
            return

        # Section 10.4: album hash = SHA-256 of all individual hashes combined in order.
        # For a single item, the item's own hash is the media_hash directly.
        if len(individual_hashes) == 1:
            media_hash = individual_hashes[0]
        else:
            media_hash = _compute_album_hash(individual_hashes)

        content_type = _detect_content_type(messages)
        media_group_id = first_msg.media_group_id  # None for single media

        # ── Step 2: Duplicate detection under distributed lock ──────────────────
        submission_id = ObjectId()  # Pre-generated — fingerprint references it

        try:
            is_new = await _check_and_register_fingerprint(
                db=db,
                redis=redis,
                media_hash=media_hash,
                submission_id=submission_id,
                user_id=user_id,
            )
        except Exception as e:
            logger.error(
                "_finalize_submission: fingerprint check failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            await _safe_reply(
                first_msg,
                "⚠️ An error occurred during submission. Please try again.",
            )
            return

        if not is_new:
            logger.info(
                "_finalize_submission: duplicate content — rejected",
                extra={"ctx_user_id": user_id, "ctx_hash": media_hash[:16]},
            )
            await _safe_reply(
                first_msg,
                "⚠️ <b>Duplicate Content Detected</b>\n\n"
                "This content has already been submitted. Duplicates are not accepted.",
            )
            await _write_audit_and_admin_log(
                client=client,
                action="CONTENT_DUPLICATE_REJECTED",
                target_user_id=user_id,
                detail={
                    "media_hash": media_hash,
                    "content_type": content_type,
                },
            )
            return

        # ── Step 3: Write content_submissions to MongoDB BEFORE Telegram ────────
        now = datetime.now(timezone.utc)

        submission_doc = {
            "_id": submission_id,
            "submission_id": submission_id,
            "user_id": user_id,
            "content_type": content_type,
            "media_group_id": media_group_id,
            "file_ids": file_ids,
            "individual_hashes": individual_hashes,
            "media_hash": media_hash,
            "status": "PENDING",
            "submitted_at": now,
            "reviewed_by": None,
            "reviewed_at": None,
            "rejection_reason": None,
            "vault_id": None,
            "hub_card_message_id": None,
        }

        try:
            await db["content_submissions"].insert_one(submission_doc)
        except Exception as e:
            logger.error(
                "_finalize_submission: content_submissions insert failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            # Roll back fingerprint so the user can retry successfully
            try:
                await db["content_fingerprints"].delete_one(
                    {"submission_id": submission_id}
                )
            except Exception as rb_err:
                logger.error(
                    "_finalize_submission: fingerprint rollback failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(rb_err)},
                )
            await _safe_reply(
                first_msg,
                "⚠️ Failed to record submission. Please try again.",
            )
            return

        # ── Step 4: Increment daily cap (non-fatal) ─────────────────────────────
        try:
            await redis.incr(cap_key)
            await redis.expire(cap_key, 86400)
        except Exception as e:
            logger.warning(
                "_finalize_submission: cap increment failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        # ── Step 5: Resolve user's permanent topic ───────────────────────────────
        # Canonical routing path per Section 9.2: get_topic_manager().get_or_create_user_topic().
        # If topic creation fails, we proceed without a thread_id — the card will
        # land in the hub general feed rather than the user's dedicated topic.
        topic_id: Optional[int] = None
        try:
            topic_mgr = get_topic_manager()
            topic_id = await topic_mgr.get_or_create_user_topic(
                bot=client,
                user_id=user_id,
                full_name=first_msg.from_user.full_name if first_msg.from_user else None,
                username=first_msg.from_user.username if first_msg.from_user else None,
            )
        except Exception as e:
            logger.warning(
                "_finalize_submission: topic get_or_create failed — posting without thread",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        # Admin always sees full identity — anonymous mode removed (Section 10.3)
        user_full_name = (
            first_msg.from_user.full_name if first_msg.from_user else "Unknown"
        )
        user_username = (
            f"@{first_msg.from_user.username}"
            if first_msg.from_user and first_msg.from_user.username
            else "—"
        )

        # ── Step 6: Forward media to hub topic — FloodWait handled explicitly ────
        try:
            forward_success = await forward_to_verification(
                client=client,
                messages=messages,
                submitter_user_id=user_id,
                topic_id=topic_id,
            )
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            logger.info(
                "_finalize_submission: FloodWait on forward_to_verification — retrying",
                extra={"ctx_user_id": user_id, "ctx_wait": wait},
            )
            await asyncio.sleep(wait)
            try:
                forward_success = await forward_to_verification(
                    client=client,
                    messages=messages,
                    submitter_user_id=user_id,
                    topic_id=topic_id,
                )
            except Exception as retry_err:
                logger.error(
                    "_finalize_submission: forward retry failed after FloodWait",
                    extra={"ctx_user_id": user_id, "ctx_error": str(retry_err)},
                )
                forward_success = False
        except Exception as e:
            logger.error(
                "_finalize_submission: forward_to_verification failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )
            forward_success = False

        if not forward_success:
            # Submission record persists in MongoDB for admin recovery.
            # Admin can find un-forwarded submissions: {status: "PENDING",
            # hub_card_message_id: null}.
            logger.error(
                "_finalize_submission: forward failed — submission in DB for recovery",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_submission_id": str(submission_id),
                },
            )
            await _safe_reply(
                first_msg,
                "❌ <b>Failed to submit content.</b>\n\nPlease try again later.",
            )
            return

        # ── Step 7: Register in pending registry (non-fatal) ─────────────────────
        # This is CRITICAL: it links the moderation buttons to the buffered messages.
        try:
            await register_pending(user_id, messages)
        except Exception as e:
            logger.warning(
                "_finalize_submission: register_pending failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        # ── Step 8: Dual audit log — MongoDB + Admin Logs topic ─────────────────
        await _write_audit_and_admin_log(
            client=client,
            action="CONTENT_SUBMITTED",
            target_user_id=user_id,
            detail={
                "submission_id": sub_id_str,
                "content_type": content_type,
                "media_count": len(messages),
                "media_hash": media_hash,
            },
        )

        # ── Step 9: Notify user ──────────────────────────────────────────────────
        await _safe_reply(
            first_msg,
            "✅ <b>Content Submitted!</b>\n\n"
            "Our moderators will review it shortly. "
            "You'll be notified once a decision is made.",
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