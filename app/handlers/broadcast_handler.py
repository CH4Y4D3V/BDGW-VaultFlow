"""
app/handlers/broadcast_handler.py

Admin /broadcast command handler — BDGW VaultFlow.

Per spec Section 9.5 (Hub Admin Commands) and the broadcast requirement
defined in the E-01 task specification.

PURPOSE:
    Admin sends /broadcast in bot private chat → bot acquires global lock,
    creates a MongoDB-backed session (COLLECTING), prompts admin for content.
    Admin sends text / media / album → bot buffers (albums) → shows preview
    with Confirm / Cancel buttons → on confirm, broadcasts to all non-banned
    users with FloodWait-safe delivery → emits BROADCAST_SENT to Admin Logs
    topic and audit_logs collection.

STATE MACHINE (broadcast_sessions collection):
    COLLECTING   → admin is composing content
    CONFIRMING   → admin is reviewing preview before sending
    BROADCASTING → broadcast in progress (lock held)
    COMPLETED    → finished successfully
    CANCELLED    → cancelled by admin or system

CRITICAL GUARANTEES:
    • Only ONE active broadcast at a time (Redis global lock).
    • All FSM state written to MongoDB BEFORE any Telegram message is sent.
    • FloodWait retried automatically per user.
    • Blocked/deactivated users skipped silently (no noise to admin).
    • Album content buffered for ALBUM_BUFFER_SECONDS to collect all frames.
    • Admin Logs topic + audit_logs written on completion.
    • No hardcoded IDs — all config via hub_config.

MongoDB collection: broadcast_sessions
    admin_user_id  : int
    state          : str  (COLLECTING|CONFIRMING|BROADCASTING|COMPLETED|CANCELLED)
    messages       : list[dict]  {from_chat_id, message_id, is_album, album_size}
    created_at     : datetime
    confirmed_at   : datetime | None
    completed_at   : datetime | None
    target_total   : int
    sent_count     : int
    failed_count   : int

Redis lock key : broadcast:global:lock
Lock TTL       : BROADCAST_LOCK_TTL_SECONDS (default 3600s — covers largest possible broadcast)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from pyrogram import Client, filters
from pyrogram.errors import (
    ChatWriteForbidden,
    FloodWait,
    InputUserDeactivated,
    MessageIdInvalid,
    PeerIdInvalid,
    UserIsBlocked,
)
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.core.database import DatabaseManager
from app.distribution.lock_service import DistributedLockService
from app.distribution.rate_limiter import TokenBucket
from app.repositories.admin_repository import AdminRepository
from app.services.audit_service import AuditService

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module constants — all tunable without code changes
# ---------------------------------------------------------------------------

ALBUM_BUFFER_SECONDS: float = 3.0
"""Seconds to wait after first album frame before treating album as complete."""

BROADCAST_LOCK_KEY: str = "broadcast:global:lock"
"""Redis key for the global broadcast mutex."""

BROADCAST_LOCK_TTL_SECONDS: int = 3600
"""Lock TTL in seconds — must outlast the longest possible broadcast run."""

PROGRESS_UPDATE_INTERVAL: int = 50
"""Edit admin's progress message every N users sent."""

MAX_FLOOD_RETRIES: int = 8
"""Maximum FloodWait retry attempts per Telegram call."""

INTER_SEND_DELAY: float = 0.05
"""Minimum delay between per-user sends to reduce flood risk (seconds)."""

BROADCAST_COLLECTION: str = "broadcast_sessions"
"""MongoDB collection name for broadcast FSM state."""

# ---------------------------------------------------------------------------
# In-memory album buffer (keyed by "{admin_user_id}:{media_group_id}")
# Cleared after flush; not persisted — only used for the brief buffering window.
# ---------------------------------------------------------------------------

_album_buffers: dict[str, list[Message]] = {}
_album_tasks: dict[str, asyncio.Task] = {}

# ---------------------------------------------------------------------------
# FloodWait-safe Telegram call helper
# ---------------------------------------------------------------------------


async def _safe_call(coro_factory, max_retries: int = MAX_FLOOD_RETRIES):
    """
    Execute a Telegram API coroutine factory with automatic FloodWait retry.

    The factory is called fresh on every attempt so the coroutine object is
    not reused across retries (Pyrogram coroutines are single-use).

    Args:
        coro_factory: Callable (no args) returning a coroutine.
        max_retries: Maximum attempts before re-raising.

    Returns:
        Return value of the successful coroutine call.

    Raises:
        FloodWait: If max_retries is exhausted without success.
        Any non-FloodWait exception from the last attempt.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            return await coro_factory()
        except FloodWait as exc:
            wait_secs = exc.value + 1
            logger.warning(
                "FloodWait: sleeping %ds (attempt %d/%d)",
                wait_secs, attempt + 1, max_retries,
            )
            await asyncio.sleep(wait_secs)
            last_exc = exc
        except (
            UserIsBlocked,
            InputUserDeactivated,
            ChatWriteForbidden,
            PeerIdInvalid,
            MessageIdInvalid,
        ):
            # Expected per-user failures — caller decides how to handle
            raise
        except Exception as exc:
            logger.error("Unexpected error in _safe_call: %s", exc, exc_info=True)
            raise
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Admin role check
# ---------------------------------------------------------------------------


async def _is_admin(user_id: int) -> bool:
    try:
        repo = AdminRepository()
        admin_doc = await repo.get_active_by_user_id(user_id)
        return admin_doc is not None
    except Exception as exc:
        logger.error("Admin role check failed for user %d: %s", user_id, exc)
        return False

# ---------------------------------------------------------------------------
# MongoDB session helpers
# ---------------------------------------------------------------------------


async def _create_session(admin_user_id: int) -> str:
    """
    Insert a new broadcast session document into MongoDB and return its ID.

    All fields are set to safe initial values. The document is persisted to
    MongoDB BEFORE any Telegram message is sent (restart safety).

    Args:
        admin_user_id: Telegram user ID of the initiating admin.

    Returns:
        String representation of the new session's ObjectId.

    Raises:
        RuntimeError: If MongoDB insertion fails.
    """
    db = DatabaseManager.get_db()
    doc = {
        "admin_user_id": admin_user_id,
        "state": "COLLECTING",
        "messages": [],
        "created_at": datetime.now(timezone.utc),
        "confirmed_at": None,
        "completed_at": None,
        "target_total": 0,
        "sent_count": 0,
        "failed_count": 0,
    }
    result = await db[BROADCAST_COLLECTION].insert_one(doc)
    if not result.inserted_id:
        raise RuntimeError(
            f"MongoDB insert_one returned no inserted_id for broadcast session "
            f"(admin_user_id={admin_user_id})"
        )
    session_id = str(result.inserted_id)
    logger.info(
        "Broadcast session created: session_id=%s admin=%d",
        session_id, admin_user_id,
    )
    return session_id


async def _get_session(session_id: str) -> Optional[dict]:
    """
    Fetch a broadcast session by its string session_id from MongoDB.

    Args:
        session_id: String representation of an ObjectId.

    Returns:
        The session document dict, or None if not found or on DB error.
    """
    try:
        db = DatabaseManager.get_db()
        return await db[BROADCAST_COLLECTION].find_one(
            {"_id": ObjectId(session_id)}
        )
    except Exception as exc:
        logger.error(
            "Failed to fetch broadcast session %s: %s", session_id, exc
        )
        return None


async def _get_admin_active_session(admin_user_id: int) -> Optional[dict]:
    """
    Return the most recent COLLECTING or CONFIRMING session for the given admin.

    Args:
        admin_user_id: Telegram user ID of the admin.

    Returns:
        Session document dict, or None if no active session exists.
    """
    try:
        db = DatabaseManager.get_db()
        return await db[BROADCAST_COLLECTION].find_one(
            {
                "admin_user_id": admin_user_id,
                "state": {"$in": ["COLLECTING", "CONFIRMING"]},
            },
            sort=[("created_at", -1)],
        )
    except Exception as exc:
        logger.error(
            "Failed to query active broadcast session for admin %d: %s",
            admin_user_id, exc,
        )
        return None


async def _update_session(session_id: str, update_fields: dict) -> None:
    """
    Apply a $set update to a broadcast session document.

    Failures are logged but never raised — callers should not crash on
    progress-tracking failures.

    Args:
        session_id: String ObjectId of the session.
        update_fields: Dict of field→value pairs to $set.
    """
    try:
        db = DatabaseManager.get_db()
        await db[BROADCAST_COLLECTION].update_one(
            {"_id": ObjectId(session_id)},
            {"$set": update_fields},
        )
    except Exception as exc:
        logger.error(
            "Failed to update broadcast session %s with %s: %s",
            session_id, update_fields, exc,
        )


async def _append_message_entry(session_id: str, entry: dict) -> None:
    """
    Append a message entry to the broadcast session's messages array.

    Each entry records the source information needed to copy the content to
    target users: {from_chat_id, message_id, is_album, album_size}.

    Args:
        session_id: String ObjectId of the session.
        entry: Dict describing the collected message.
    """
    try:
        db = DatabaseManager.get_db()
        await db[BROADCAST_COLLECTION].update_one(
            {"_id": ObjectId(session_id)},
            {"$push": {"messages": entry}},
        )
    except Exception as exc:
        logger.error(
            "Failed to append message entry to session %s: %s", session_id, exc
        )


async def _cancel_stale_sessions(admin_user_id: int) -> None:
    """
    Mark any lingering COLLECTING/CONFIRMING sessions as CANCELLED for the admin.

    Prevents orphaned sessions accumulating across bot restarts or multiple
    /broadcast invocations. Safe to call unconditionally.

    Args:
        admin_user_id: Telegram user ID whose stale sessions to cancel.
    """
    try:
        db = DatabaseManager.get_db()
        result = await db[BROADCAST_COLLECTION].update_many(
            {
                "admin_user_id": admin_user_id,
                "state": {"$in": ["COLLECTING", "CONFIRMING"]},
            },
            {"$set": {"state": "CANCELLED"}},
        )
        if result.modified_count:
            logger.info(
                "Cancelled %d stale broadcast session(s) for admin %d",
                result.modified_count, admin_user_id,
            )
    except Exception as exc:
        logger.warning(
            "Failed to cancel stale sessions for admin %d: %s",
            admin_user_id, exc,
        )


async def _check_global_broadcast_active() -> bool:
    """
    Return True if any broadcast is currently in BROADCASTING state.

    Used to prevent starting a new broadcast while one is running.

    Returns:
        True if a BROADCASTING session exists in MongoDB; False otherwise.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db[BROADCAST_COLLECTION].find_one({"state": "BROADCASTING"})
        return doc is not None
    except Exception as exc:
        logger.error("Failed to check active broadcast state: %s", exc)
        # Fail safe: assume one is running to prevent double-broadcast
        return True


# ---------------------------------------------------------------------------
# Admin Logs + Audit emit
# ---------------------------------------------------------------------------


async def _emit_broadcast_completed_log(
    client: Client,
    admin_user_id: int,
    admin_name: str,
    session_id: str,
    total: int,
    sent: int,
    failed: int,
) -> None:
    """
    Emit a BROADCAST_SENT event to both:
        1. Admin Logs topic in the Verification Hub (Telegram message).
        2. audit_logs collection in MongoDB.

    All failures are caught and logged — this function never raises.

    Args:
        client: Pyrogram client for sending the Admin Logs message.
        admin_user_id: ID of the admin who triggered the broadcast.
        admin_name: Display name for the Admin Logs entry.
        session_id: MongoDB session ID for the audit trail.
        total: Total users targeted.
        sent: Users successfully delivered to.
        failed: Users skipped (blocked, deactivated, error).
    """
    # 1. Write to audit_logs collection
    try:
        from app.services.audit_service import get_audit
        audit = get_audit()
        await audit.log(
            action="BROADCAST_SENT",
            performed_by=admin_user_id,
            target_user_id=None,
            details={
                "session_id": session_id,
                "target_total": total,
                "sent_count": sent,
                "failed_count": failed,
            },
        )
    except Exception as exc:
        logger.error(
            "Failed to write audit_logs for broadcast session %s: %s",
            session_id, exc,
        )

    # 2. Post to Admin Logs topic
    try:
        from app.services.topic_manager import _get_hub_config_int
        hub_sg_id = await _get_hub_config_int("hub_supergroup_id")
        logs_topic_id = await _get_hub_config_int("admin_logs_topic_id")

        if not hub_sg_id or not logs_topic_id:
            logger.warning(
                "hub_supergroup_id or admin_logs_topic_id missing from hub_config "
                "— skipping Admin Logs emit for broadcast %s",
                session_id,
            )
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        text = (
            "📢 <b>BROADCAST SENT</b>\n"
            f"Admin     : {admin_name}\n"
            f"Admin ID  : <code>{admin_user_id}</code>\n"
            f"Target    : All Users\n"
            f"Sent      : {sent} / {total}\n"
            f"Failed    : {failed}\n"
            f"Session   : <code>{session_id}</code>\n"
            f"Time      : {timestamp}"
        )
        await _safe_call(
            lambda: client.send_message(
                chat_id=hub_sg_id,
                text=text,
                message_thread_id=logs_topic_id,
                parse_mode="html",
            )
        )
    except Exception as exc:
        logger.error(
            "Failed to post Admin Logs for broadcast session %s: %s",
            session_id, exc,
        )


# ---------------------------------------------------------------------------
# Album buffering
# ---------------------------------------------------------------------------


async def _flush_album(
    client: Client,
    admin_user_id: int,
    session_id: str,
    buffer_key: str,
) -> None:
    """
    Flush a buffered album after ALBUM_BUFFER_SECONDS have elapsed.

    Waits for the buffer window, then:
        1. Extracts all buffered messages.
        2. Writes the album entry to MongoDB (restart safety).
        3. Transitions session state to CONFIRMING.
        4. Sends the confirmation preview to the admin.

    Args:
        client: Pyrogram client.
        admin_user_id: Admin's Telegram user ID.
        session_id: Active broadcast session ID.
        buffer_key: Key used to index into _album_buffers / _album_tasks.
    """
    await asyncio.sleep(ALBUM_BUFFER_SECONDS)

    messages = _album_buffers.pop(buffer_key, [])
    _album_tasks.pop(buffer_key, None)

    if not messages:
        logger.warning("Album buffer was empty on flush for key=%s", buffer_key)
        return

    first_msg = messages[0]
    album_size = len(messages)

    # Write to MongoDB BEFORE sending Telegram message (restart safety)
    entry = {
        "from_chat_id": first_msg.chat.id,
        "message_id": first_msg.id,
        "media_group_id": first_msg.media_group_id,
        "is_album": True,
        "album_size": album_size,
    }
    await _append_message_entry(session_id, entry)
    await _update_session(session_id, {"state": "CONFIRMING"})

    # Now send confirmation to admin
    try:
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Confirm Broadcast",
                    callback_data=f"broadcast:confirm:{session_id}",
                ),
                InlineKeyboardButton(
                    "❌ Cancel",
                    callback_data=f"broadcast:cancel:{session_id}",
                ),
            ]
        ])
        await _safe_call(
            lambda: client.send_message(
                chat_id=admin_user_id,
                text=(
                    "📢 <b>Broadcast Preview</b>\n\n"
                    f"Type    : Album ({album_size} items)\n"
                    f"Session : <code>{session_id}</code>\n\n"
                    "Confirm to broadcast this to all non-banned users."
                ),
                reply_markup=keyboard,
                parse_mode="html",
            )
        )
    except Exception as exc:
        logger.error(
            "Failed to send album confirmation to admin %d: %s",
            admin_user_id, exc,
        )


def _schedule_album_flush(
    client: Client,
    admin_user_id: int,
    session_id: str,
    buffer_key: str,
) -> None:
    """
    Schedule (or reschedule) the album flush task for the given buffer_key.

    If a flush task is already pending for this key (same album receiving more
    frames), it is cancelled and replaced. The timer resets on every new frame
    receipt, ensuring the album is complete before flushing.

    Args:
        client: Pyrogram client.
        admin_user_id: Admin's Telegram user ID.
        session_id: Active broadcast session ID.
        buffer_key: Buffer key in format "{admin_id}:{media_group_id}".
    """
    existing = _album_tasks.get(buffer_key)
    if existing and not existing.done():
        existing.cancel()

    task = asyncio.create_task(
        _flush_album(client, admin_user_id, session_id, buffer_key)
    )
    _album_tasks[buffer_key] = task


# ---------------------------------------------------------------------------
# Broadcast execution
# ---------------------------------------------------------------------------


async def _execute_broadcast(
    client: Client,
    admin_user_id: int,
    admin_name: str,
    session_id: str,
    session: dict,
) -> None:
    """
    Execute the broadcast: deliver content to every non-banned user.

    This function runs as a background asyncio Task. It:
        1. Writes BROADCASTING state to MongoDB FIRST (restart safety).
        2. Acquires the global Redis broadcast lock.
        3. Fetches all non-banned user IDs.
        4. Iterates users, sending with FloodWait-safe helper.
        5. Updates progress counters in MongoDB every PROGRESS_UPDATE_INTERVAL.
        6. Edits admin's progress message at the same interval.
        7. On completion: writes COMPLETED state, emits audit log.
        8. Releases the Redis lock.

    Users who have blocked the bot, deactivated their account, or are otherwise
    unreachable are counted as failed but do NOT halt the broadcast.

    Args:
        client: Pyrogram client.
        admin_user_id: Admin who triggered the broadcast.
        admin_name: Full name string for Admin Logs.
        session_id: MongoDB broadcast session ID.
        session: The session document (from CONFIRMING state).
    """
    # ── Step 1: Write BROADCASTING state to MongoDB before any Telegram calls ──
    await _update_session(
        session_id,
        {
            "state": "BROADCASTING",
            "confirmed_at": datetime.now(timezone.utc),
        },
    )

    # ── Step 2: Acquire global broadcast lock ─────────────────────────────────
    try:
        db = DatabaseManager.get_db()
        lock_service = DistributedLockService(db, worker_id="broadcast_worker")
        async with lock_service.lock(BROADCAST_LOCK_KEY, ttl_seconds=BROADCAST_LOCK_TTL_SECONDS):
            await _run_broadcast_loop(
                client, admin_user_id, admin_name, session_id, session
            )
    except Exception as exc:
        logger.error(
            "Fatal error during broadcast execution session=%s: %s",
            session_id, exc, exc_info=True,
        )
        await _update_session(session_id, {"state": "CANCELLED"})
        try:
            await _safe_call(
                lambda: client.send_message(
                    admin_user_id,
                    f"❌ Broadcast failed with an unexpected error: {exc}\n"
                    f"Session: <code>{session_id}</code>",
                    parse_mode="html",
                )
            )
        except Exception:
            pass


async def _run_broadcast_loop(
    client: Client,
    admin_user_id: int,
    admin_name: str,
    session_id: str,
    session: dict,
) -> None:
    """
    Inner broadcast loop, called while holding the Redis lock.

    Iterates all non-banned users and delivers broadcast content, handling
    FloodWait and per-user failures gracefully.

    Args:
        client: Pyrogram client.
        admin_user_id: Admin's Telegram user ID.
        admin_name: Admin display name for audit.
        session_id: Broadcast session ObjectId string.
        session: Session document dict from MongoDB.
    """
    from app.repositories.user_repository import UserRepository

    users_repo = UserRepository()

    # Fetch all non-banned user IDs
    try:
        all_user_ids: list[int] = await users_repo.get_all_non_banned_user_ids()
    except Exception as exc:
        logger.error(
            "Failed to fetch non-banned users for broadcast %s: %s",
            session_id, exc,
        )
        await _update_session(session_id, {"state": "CANCELLED"})
        await _safe_call(
            lambda: client.send_message(
                admin_user_id,
                "❌ Broadcast aborted: could not fetch user list. Check logs.",
            )
        )
        return

    total = len(all_user_ids)
    await _update_session(session_id, {"target_total": total})

    # Send initial progress message to admin
    progress_msg_id: Optional[int] = None
    try:
        prog_msg = await _safe_call(
            lambda: client.send_message(
                admin_user_id,
                f"📡 Broadcasting to <b>{total}</b> users...\n"
                "0 sent / 0 failed",
                parse_mode="html",
            )
        )
        progress_msg_id = prog_msg.id if prog_msg else None
    except Exception as exc:
        logger.error(
            "Failed to send progress message to admin %d: %s",
            admin_user_id, exc,
        )

    # Resolve content to forward
    messages_list = session.get("messages", [])
    if not messages_list:
        logger.error(
            "Broadcast session %s has no messages. Aborting.", session_id
        )
        await _update_session(session_id, {"state": "CANCELLED"})
        return

    msg_entry = messages_list[0]
    from_chat_id: int = msg_entry["from_chat_id"]
    message_id: int = msg_entry["message_id"]
    is_album: bool = msg_entry.get("is_album", False)

    sent = 0
    failed = 0
    
    # Use a token bucket for rate limiting to avoid flood waits
    # Rate of 20 messages per second, with a capacity of 20.
    bucket = TokenBucket(rate=20, capacity=20)

    # ── Main broadcast loop ──────────────────────────────────────────────────
    for idx, user_id in enumerate(all_user_ids):
        # Skip the broadcasting admin — they already see the content
        if user_id == admin_user_id:
            continue

        await bucket.wait_and_consume()

        try:
            if is_album:
                await _safe_call(
                    lambda uid=user_id: client.copy_media_group(
                        chat_id=uid,
                        from_chat_id=from_chat_id,
                        message_id=message_id,
                    )
                )
            else:
                await _safe_call(
                    lambda uid=user_id: client.copy_message(
                        chat_id=uid,
                        from_chat_id=from_chat_id,
                        message_id=message_id,
                    )
                )
            sent += 1

        except (
            UserIsBlocked,
            InputUserDeactivated,
            ChatWriteForbidden,
            PeerIdInvalid,
        ):
            # Expected: user blocked bot or account is gone — not a real error
            failed += 1

        except Exception as exc:
            logger.warning(
                "Broadcast to user %d failed (session=%s): %s",
                user_id, session_id, exc,
            )
            failed += 1

        # Periodic progress update
        if (idx + 1) % PROGRESS_UPDATE_INTERVAL == 0:
            try:
                await _update_session(
                    session_id,
                    {"sent_count": sent, "failed_count": failed},
                )
            except Exception:
                pass  # non-critical progress counter

            if progress_msg_id:
                try:
                    await _safe_call(
                        lambda: client.edit_message_text(
                            chat_id=admin_user_id,
                            message_id=progress_msg_id,
                            text=(
                                f"📡 Broadcasting to <b>{total}</b> users...\n"
                                f"{sent} sent / {failed} failed"
                            ),
                            parse_mode="html",
                        )
                    )
                except Exception:
                    pass  # progress edit failure is cosmetic only
    # ── End broadcast loop ───────────────────────────────────────────────────

    # Write final counts to MongoDB BEFORE sending completion message
    await _update_session(
        session_id,
        {
            "state": "COMPLETED",
            "completed_at": datetime.now(timezone.utc),
            "sent_count": sent,
            "failed_count": failed,
        },
    )

    # Emit Admin Logs + audit_logs
    await _emit_broadcast_completed_log(
        client, admin_user_id, admin_name,
        session_id, total, sent, failed,
    )

    # Notify admin of completion
    try:
        await _safe_call(
            lambda: client.send_message(
                admin_user_id,
                (
                    "✅ <b>Broadcast Complete</b>\n\n"
                    f"Total targets : {total}\n"
                    f"Sent          : {sent}\n"
                    f"Failed/Skipped: {failed}\n"
                    f"Session       : <code>{session_id}</code>"
                ),
                parse_mode="html",
            )
        )
    except Exception as exc:
        logger.error(
            "Failed to send completion notice to admin %d: %s",
            admin_user_id, exc,
        )


# ---------------------------------------------------------------------------
# Handler: /broadcast command
# ---------------------------------------------------------------------------


async def _handle_broadcast_command(client: Client, message: Message) -> None:
    """
    Handle /broadcast command from an admin in private chat.

    Validates admin role, checks for any active ongoing broadcast, cancels
    stale sessions from prior invocations, then creates a fresh MongoDB
    broadcast session and prompts the admin to send content.

    All MongoDB writes happen before any Telegram reply (restart safety).

    Args:
        client: Pyrogram client.
        message: The /broadcast command message.
    """
    if not message.from_user:
        return

    admin_user_id = message.from_user.id

    if not await _is_admin(admin_user_id):
        return  # silently ignore — do not expose admin commands to non-admins

    # Block if another broadcast is currently running
    if await _check_global_broadcast_active():
        try:
            await _safe_call(
                lambda: message.reply(
                    "⚠️ A broadcast is currently in progress.\n"
                    "Please wait for it to complete before starting a new one."
                )
            )
        except Exception as exc:
            logger.error(
                "Failed to send 'broadcast active' notice to admin %d: %s",
                admin_user_id, exc,
            )
        return

    # Cancel any stale open sessions for this admin
    await _cancel_stale_sessions(admin_user_id)

    # Create session in MongoDB BEFORE sending the Telegram reply
    try:
        session_id = await _create_session(admin_user_id)
    except Exception as exc:
        logger.error(
            "Failed to create broadcast session for admin %d: %s",
            admin_user_id, exc,
        )
        try:
            await _safe_call(
                lambda: message.reply(
                    "❌ Failed to start broadcast session. Check server logs."
                )
            )
        except Exception:
            pass
        return

    # Prompt admin to send content
    try:
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "❌ Cancel",
                    callback_data=f"broadcast:cancel:{session_id}",
                )
            ]
        ])
        await _safe_call(
            lambda: message.reply(
                "📢 <b>Broadcast Mode Active</b>\n\n"
                "Send your broadcast content now.\n"
                "Supported: text, photo, video, document, audio, voice, GIF, album.\n\n"
                "After sending, you'll see a confirmation preview.\n"
                "Send /cancel at any time to abort.\n\n"
                f"Session: <code>{session_id}</code>",
                reply_markup=keyboard,
                parse_mode="html",
            )
        )
    except Exception as exc:
        logger.error(
            "Failed to send broadcast prompt to admin %d: %s",
            admin_user_id, exc,
        )


# ---------------------------------------------------------------------------
# Handler: content collection
# ---------------------------------------------------------------------------


async def _handle_broadcast_content(client: Client, message: Message) -> None:
    """
    Collect broadcast content from an admin who has an active COLLECTING session.

    Handles three content categories:
        • /cancel command — transitions session to CANCELLED.
        • Album (media_group_id set) — buffers frames; schedules delayed flush.
        • Non-album message — saves immediately; transitions to CONFIRMING.

    MongoDB state is always updated BEFORE sending the Telegram confirmation.

    Args:
        client: Pyrogram client.
        message: Incoming message from the admin.
    """
    if not message.from_user:
        return

    admin_user_id = message.from_user.id

    # Retrieve the admin's open session
    session = await _get_admin_active_session(admin_user_id)
    if not session or session["state"] != "COLLECTING":
        return  # not in collecting phase — ignore

    session_id = str(session["_id"])

    # Handle /cancel
    if message.text and message.text.strip().lower() in ("/cancel", "/cancel@bdgwbot"):
        await _update_session(session_id, {"state": "CANCELLED"})
        try:
            await _safe_call(lambda: message.reply("✅ Broadcast cancelled."))
        except Exception as exc:
            logger.error(
                "Failed to ack cancel to admin %d: %s", admin_user_id, exc
            )
        return

    media_group_id = message.media_group_id

    if media_group_id:
        # Album frame received — buffer it and reschedule flush
        buffer_key = f"{admin_user_id}:{media_group_id}"
        if buffer_key not in _album_buffers:
            _album_buffers[buffer_key] = []
        _album_buffers[buffer_key].append(message)
        _schedule_album_flush(client, admin_user_id, session_id, buffer_key)
        return

    # Single message — determine content type label for preview
    content_type = "Text"
    if message.photo:
        content_type = "Photo"
    elif message.video:
        content_type = "Video"
    elif message.document:
        content_type = "Document"
    elif message.audio:
        content_type = "Audio"
    elif message.voice:
        content_type = "Voice"
    elif message.animation:
        content_type = "GIF"
    elif message.sticker:
        content_type = "Sticker"
    elif message.video_note:
        content_type = "Video Note"

    # Write entry to MongoDB BEFORE sending Telegram confirmation
    entry = {
        "from_chat_id": message.chat.id,
        "message_id": message.id,
        "media_group_id": None,
        "is_album": False,
        "album_size": 1,
    }
    await _append_message_entry(session_id, entry)
    await _update_session(session_id, {"state": "CONFIRMING"})

    # Send confirmation preview
    try:
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Confirm Broadcast",
                    callback_data=f"broadcast:confirm:{session_id}",
                ),
                InlineKeyboardButton(
                    "❌ Cancel",
                    callback_data=f"broadcast:cancel:{session_id}",
                ),
            ]
        ])
        await _safe_call(
            lambda: client.send_message(
                chat_id=admin_user_id,
                text=(
                    "📢 <b>Broadcast Preview</b>\n\n"
                    f"Type    : {content_type}\n"
                    f"Session : <code>{session_id}</code>\n\n"
                    "Confirm to broadcast this to all non-banned users."
                ),
                reply_markup=keyboard,
                parse_mode="html",
            )
        )
    except Exception as exc:
        logger.error(
            "Failed to send confirmation UI to admin %d: %s",
            admin_user_id, exc,
        )


# ---------------------------------------------------------------------------
# Handler: confirm callback
# ---------------------------------------------------------------------------


async def _handle_confirm(client: Client, callback: CallbackQuery) -> None:
    """
    Handle broadcast:confirm:{session_id} callback query.

    Validates the admin, validates the session state and ownership, then
    launches the broadcast execution as a background asyncio Task so the
    callback returns immediately to Telegram.

    Args:
        client: Pyrogram client.
        callback: The confirm callback query.
    """
    if not callback.from_user:
        await _ack_callback(callback, "Unknown user.", alert=True)
        return

    admin_user_id = callback.from_user.id

    if not await _is_admin(admin_user_id):
        await _ack_callback(callback, "Unauthorized.", alert=True)
        return

    parts = (callback.data or "").split(":")
    if len(parts) < 3:
        await _ack_callback(callback, "Invalid callback data.", alert=True)
        return

    session_id = parts[2]
    session = await _get_session(session_id)

    if not session:
        await _ack_callback(callback, "Session not found or expired.", alert=True)
        return

    if session["admin_user_id"] != admin_user_id:
        await _ack_callback(callback, "This session does not belong to you.", alert=True)
        return

    if session["state"] != "CONFIRMING":
        await _ack_callback(
            callback,
            f"Cannot confirm: session is in state '{session['state']}'.",
            alert=True,
        )
        return

    if not session.get("messages"):
        await _ack_callback(callback, "No content attached to this session.", alert=True)
        return

    await _ack_callback(callback, "✅ Broadcast started.")

    # Remove confirm/cancel buttons to prevent double-confirm
    try:
        await callback.message.edit_reply_markup(None)
    except Exception:
        pass

    admin_name = callback.from_user.full_name or f"Admin {admin_user_id}"

    # Launch as a background task — handler must return promptly
    asyncio.create_task(
        _execute_broadcast(client, admin_user_id, admin_name, session_id, session),
        name=f"broadcast:{session_id}",
    )


# ---------------------------------------------------------------------------
# Handler: cancel callback
# ---------------------------------------------------------------------------


async def _handle_cancel(client: Client, callback: CallbackQuery) -> None:
    """
    Handle broadcast:cancel:{session_id} callback query.

    Validates ownership, then transitions session to CANCELLED in MongoDB.
    Does NOT attempt to abort a BROADCASTING session (it cannot be interrupted
    mid-run from a callback — the lock protects it).

    Args:
        client: Pyrogram client.
        callback: The cancel callback query.
    """
    if not callback.from_user:
        await _ack_callback(callback, "Unknown user.", alert=True)
        return

    admin_user_id = callback.from_user.id

    if not await _is_admin(admin_user_id):
        await _ack_callback(callback, "Unauthorized.", alert=True)
        return

    parts = (callback.data or "").split(":")
    if len(parts) < 3:
        await _ack_callback(callback, "Invalid callback data.", alert=True)
        return

    session_id = parts[2]
    session = await _get_session(session_id)

    if not session:
        await _ack_callback(callback, "Session not found.", alert=True)
        return

    if session["admin_user_id"] != admin_user_id:
        await _ack_callback(callback, "Not your session.", alert=True)
        return

    terminal_states = {"BROADCASTING", "COMPLETED", "CANCELLED"}
    if session["state"] in terminal_states:
        await _ack_callback(
            callback,
            f"Cannot cancel: state is '{session['state']}'.",
            alert=True,
        )
        return

    await _update_session(session_id, {"state": "CANCELLED"})
    await _ack_callback(callback, "✅ Broadcast cancelled.")

    try:
        await callback.message.edit_reply_markup(None)
    except Exception:
        pass

    try:
        await _safe_call(
            lambda: callback.message.reply("❌ Broadcast session cancelled.")
        )
    except Exception as exc:
        logger.error(
            "Failed to send cancel confirmation to admin %d: %s",
            admin_user_id, exc,
        )


# ---------------------------------------------------------------------------
# Callback answer helper
# ---------------------------------------------------------------------------


async def _ack_callback(
    callback: CallbackQuery, text: str = "", alert: bool = False
) -> None:
    """
    Answer a callback query, catching and logging any error.

    Args:
        callback: The callback query to answer.
        text: Optional text shown in the Telegram toast or alert.
        alert: If True, shows as a popup alert instead of a toast.
    """
    try:
        await callback.answer(text, show_alert=alert)
    except Exception as exc:
        logger.warning("Failed to answer callback query: %s", exc)


# ---------------------------------------------------------------------------
# Custom Pyrogram filter: sender has an active COLLECTING broadcast session
# ---------------------------------------------------------------------------


async def _collecting_filter_func(_, __, message: Message) -> bool:
    """
    Custom filter function used by Pyrogram's create() to gate the content
    collector handler.

    Returns True only if:
        • Message is in a private chat.
        • Sender is an active OWNER or ADMIN.
        • Sender has a broadcast session currently in COLLECTING state.

    Args:
        _: Filter object (unused).
        __: Client (unused — admin check uses its own DB repo).
        message: Incoming message to test.

    Returns:
        True if the message should be routed to the broadcast content handler.
    """
    if not message.from_user:
        return False
    if not message.chat or message.chat.type.name != "PRIVATE":
        return False
    user_id = message.from_user.id
    if not await _is_admin(user_id):
        return False
    session = await _get_admin_active_session(user_id)
    return session is not None and session.get("state") == "COLLECTING"


collecting_filter = filters.create(_collecting_filter_func)

# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------


def register_handlers(app: Client) -> None:
    """
    Register all broadcast handlers with the Pyrogram Client instance.

    Must be called once during bot startup, AFTER:
        • MongoDB connection is established (get_database() is available).
        • hub_config is loaded (hub_config.get() works).
        • Redis connection is ready (LockService.acquire() works).

    Registered handlers (in priority order):
        1. /broadcast command — private chat, admin only
        2. Broadcast content collector — private chat, COLLECTING session active
        3. broadcast:confirm:* callback
        4. broadcast:cancel:* callback

    Args:
        app: Configured Pyrogram Client instance.
    """
 
    async def _hub_check(_, __, message) -> bool:
        from app.services.topic_manager import _get_hub_config_int
        hub_id = await _get_hub_config_int("hub_supergroup_id")
        return bool(message.chat and message.chat.id == hub_id)

    hub_filter = filters.create(_hub_check)

    app.add_handler(
        MessageHandler(
            _handle_broadcast_command,
            filters.command("broadcast") & hub_filter,
        )
    )

    # FIX: wrap _handle_broadcast_content so it ALWAYS raises StopPropagation,
    # regardless of which internal code path exits (there are ~15 bare returns).
    #
    # ROOT CAUSE: _handle_broadcast_content had no StopPropagation anywhere.
    # When an admin sent broadcast content in private chat, the handler ran and
    # returned normally. Pyrogram then continued to group=2 (submission_handler).
    # Since admins are verified creators, submission_handler processed the same
    # message as a content submission — creating a spurious moderation card in
    # the hub for every single piece of broadcast content the admin composed.
    #
    # Fix: every message that passes collecting_filter belongs exclusively to the
    # broadcast flow and must never reach any lower-priority handler group.
    async def _broadcast_content_guard(client: Client, message: Message) -> None:
        try:
            await _handle_broadcast_content(client, message)
        except (StopPropagation, ContinuePropagation):
            raise
        except Exception:
            logger.exception("_handle_broadcast_content raised unexpectedly")
        finally:
            # Always stop propagation — this message is owned by the broadcast FSM.
            raise StopPropagation

    app.add_handler(
        MessageHandler(
            _broadcast_content_guard,
            collecting_filter & filters.private,
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            _handle_confirm,
            filters.regex(r"^broadcast:confirm:[a-f0-9]{24}$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            _handle_cancel,
            filters.regex(r"^broadcast:cancel:[a-f0-9]{24}$"),
        )
    )
    logger.info(
        "Broadcast handlers registered (collection=%s, lock_key=%s).",
        BROADCAST_COLLECTION, BROADCAST_LOCK_KEY,
    )
