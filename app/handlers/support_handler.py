#app/handlers/support_handler.py
"""
Support system handlers for BDGW VaultFlow.

Implements the permanent user-topic-based support model from spec Section 15:
  - /help (and legacy /admin, /support) → opens or continues the user's
    permanent Verification Hub topic.
  - Admin accept callback → locks session, sets status=accepted, notifies user.
  - Private message bridge → forwards all non-admin, non-command DMs to the
    user's permanent topic via SupportService.
  - /closesupport (legacy admin alias) → delegates to admin_handler.

Security invariants enforced here:
  1. Admin messages are NEVER forwarded to user topics.
  2. Only ADMIN_IDS members may accept a support session.
  3. A Redis distributed lock prevents duplicate session acceptance.
  4. MongoDB session state is written BEFORE any Telegram message is sent
     (restart-safety, spec Section 25).
  5. All admin actions are written to audit_logs.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from html import escape
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified, PeerIdInvalid
from pyrogram.types import CallbackQuery, Message

from app.config import settings
from app.core.database import DatabaseManager
from app.services.support_service import build_accept_markup, get_support_service
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _send_with_flood_retry(coro_factory, max_attempts: int = 3):
    """
    Execute a coroutine factory (callable returning a coroutine) with
    FloodWait retry logic.

    Args:
        coro_factory: Zero-argument callable that returns an awaitable.
        max_attempts: Maximum number of attempts before re-raising.

    Returns:
        The result of the successful coroutine.

    Raises:
        The last exception if all attempts are exhausted.
    """
    for attempt in range(max_attempts):
        try:
            return await coro_factory()
        except FloodWait as fw:
            if attempt == max_attempts - 1:
                raise
            await asyncio.sleep(fw.value + 1)
        except Exception:
            if attempt == max_attempts - 1:
                raise
            await asyncio.sleep(2 ** attempt)


async def is_session_active(db, user_id: int) -> bool:
    """
    Return True if the user has an open (non-closed) support session.

    A document that exists but has no "status" field is treated as
    inactive rather than active (defensive default).
    """
    doc = await db["user_topics"].find_one({"user_id": user_id})
    if not doc:
        return False
    status = doc.get("status")
    if not status or status == "closed":
        return False
    return True


async def _write_audit_log(
    db,
    action: str,
    user_id: int,
    actor_id: Optional[int],
    details: dict,
) -> None:
    """
    Write one audit record to the audit_logs collection.

    Failures are silently logged — audit writes must never abort the
    primary operation.
    """
    doc = {
        "action": action,
        "user_id": user_id,
        "actor_id": actor_id,
        "details": details,
        "timestamp": datetime.now(timezone.utc),
        "source": "support_handler",
    }
    try:
        await db["audit_logs"].insert_one(doc)
    except Exception as e:
        log.error(
            "support_audit_write_failed",
            extra={"ctx_action": action, "ctx_error": str(e)},
        )


# ─────────────────────────────────────────────────────────────────────────────
# Command handlers
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(
    filters.command(["admin", "support", "help"]) & filters.private
)
async def cmd_support(client: Client, message: Message) -> None:
    """
    Entry point for the support system (/help, /support, /admin).

    For admins: returns a short informational reply.
    For users:
      - If no active session → creates a pending session record in MongoDB
        FIRST, then sends confirmations and notifies the admin topic.
      - If session already active → directs user to wait.

    MongoDB write happens before any Telegram send to satisfy the
    restart-safety requirement (spec Section 25).
    """
    db = DatabaseManager.get_db()
    user_id = message.from_user.id

    if user_id in settings.ADMIN_IDS:
        await _send_with_flood_retry(
            lambda: message.reply(
                "ℹ️ You are an admin. Use <code>/close</code> "
                "inside a user topic to manage sessions.",
                parse_mode=ParseMode.HTML,
            )
        )
        return

    session_active = await is_session_active(db, user_id)

    if session_active:
        await _send_with_flood_retry(
            lambda: message.reply(
                "⚠️ You already have an active support session. "
                "An admin will respond shortly.",
                parse_mode=ParseMode.HTML,
            )
        )
        return

    # ── Restart-safe: write DB state BEFORE sending any Telegram message ──
    now = datetime.now(timezone.utc)
    await db["user_topics"].update_one(
        {"user_id": user_id},
        {
            "$set": {
                "status": "pending",
                "updated_at": now,
                "last_activity_at": now,
            },
            "$setOnInsert": {
                "user_id": user_id,
                "created_at": now,
            },
        },
        upsert=True,
    )

    # ── Ensure permanent topic exists ──────────────────────────────────────
    topic_manager = get_topic_manager()
    topic_id = await topic_manager.get_or_create_user_topic(client, user_id)

    # ── Notify user ────────────────────────────────────────────────────────
    try:
        await _send_with_flood_retry(
            lambda: message.reply(
                "✅ Support request received. Our team will respond as soon as possible.",
                parse_mode=ParseMode.HTML,
            )
        )
    except Exception as e:
        log.error(
            "support_user_reply_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    # ── Notify admin topic ─────────────────────────────────────────────────
    user = message.from_user
    user_link = f"tg://user?id={user_id}"
    safe_name = escape(user.full_name or "Unknown")
    safe_username = escape(user.username or "no_username")

    accept_notice_text = (
        f"📩 <b>Support Request</b>\n\n"
        f"👤 <a href='{user_link}'>{safe_name}</a> "
        f"(@{safe_username}) "
        f"[<code>{user_id}</code>]\n\n"
        f"User is waiting for help.\n\n"
        f"👇 Click below to accept:"
    )

    try:
        support_service = get_support_service()
        await support_service.notify_to_topic(
            client=client,
            user_id=user_id,
            text=accept_notice_text,
            reply_markup=build_accept_markup(user_id),
        )
    except Exception as e:
        log.error(
            "support_topic_notify_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    await _write_audit_log(
        db=db,
        action="SUPPORT_SESSION_OPENED",
        user_id=user_id,
        actor_id=user_id,
        details={"topic_id": topic_id},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Callback: admin accepts a support session
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^support_accept:(\d+)$"))
async def handle_support_accept(client: Client, callback: CallbackQuery) -> None:
    """
    Admin callback to accept a pending support session.

    Security:
      - Only ADMIN_IDS members may accept.
      - A Redis distributed lock (lock:support:accept:{user_id}) prevents
        two simultaneous accepts from racing on the DB write.

    State machine:
      pending → accepted

    Post-accept:
      - Edits the admin topic message to record who accepted.
      - Sends a DM to the user confirming acceptance.
      - Writes to audit_logs.
    """
    user_id = int(callback.data.split(":")[1])
    actor_id = callback.from_user.id
    db = DatabaseManager.get_db()

    # ── Admin authority check ──────────────────────────────────────────────
    if actor_id not in settings.ADMIN_IDS:
        await callback.answer("Not authorised.", show_alert=True)
        return

    # ── Distributed lock to prevent duplicate accepts ──────────────────────
    redis_client = None
    lock_key = f"lock:support:accept:{user_id}"
    lock_acquired = False

    try:
        from app.core.redis import get_redis_client
        redis_client = await get_redis_client()
        lock_acquired = await redis_client.set(lock_key, "1", ex=30, nx=True)
    except Exception as e:
        log.warning(
            "support_accept_redis_unavailable",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        # Proceed without lock — log the risk; do NOT silently fail the accept.
        lock_acquired = True

    if not lock_acquired:
        await callback.answer(
            "This session is already being accepted.", show_alert=True
        )
        return

    try:
        # ── Idempotency check ──────────────────────────────────────────────
        doc = await db["user_topics"].find_one({"user_id": user_id})
        if not doc or doc.get("status") == "accepted":
            await callback.answer(
                "This ticket is already accepted or invalid.", show_alert=True
            )
            return

        admin_name = escape(callback.from_user.first_name or "Admin")
        now = datetime.now(timezone.utc)

        # ── Restart-safe: write MongoDB BEFORE sending any Telegram message ─
        await db["user_topics"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": "accepted",
                    "accepted_at": now,
                    "accepted_by": actor_id,
                    "accepted_by_name": admin_name,
                    "updated_at": now,
                }
            },
        )

        # ── Edit admin topic message ───────────────────────────────────────
        try:
            existing_text = callback.message.text or ""
            await _send_with_flood_retry(
                lambda: callback.message.edit_text(
                    f"{existing_text}\n\n✅ <b>Accepted by {admin_name}</b>",
                    reply_markup=None,
                    parse_mode=ParseMode.HTML,
                )
            )
        except MessageNotModified:
            pass
        except Exception as e:
            log.warning(
                "support_accept_edit_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        # ── Notify user ────────────────────────────────────────────────────
        try:
            await _send_with_flood_retry(
                lambda: client.send_message(
                    chat_id=user_id,
                    text=(
                        "✅ An admin has accepted your support request. "
                        "You can now chat freely."
                    ),
                    parse_mode=ParseMode.HTML,
                )
            )
        except PeerIdInvalid:
            log.warning(
                "support_accept_user_unreachable",
                extra={"ctx_user_id": user_id},
            )
        except Exception as e:
            log.warning(
                "support_accept_user_notify_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            )

        # ── Audit log ──────────────────────────────────────────────────────
        await _write_audit_log(
            db=db,
            action="SUPPORT_SESSION_ACCEPTED",
            user_id=user_id,
            actor_id=actor_id,
            details={"accepted_by_name": admin_name},
        )

        await callback.answer("Session accepted.")

    except Exception as e:
        log.error(
            "support_accept_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )
        await callback.answer("An error occurred. Please try again.", show_alert=True)
    finally:
        if redis_client and lock_acquired:
            try:
                await redis_client.delete(lock_key)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# Private message bridge
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(
    (
        filters.text
        | filters.photo
        | filters.video
        | filters.document
    )
    & filters.private
    & ~filters.command([]),
    group=1,
)
async def private_message_handler(client: Client, message: Message) -> None:
    """
    Routes all private non-command messages to the user's permanent topic
    via SupportService.handle_user_message().

    Admin messages are explicitly excluded: admin PMs must never be
    forwarded to a user's support topic.
    """
    user_id = message.from_user.id

    # ── Never forward admin messages to user topics ────────────────────────
    if user_id in settings.ADMIN_IDS:
        return

    try:
        support_service = get_support_service()
        await support_service.handle_user_message(client, message)
    except Exception as e:
        log.error(
            "support_message_bridge_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
            exc_info=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Legacy admin close alias
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(
    filters.command("closesupport")
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
async def cmd_close_support_legacy(client: Client, message: Message) -> None:
    """
    Legacy alias for /close in the Verification Hub.

    Only ADMIN_IDS members may invoke this command.  Delegates to the
    canonical close handler in admin_handler.py.
    """
    if message.from_user.id not in settings.ADMIN_IDS:
        return  # Silently ignore non-admins

    try:
        from app.handlers.admin_handler import handle_close_command
        await handle_close_command(client, message)
    except Exception as e:
        log.error(
            "support_close_legacy_failed",
            extra={
                "ctx_user_id": message.from_user.id,
                "ctx_error": str(e),
            },
            exc_info=True,
        )