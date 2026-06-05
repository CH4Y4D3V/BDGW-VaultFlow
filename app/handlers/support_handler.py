# app/handlers/support_handler.py
"""
Support system handlers — Section 15 (BDGW VaultFlow Master Reference v1.0).

Implements the permanent user-topic-based support model:

  /help (+ /support, /admin aliases)
    → Checks support_sessions for an existing PENDING or ACTIVE session.
    → If none: creates PENDING session document in MongoDB FIRST (restart-safe),
      then posts the spec-compliant Section 15.2 card to the user's hub topic,
      schedules the 5-minute unattended notification timer.
    → If PENDING: directs user to wait.
    → If ACTIVE:  tells user a session is already in progress.

  support_accept callback
    → Admin-only.  Redis distributed lock prevents race conditions.
    → Updates session status from PENDING → ACTIVE in support_sessions.
    → Edits the admin topic card, notifies user, writes Admin Logs + audit.

  private_message_handler  (group=1, runs after group=0 FSM handlers)
    → Skips admins and users in active non-support FSM states.
    → If user has no PENDING/ACTIVE session: auto-opens one (trigger=unhandled).
    → Forwards every eligible DM to the user's hub topic via SupportService.
    → Logs each message to CleanupService for user-side deletion on closure.

  cmd_close_support_legacy  (/closesupport in hub)
    → Delegates to admin_handler.handle_close_command.
    → Calls CleanupService.delete_user_support_history() on success
      (this file's only direct call-site for the user-side cleanup —
       admin_handler.handle_close_command must also call it directly;
       see Dependency notes at the bottom of this file).

Security invariants:
  1. Admin messages are NEVER forwarded to user topics.
  2. Only settings.ADMIN_IDS members may accept a support session.
  3. A Redis distributed lock prevents duplicate session acceptance.
  4. MongoDB session state is written BEFORE any Telegram message is sent.
  5. All admin actions write to both audit_logs AND the Admin Logs topic.

Group-propagation note (DEPENDENCY on other handlers):
  private_message_handler sits in group=1 so FSM handlers in group=0
  (payment, takedown) can claim messages first.  For the bridge to fire
  reliably, those handlers MUST NOT raise StopPropagation when the user
  is in an IDLE / non-applicable FSM state.  If they do raise it, the
  only defence available in this file is the _user_in_active_fsm() guard
  which prevents double-processing on the infrequent paths where the
  message somehow reaches both.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from html import escape
from typing import Optional

from bson import ObjectId
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified, PeerIdInvalid
from pyrogram.types import CallbackQuery, Message

from app.config import settings
from app.core.database import DatabaseManager
from app.services.support_service import (
    build_accept_markup,
    get_support_service,
    send_admin_log_entry,
)
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Telegram retry helper
# ─────────────────────────────────────────────────────────────────────────────

async def _send_with_flood_retry(coro_factory, max_attempts: int = 3):
    """
    Execute a coroutine factory with FloodWait-aware retry.

    Args:
        coro_factory:  Zero-argument callable returning an awaitable.
        max_attempts:  Maximum attempts before the last exception is re-raised.

    Returns:
        The result of the first successful execution.

    Raises:
        The last exception if all attempts are exhausted.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            return await coro_factory()
        except FloodWait as fw:
            last_exc = fw
            if attempt == max_attempts - 1:
                break
            await asyncio.sleep(fw.value + 1)
        except Exception as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                break
            await asyncio.sleep(2 ** attempt)
    raise last_exc  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────────────────────
# Database helpers
# ─────────────────────────────────────────────────────────────────────────────

async def is_session_active(db, user_id: int) -> bool:
    """
    Return True if the user has a PENDING or ACTIVE support session.

    Queries the support_sessions collection (Section 25A.8).
    A PENDING session means a request is waiting for admin acceptance.
    An ACTIVE session means an admin is currently handling it.

    Args:
        db:       Motor database handle.
        user_id:  Telegram user ID.

    Returns:
        True if a PENDING or ACTIVE session exists, False otherwise.
    """
    doc = await db["support_sessions"].find_one(
        {"user_id": user_id, "status": {"$in": ["PENDING", "ACTIVE"]}}
    )
    return doc is not None


async def _get_active_session(db, user_id: int) -> Optional[dict]:
    """
    Return the most recent PENDING or ACTIVE session document for a user,
    or None if no such session exists.

    Args:
        db:       Motor database handle.
        user_id:  Telegram user ID.

    Returns:
        The session document dict, or None.
    """
    return await db["support_sessions"].find_one(
        {"user_id": user_id, "status": {"$in": ["PENDING", "ACTIVE"]}},
        sort=[("opened_at", -1)],
    )


async def _write_audit_log(
    db,
    action: str,
    user_id: int,
    actor_id: Optional[int],
    details: dict,
) -> None:
    """
    Write one record to the audit_logs collection (Section 25A.17).

    Per spec Section 22, all events must be written simultaneously to
    both audit_logs (MongoDB) and the Admin Logs topic (Telegram).
    This function handles the MongoDB side only; callers are responsible
    for sending the Admin Logs topic entry via send_admin_log_entry().

    Failures are silently logged — audit writes must never abort the
    primary operation.

    Args:
        db:       Motor database handle.
        action:   Action type string (see Section 22 for the full list).
        user_id:  Telegram ID of the user being acted upon.
        actor_id: Telegram ID of the admin who triggered the action
                  (None if system-triggered).
        details:  Action-specific payload dict.
    """
    doc = {
        "action": action,
        "admin_user_id": actor_id,
        "target_user_id": user_id,
        "detail": details,
        "timestamp": datetime.now(timezone.utc),
    }
    try:
        await db["audit_logs"].insert_one(doc)
    except Exception as exc:
        log.error(
            "support_audit_write_failed",
            extra={"ctx_action": action, "ctx_error": str(exc)},
        )


async def _user_in_active_fsm(db, user_id: int) -> bool:
    """
    Return True if the user is currently inside an active non-support FSM flow.

    Checks:
      - An unexpired payment session (payment collection, Section 25A.4).
      - A generic FSM state document (fsm_states collection) where state
        is not IDLE/null — used by takedown, onboarding, and similar flows.

    This guard prevents the support message bridge from double-processing
    messages that are actively owned by another flow.

    Args:
        db:       Motor database handle.
        user_id:  Telegram user ID.

    Returns:
        True if an active non-support FSM state is detected.
    """
    now = datetime.now(timezone.utc)

    # Active payment session
    payment_session = await db["payment_sessions"].find_one({
        "user_id": user_id,
        "status": "ACTIVE",
        "expires_at": {"$gt": now},
    })
    if payment_session:
        return True

    # Generic FSM state (takedown, onboarding, etc.)
    fsm_state = await db["fsm_states"].find_one({
        "user_id": user_id,
        "state": {"$nin": [None, "", "IDLE"]},
    })
    if fsm_state:
        return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# 5-minute unattended timer  (Section 15.3)
# ─────────────────────────────────────────────────────────────────────────────

async def _unattended_check(
    client: Client, user_id: int, session_id: str
) -> None:
    """
    Background coroutine: fire a "no admin available" notification if the
    support session is still PENDING after 5 minutes (Section 15.3).

    The notification fires exactly once per session (guarded by the
    notified_unattended flag in support_sessions).

    Args:
        client:     Active Pyrogram client.
        user_id:    Telegram user ID.
        session_id: String representation of the session's MongoDB ObjectId.

    Note:
        This task is NOT restart-safe on its own. If the bot restarts while
        this task is pending, the timer is lost. Restart recovery must
        re-schedule this task for all sessions where:
          status=PENDING AND notified_unattended=False AND
          (opened_at + 5min) > now.
        That recovery logic belongs in the startup/restart handler,
        not in this file.
    """
    await asyncio.sleep(300)  # 5 minutes

    db = DatabaseManager.get_db()
    try:
        session = await db["support_sessions"].find_one({
            "_id": ObjectId(session_id),
            "user_id": user_id,
            "status": "PENDING",
            "notified_unattended": {"$ne": True},
        })
        if not session:
            return  # Accepted or closed already — nothing to do.

        # Mark as notified BEFORE sending the Telegram message (restart-safe).
        await db["support_sessions"].update_one(
            {"_id": ObjectId(session_id)},
            {"$set": {"notified_unattended": True}},
        )

        await _send_with_flood_retry(
            lambda: client.send_message(
                chat_id=user_id,
                text=(
                    "ℹ️ No admin is currently available.\n"
                    "Your request has been noted. "
                    "An admin will respond when available."
                ),
                parse_mode=ParseMode.HTML,
            )
        )
    except PeerIdInvalid:
        log.warning(
            "unattended_notify_user_unreachable",
            extra={"ctx_user_id": user_id},
        )
    except Exception as exc:
        log.warning(
            "unattended_notify_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )


# ─────────────────────────────────────────────────────────────────────────────
# Internal: create and open a support session
# ─────────────────────────────────────────────────────────────────────────────

async def _open_support_session(
    client: Client,
    user_id: int,
    from_user,
    trigger: str,
    message: Optional[Message] = None,
) -> None:
    """
    Create a new PENDING support session and post the spec card to hub topic.

    Invariant: MongoDB write occurs BEFORE any Telegram message is sent
    (Section 25 — restart safety).

    Steps:
      1. Get/create the user's permanent topic.
      2. Write the support_sessions document with status=PENDING.
      3. Build and post the Section 15.2 card with Accept button to topic.
      4. Send confirmation DM to user.
      5. Write audit log + Admin Logs entry.
      6. Schedule the 5-minute unattended check task.

    Args:
        client:    Active Pyrogram client.
        user_id:   Telegram user ID of the requesting user.
        from_user: Pyrogram User object for card building.
        trigger:   "manual" | "takedown_rejection" | "unhandled"
        message:   Optional originating Message for text preview in card.
    """
    db = DatabaseManager.get_db()
    support_service = get_support_service()
    topic_manager = get_topic_manager()

    # Step 1 — resolve permanent topic ------------------------------------
    topic_id: int = await topic_manager.get_or_create_user_topic(client, user_id)

    # Step 2 — write session to MongoDB BEFORE any Telegram action ---------
    now = datetime.now(timezone.utc)
    result = await db["support_sessions"].insert_one({
        "user_id": user_id,
        "topic_id": topic_id,
        "status": "PENDING",
        "trigger": trigger,
        "opened_at": now,
        "accepted_by": None,
        "accepted_at": None,
        "closed_by": None,
        "closed_at": None,
        "notified_unattended": False,
    })
    session_id = str(result.inserted_id)

    # Step 3 — build and post the support card to hub topic ----------------
    try:
        card_text = await support_service.build_user_support_card(
            db=db,
            user_id=user_id,
            from_user=from_user,
            message=message,
        )
        await support_service.notify_to_topic(
            client=client,
            user_id=user_id,
            text=card_text,
            reply_markup=build_accept_markup(user_id),
        )
    except Exception as exc:
        log.error(
            "support_card_post_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )

    # Step 4 — DM confirmation to user ------------------------------------
    try:
        await _send_with_flood_retry(
            lambda: client.send_message(
                chat_id=user_id,
                text=(
                    "✅ Support request received. "
                    "Our team will respond as soon as possible."
                ),
                parse_mode=ParseMode.HTML,
            )
        )
    except PeerIdInvalid:
        log.warning("support_open_user_unreachable", extra={"ctx_user_id": user_id})
    except Exception as exc:
        log.warning(
            "support_open_user_dm_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )

    # Step 5 — audit log + Admin Logs topic --------------------------------
    await _write_audit_log(
        db=db,
        action="SUPPORT_SESSION_OPENED",
        user_id=user_id,
        actor_id=user_id,
        details={"session_id": session_id, "topic_id": topic_id, "trigger": trigger},
    )
    # Admin Logs entry: "SUPPORT REQUEST" is informational, not a named
    # action in Section 9.4, but we emit it for traceability.
    await send_admin_log_entry(
        client=client,
        action_type="SUPPORT REQUEST OPENED",
        admin_user_id=None,
        admin_name="System",
        target_user_id=user_id,
        target_name=getattr(from_user, "full_name", None),
        target_username=getattr(from_user, "username", None),
        detail=f"trigger={trigger}, session_id={session_id}",
    )

    # Step 6 — schedule unattended check ----------------------------------
    asyncio.create_task(
        _unattended_check(client, user_id, session_id),
        name=f"unattended:{user_id}:{session_id}",
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

    For admins: returns a short informational reply (not forwarded).
    For users:
      - ACTIVE session  → advise user to wait; their admin is responding.
      - PENDING session → advise user their request is queued.
      - No session      → create PENDING session (MongoDB first), post card,
                          schedule 5-minute unattended timer.

    Args:
        client:  Active Pyrogram client.
        message: The incoming /help (or alias) command message.
    """
    db = DatabaseManager.get_db()
    user_id = message.from_user.id

    # Admins get a short hint and nothing else.
    if user_id in settings.ADMIN_IDS:
        await _send_with_flood_retry(
            lambda: message.reply(
                "ℹ️ You are an admin. Use <code>/close</code> inside a "
                "user topic to manage support sessions.",
                parse_mode=ParseMode.HTML,
            )
        )
        return

    # Check for an existing open session.
    existing_session = await _get_active_session(db, user_id)
    if existing_session:
        status = existing_session.get("status", "PENDING")
        if status == "ACTIVE":
            reply_text = (
                "✅ You have an active support session. "
                "An admin is reviewing your request — please reply here."
            )
        else:
            reply_text = (
                "⏳ Your support request is already queued. "
                "An admin will accept it shortly."
            )
        try:
            await _send_with_flood_retry(
                lambda: message.reply(reply_text, parse_mode=ParseMode.HTML)
            )
        except Exception as exc:
            log.warning(
                "support_cmd_existing_reply_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )
        return

    # No open session — create one.
    try:
        await _open_support_session(
            client=client,
            user_id=user_id,
            from_user=message.from_user,
            trigger="manual",
            message=message,
        )
    except Exception as exc:
        log.error(
            "support_cmd_open_session_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            exc_info=True,
        )
        try:
            await _send_with_flood_retry(
                lambda: message.reply(
                    "⚠️ Could not open a support request right now. "
                    "Please try again in a moment.",
                    parse_mode=ParseMode.HTML,
                )
            )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Callback: admin accepts a support session
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^support_accept:(\d+)$"))
async def handle_support_accept(client: Client, callback: CallbackQuery) -> None:
    """
    Admin callback: accept a PENDING support session.

    State machine: PENDING → ACTIVE.

    Security:
      - Only members of settings.ADMIN_IDS may accept.
      - A Redis distributed lock (lock:support:accept:{user_id}) prevents
        two simultaneous accepts from racing on the DB write.

    On success:
      - Updates support_sessions: status=ACTIVE, accepted_by, accepted_at.
      - Edits the admin topic card (removes Accept button, adds accepted-by note).
      - Sends a DM to the user confirming acceptance.
      - Writes to audit_logs (MongoDB).
      - Writes to Admin Logs topic (Telegram, Section 9.4).

    Args:
        client:   Active Pyrogram client.
        callback: The callback query from the admin's button press.
    """
    user_id = int(callback.data.split(":")[1])
    actor_id = callback.from_user.id
    db = DatabaseManager.get_db()

    # Admin authority check ------------------------------------------------
    if actor_id not in settings.ADMIN_IDS:
        await callback.answer("Not authorised.", show_alert=True)
        return

    # Distributed lock to prevent race on duplicate accepts ----------------
    redis_client = None
    lock_key = f"lock:support:accept:{user_id}"
    lock_acquired = False

    try:
        from app.core.redis import get_redis_client
        redis_client = await get_redis_client()
        lock_acquired = await redis_client.set(lock_key, "1", ex=30, nx=True)
    except Exception as exc:
        log.warning(
            "support_accept_redis_unavailable",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )
        # Proceed without lock; log the risk but do not silently block the accept.
        lock_acquired = True

    if not lock_acquired:
        await callback.answer(
            "This session is already being accepted by another admin.",
            show_alert=True,
        )
        return

    try:
        # Idempotency / ownership check ------------------------------------
        session = await db["support_sessions"].find_one(
            {"user_id": user_id, "status": {"$in": ["PENDING", "ACTIVE"]}}
        )
        if not session:
            await callback.answer(
                "No open session found for this user.", show_alert=True
            )
            return

        if session.get("status") == "ACTIVE":
            accepted_by_name = session.get("accepted_by_name", "another admin")
            await callback.answer(
                f"Session already accepted by {accepted_by_name}.",
                show_alert=True,
            )
            return

        admin_name = escape(callback.from_user.full_name or "Admin")
        now = datetime.now(timezone.utc)

        # Restart-safe: write MongoDB BEFORE any Telegram message ----------
        await db["support_sessions"].update_one(
            {"_id": session["_id"]},
            {
                "$set": {
                    "status": "ACTIVE",
                    "accepted_by": actor_id,
                    "accepted_by_name": admin_name,
                    "accepted_at": now,
                }
            },
        )

        # Edit the admin topic card ----------------------------------------
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
        except Exception as exc:
            log.warning(
                "support_accept_edit_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )

        # DM the user ------------------------------------------------------
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
        except Exception as exc:
            log.warning(
                "support_accept_user_dm_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )

        # Audit log (MongoDB) ----------------------------------------------
        await _write_audit_log(
            db=db,
            action="SUPPORT_ACCEPTED",
            user_id=user_id,
            actor_id=actor_id,
            details={
                "session_id": str(session["_id"]),
                "accepted_by_name": admin_name,
            },
        )

        # Admin Logs topic (Telegram, Section 9.4) -------------------------
        # Fetch target user details for the log entry.
        target_doc = await db["users"].find_one({"user_id": user_id}) or {}
        await send_admin_log_entry(
            client=client,
            action_type="SUPPORT ACCEPTED",
            admin_user_id=actor_id,
            admin_name=admin_name,
            target_user_id=user_id,
            target_name=target_doc.get("full_name", "Unknown"),
            target_username=target_doc.get("username"),
            detail=f"session_id={session['_id']}",
        )

        await callback.answer("Session accepted.")

    except Exception as exc:
        log.error(
            "support_accept_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            exc_info=True,
        )
        await callback.answer(
            "An error occurred. Please try again.", show_alert=True
        )
    finally:
        if redis_client and lock_acquired:
            try:
                await redis_client.delete(lock_key)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# Private message bridge  (Section 15.4)
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(
    (
        filters.text
        | filters.photo
        | filters.video
        | filters.document
        | filters.audio
        | filters.voice
        | filters.video_note
        | filters.sticker
        | filters.animation
    )
    & filters.private
    & ~filters.command([]),
    group=1,
    # IMPORTANT — group=1 intentionally runs AFTER group=0 (FSM handlers).
    # For this handler to receive messages reliably, FSM handlers in group=0
    # (takedown, payment) MUST NOT raise StopPropagation when the user is in
    # an IDLE/non-applicable state.  See Dependency notes at the top of file.
)
async def private_message_handler(client: Client, message: Message) -> None:
    """
    Route all private non-command DMs to the user's permanent hub topic.

    Admin messages are NEVER forwarded.

    If the user is in an active non-support FSM flow (payment session,
    takedown FSM) the message is skipped — it belongs to that flow.

    If there is no open support session, one is auto-opened with
    trigger="unhandled" before the message is forwarded.

    Args:
        client:  Active Pyrogram client.
        message: The incoming private message.
    """
    user_id = message.from_user.id

    # Never forward admin messages to user topics.
    if user_id in settings.ADMIN_IDS:
        return

    db = DatabaseManager.get_db()

    # Bail out if user is actively inside another FSM flow.
    try:
        in_fsm = await _user_in_active_fsm(db, user_id)
        if in_fsm:
            return
    except Exception as exc:
        log.warning(
            "support_bridge_fsm_check_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )

    try:
        # Auto-open a session if none exists (trigger=unhandled).
        existing = await _get_active_session(db, user_id)
        if existing is None:
            await _open_support_session(
                client=client,
                user_id=user_id,
                from_user=message.from_user,
                trigger="unhandled",
                message=message,
            )

        # Forward the message to the hub topic.
        support_service = get_support_service()
        await support_service.handle_user_message(client, message)

    except Exception as exc:
        log.error(
            "support_message_bridge_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
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

    Only members of settings.ADMIN_IDS may invoke this.  Delegates to
    the canonical close handler in admin_handler.py, then immediately
    triggers CleanupService.delete_user_support_history() so user-side
    support messages are silently deleted (Section 20, Rule 5 and
    Section 15.5).

    DEPENDENCY: admin_handler.handle_close_command() must ALSO call
    CleanupService.delete_user_support_history() directly, because the
    primary /close command is registered there and this alias only covers
    /closesupport.

    Args:
        client:  Active Pyrogram client.
        message: The /closesupport command message.
    """
    actor_id = message.from_user.id

    if actor_id not in settings.ADMIN_IDS:
        return  # Silently ignore non-admins.

    # Resolve the user_id from the topic this command was sent in.
    # The canonical close handler uses message_thread_id → user_topics lookup.
    user_id: Optional[int] = None
    try:
        from app.handlers.admin_handler import handle_close_command
        await handle_close_command(client, message)

        # After close: resolve user_id from the topic mapping so we can
        # trigger the user-side message cleanup.
        if message.message_thread_id:
            db = DatabaseManager.get_db()
            topic_doc = await db["user_topics"].find_one(
                {"topic_id": message.message_thread_id}
            )
            if topic_doc:
                user_id = topic_doc.get("user_id")

    except Exception as exc:
        log.error(
            "support_close_legacy_delegate_failed",
            extra={"ctx_actor_id": actor_id, "ctx_error": str(exc)},
            exc_info=True,
        )
        return

    # Trigger user-side cleanup (Section 20 Rule 5, Section 15.5).
    if user_id is not None:
        try:
            from app.services.cleanup_service import get_cleanup_service
            cleanup = get_cleanup_service()
            deleted = await cleanup.delete_user_support_history(user_id)
            log.info(
                "support_close_cleanup_done",
                extra={"ctx_user_id": user_id, "ctx_deleted": deleted},
            )
        except Exception as exc:
            log.error(
                "support_close_cleanup_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )
    else:
        log.warning(
            "support_close_cleanup_skipped_no_user_id",
            extra={"ctx_thread_id": message.message_thread_id},
        )


# ─────────────────────────────────────────────────────────────────────────────
# DEPENDENCY NOTES (files not in scope of this session):
#
# 1. admin_handler.handle_close_command  [HIGH PRIORITY]
#    Must call `await get_cleanup_service().delete_user_support_history(user_id)`
#    immediately after updating the session to CLOSED.
#    Without this, the primary /close command leaves user-side messages intact.
#
# 2. FSM handlers in group=0 (takedown_handler.py, payment_handler.py)
#    Must NOT raise StopPropagation when the user is in IDLE / non-applicable
#    state.  They should either return cleanly or raise ContinuePropagation so
#    that private_message_handler in group=1 receives the update.
#
# 3. Restart recovery (startup handler / scheduler)
#    On bot restart, must re-schedule _unattended_check tasks for all
#    support_sessions where:
#      status=PENDING AND notified_unattended=False AND
#      (opened_at + 5 min) > now
#    and must also call delete_user_support_history for any sessions
#    that were CLOSED while the bot was offline but cleanup never ran.
# ─────────────────────────────────────────────────────────────────────────────
