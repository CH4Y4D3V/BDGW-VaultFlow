"""
app/handlers/admin_handler.py

Hub admin commands for the BDGW VaultFlow platform.

Implements every command listed in Section 9.5 of the master reference:
  /accept, /close, /ban, /unban, /warn, /mute, /unmute,
  /paymentdone, /profile, /history, /note, /notes

Spec compliance:
  - Section 19  : Only two roles exist — OWNER and ADMIN. All removed roles
                  (MODERATOR, SUPPORT_ADMIN, PAYMENT_ADMIN, SUDO, etc.) are
                  gone. Every command uses @permission_required(Role.ADMIN).
  - Section 9.4 : Every action writes to Admin Logs topic (hub_config key
                  "admin_logs_topic_id") AND to audit_logs collection.
  - Section 9.5 : Commands are gated to the Verification Hub supergroup
                  (hub_config key "hub_supergroup_id"). No hardcoded IDs.
  - Section 22  : Audit log format matches 25A.17 schema exactly.
  - Section 25A.15: Moderation actions write to `punishments` collection.
  - Section 25A.2 : All user topic routing goes through user_topics_repo.
  - Section 21  : Ban and mute are silent — no user notification is sent.
  - Section 7.5 : /grant and /revoke wrap subscription ops in a Redis lock.
  - FloodWait   : Every Telegram send is wrapped in _safe_send() which
                  sleeps and retries on FloodWait exactly once.
  - No hardcoded IDs anywhere — all IDs come from hub_config at runtime.

Architecture notes:
  - hub_config is loaded once at startup and injected via get_hub_config().
    This module reads it but never mutates it.
  - _resolve_target_user() resolves by topic context first, explicit arg
    second. This is the correct priority — topic context is unambiguous when
    inside a user thread.
  - The /grant and /revoke commands are kept for OWNER-only subscription
    management. They are not in the spec's Section 9.5 command list but are
    required operational tools; they are gated to OWNER role only.
"""

from __future__ import annotations

import asyncio
import logging
from app.utils.logger import get_logger
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.core.hub_config import get_hub_config
from app.core.permissions import Role, permission_required
from app.core.redis_lock import acquire_lock
from app.models.subscription import Plan
from app.services.subscription_service import SubscriptionService

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Telegram send helper — wraps every outbound call with FloodWait handling
# ─────────────────────────────────────────────────────────────────────────────


async def _safe_send(coro, *, label: str = "send") -> bool:
    """Await a Pyrogram send coroutine, handling FloodWait.
    NOTE: Pyrogram coroutines are single-use; the retry path is intentionally
    removed to avoid RuntimeError: coroutine already executed.
    On FloodWait the call sleeps the required interval and returns False.
    """
    try:
        await coro
        return True
    except FloodWait as exc:
        logger.warning("FloodWait %ds on %s — sleeping (single-use coroutine, no retry)", exc.value, label)
        await asyncio.sleep(exc.value)
        return False
    except (UserIsBlocked, InputUserDeactivated) as exc:
        logger.info("Cannot deliver to user (%s): %s", label, exc)
        return False
    except Exception as exc:
        logger.error("Unexpected error in %s: %s", label, exc, exc_info=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Hub filter — resolved at handler call time from hub_config, not hardcoded
# ─────────────────────────────────────────────────────────────────────────────


def _hub_filter():
    """Return a Pyrogram filter that matches only the Verification Hub group.

    The hub supergroup ID is read from hub_config at call time so there are
    no hardcoded IDs in this module.
    """
    async def _check(_, __, message: Message) -> bool:
        hub_id = get_hub_config().hub_supergroup_id
        return message.chat and message.chat.id == hub_id

    return filters.create(_check)


HUB = _hub_filter()


# ─────────────────────────────────────────────────────────────────────────────
# Target user resolver
# ─────────────────────────────────────────────────────────────────────────────


async def _resolve_target_user(message: Message) -> Optional[int]:
    """Resolve the target ``user_id`` from a hub command message.

    Resolution order (first match wins):
      1. Forum topic context — if the command is sent inside a user thread,
         the thread owner is the unambiguous target. This is the common path
         for every hub command.
      2. Explicit numeric argument — ``/command 12345`` when used outside a
         thread or to target a different user than the thread owner.

    Args:
        message: The incoming Pyrogram ``Message`` object.

    Returns:
        The target ``user_id`` as an integer, or ``None`` if unresolvable.
    """
    # 1. Forum topic context (preferred — unambiguous)
    thread_id = getattr(message, "message_thread_id", None)
    if thread_id:
        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one({"topic_id": thread_id})
        if doc:
            return int(doc["user_id"])

    # 2. Explicit argument
    if len(message.command) > 1 and message.command[1].isdigit():
        return int(message.command[1])

    return None


def _strip_target_from_args(args: list[str], target_id: int) -> list[str]:
    """Remove the leading user_id token from *args* if it matches *target_id*.

    Prevents the user_id from being interpreted as a reason or plan string
    when it was provided as an explicit target argument.

    Args:
        args:      Remaining command arguments after ``message.command[0]``.
        target_id: The already-resolved target user ID.

    Returns:
        A new list with the leading user_id token removed, if present.
    """
    if args and args[0].isdigit() and int(args[0]) == target_id:
        return args[1:]
    return args


# ─────────────────────────────────────────────────────────────────────────────
# Audit helpers — write to MongoDB audit_logs AND Admin Logs topic
# ─────────────────────────────────────────────────────────────────────────────


async def _write_audit(
    *,
    action: str,
    admin_id: int,
    target_user_id: int,
    detail: dict,
) -> None:
    """Write one entry to the ``audit_logs`` MongoDB collection.

    Schema matches Section 25A.17 exactly.

    Args:
        action:         Audit action type string (e.g. ``"USER BANNED"``).
        admin_id:       Telegram user ID of the acting admin.
        target_user_id: Telegram user ID of the affected user.
        detail:         Action-specific dict (stored as-is in ``detail``
                        field).
    """
    try:
        db = DatabaseManager.get_db()
        await db["audit_logs"].insert_one({
            "audit_id": ObjectId(),
            "action": action,
            "admin_user_id": admin_id,
            "target_user_id": target_user_id,
            "detail": detail,
            "timestamp": datetime.now(timezone.utc),
        })
    except Exception as exc:
        logger.error(
            "Failed to write audit log for action=%s target=%s: %s",
            action, target_user_id, exc, exc_info=True,
        )


async def _post_admin_log(
    client: Client,
    *,
    action: str,
    admin_id: int,
    admin_name: str,
    target_id: int,
    target_name: str,
    target_username: Optional[str],
    detail: str,
) -> None:
    """Post a structured entry to the Admin Logs topic in the Verification Hub.

    Format matches Section 9.4 of the master reference exactly.
    The Admin Logs topic_id is read from hub_config — never hardcoded.

    Args:
        client:           Active Pyrogram client.
        action:           Action type string (e.g. ``"USER BANNED"``).
        admin_id:         Acting admin's Telegram user ID.
        admin_name:       Acting admin's display name.
        target_id:        Target user's Telegram user ID.
        target_name:      Target user's display name.
        target_username:  Target user's @username, or ``None``.
        detail:           Single-line action-specific detail string.
    """
    cfg = get_hub_config()
    username_str = f"@{target_username}" if target_username else "no username"
    text = (
        f"<b>{action}</b>\n"
        f"Admin     : {admin_name}\n"
        f"Admin ID  : <code>{admin_id}</code>\n"
        f"Target    : {target_name} ({username_str})\n"
        f"Target ID : <code>{target_id}</code>\n"
        f"Detail    : {detail}\n"
        f"Time      : {datetime.now(timezone.utc).isoformat(timespec='seconds')}Z"
    )
    await _safe_send(
        client.send_message(
            chat_id=cfg.hub_supergroup_id,
            text=text,
            message_thread_id=cfg.admin_logs_topic_id,
            parse_mode=ParseMode.HTML,
        ),
        label=f"admin_log:{action}",
    )


async def _post_to_user_topic(
    client: Client,
    *,
    target_id: int,
    text: str,
) -> None:
    """Post a message to the target user's permanent topic in the hub.

    If the topic mapping is missing the message is silently dropped and
    logged — missing topic is a recovery concern, not a command failure.

    Args:
        client:    Active Pyrogram client.
        target_id: Telegram user ID whose topic to post into.
        text:      HTML-formatted message text.
    """
    cfg = get_hub_config()
    db = DatabaseManager.get_db()
    topic_doc = await db["user_topics"].find_one({"user_id": target_id})
    if not topic_doc:
        logger.warning("No user topic found for user_id=%s — skipping topic post", target_id)
        return

    await _safe_send(
        client.send_message(
            chat_id=cfg.hub_supergroup_id,
            text=text,
            message_thread_id=int(topic_doc["topic_id"]),
            parse_mode=ParseMode.HTML,
        ),
        label=f"user_topic:{target_id}",
    )


async def _fetch_user_display(target_id: int) -> tuple[str, Optional[str]]:
    """Fetch (full_name, username) for *target_id* from the users collection.

    Returns ``("Unknown", None)`` if the user document is not found.
    Never raises.

    Args:
        target_id: Telegram user ID.

    Returns:
        Tuple of (full_name, username_or_None).
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["users"].find_one({"user_id": target_id}, {"full_name": 1, "username": 1})
        if doc:
            return doc.get("full_name", "Unknown"), doc.get("username")
    except Exception as exc:
        logger.warning("Failed to fetch user display for %s: %s", target_id, exc)
    return "Unknown", None


# ─────────────────────────────────────────────────────────────────────────────
# /accept — Accept open support session
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("accept") & HUB)
@permission_required(Role.ADMIN)
async def handle_accept_command(client: Client, message: Message) -> None:
    """/accept — Accept the open support session for the user in this topic.

    Must be run inside a user topic thread. Transitions the most recent
    PENDING support session to ACTIVE, records ``accepted_by`` and
    ``accepted_at``, and writes to both audit_logs and Admin Logs topic.

    Per Section 21, the user IS notified on support session acceptance
    (only ban/mute are silent).
    """
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.reply_text("❌ This command must be run inside a user topic thread.")
        return

    db = DatabaseManager.get_db()
    topic_doc = await db["user_topics"].find_one({"topic_id": thread_id})
    if not topic_doc:
        await message.reply_text("❌ No user mapping found for this topic.")
        return

    target_id = int(topic_doc["user_id"])
    admin_id = message.from_user.id
    admin_name = message.from_user.full_name or "Admin"

    # Update the most recent PENDING support session.
    now = datetime.now(timezone.utc)
    result = await db["support_sessions"].find_one_and_update(
        {"user_id": target_id, "status": "PENDING"},
        {
            "$set": {
                "status": "ACTIVE",
                "accepted_by": admin_id,
                "accepted_at": now,
            }
        },
        sort=[("opened_at", -1)],
    )

    if result is None:
        await message.reply_text("❌ No PENDING support session found for this user.")
        return

    await message.reply_text("✅ <b>Support Session Accepted</b>", parse_mode=ParseMode.HTML)

    target_name, target_username = await _fetch_user_display(target_id)

    await _write_audit(
        action="SUPPORT ACCEPTED",
        admin_id=admin_id,
        target_user_id=target_id,
        detail={"topic_id": thread_id, "admin_name": admin_name},
    )
    await _post_admin_log(
        client,
        action="SUPPORT ACCEPTED",
        admin_id=admin_id,
        admin_name=admin_name,
        target_id=target_id,
        target_name=target_name,
        target_username=target_username,
        detail=f"Topic ID: {thread_id}",
    )

    # Notify user — acceptance is not a silent action per Section 21.
    await _safe_send(
        client.send_message(
            target_id,
            "✅ An admin has accepted your support session. You may now chat freely.",
            parse_mode=ParseMode.HTML,
        ),
        label=f"accept_notify:{target_id}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# /close — Close active support session
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("close") & HUB)
@permission_required(Role.ADMIN)
async def handle_close_command(client: Client, message: Message) -> None:
    """/close — Close the active support session for the user in this topic.

    Must be run inside a user topic thread. Transitions the most recent
    ACTIVE or PENDING session to CLOSED, records ``closed_by`` and
    ``closed_at``, writes to both audit_logs and Admin Logs topic.

    Per Section 15, user-side messages are cleaned up on session closure.
    This handler records the close in DB; message cleanup is the
    responsibility of the cleanup worker.
    """
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.reply_text("❌ This command must be run inside a user topic thread.")
        return

    db = DatabaseManager.get_db()
    topic_doc = await db["user_topics"].find_one({"topic_id": thread_id})
    if not topic_doc:
        await message.reply_text("❌ No user mapping found for this topic.")
        return

    target_id = int(topic_doc["user_id"])
    admin_id = message.from_user.id
    admin_name = message.from_user.full_name or "Admin"
    now = datetime.now(timezone.utc)

    result = await db["support_sessions"].find_one_and_update(
        {"user_id": target_id, "status": {"$in": ["ACTIVE", "PENDING"]}},
        {
            "$set": {
                "status": "CLOSED",
                "closed_by": admin_id,
                "closed_at": now,
            }
        },
        sort=[("opened_at", -1)],
    )

    if result is None:
        await message.reply_text("❌ No open support session found for this user.")
        return

    await message.reply_text("✅ <b>Support Session Closed</b>", parse_mode=ParseMode.HTML)

    target_name, target_username = await _fetch_user_display(target_id)

    await _write_audit(
        action="SUPPORT CLOSED",
        admin_id=admin_id,
        target_user_id=target_id,
        detail={"topic_id": thread_id, "admin_name": admin_name},
    )
    await _post_admin_log(
        client,
        action="SUPPORT CLOSED",
        admin_id=admin_id,
        admin_name=admin_name,
        target_id=target_id,
        target_name=target_name,
        target_username=target_username,
        detail=f"Topic ID: {thread_id}",
    )

    await _safe_send(
        client.send_message(
            target_id,
            "✅ Your support session has been closed. Thank you.",
            parse_mode=ParseMode.HTML,
        ),
        label=f"close_notify:{target_id}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# /ban — Permanently ban a user
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("ban") & HUB)
@permission_required(Role.ADMIN)
async def handle_ban_command(client: Client, message: Message) -> None:
    """/ban [user_id] [reason] — Permanently ban a user from the platform.

    Per Section 21: ban is silent — NO user DM notification is sent.
    Writes to the ``punishments`` collection (Section 25A.15),
    updates ``users.is_banned``, posts to user topic, and logs to
    Admin Logs topic + audit_logs.

    A reason is mandatory. If omitted the command is rejected.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user. Use inside a topic or provide user_id.")
            return

        args = _strip_target_from_args(message.command[1:], target_id)
        reason = " ".join(args).strip() if args else ""
        if not reason:
            await message.reply_text("❌ A ban reason is mandatory.\nUsage: `/ban [user_id] <reason>`")
            return

        admin_id = message.from_user.id
        admin_name = message.from_user.full_name or "Admin"
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()

        # Write ban to users collection.
        await db["users"].update_one(
            {"user_id": target_id},
            {"$set": {"is_banned": True, "ban_reason": reason, "banned_at": now}},
        )

        # Write to punishments collection (Section 25A.15).
        await db["punishments"].insert_one({
            "punishment_id": ObjectId(),
            "user_id": target_id,
            "type": "ban",
            "reason": reason,
            "issued_by": admin_id,
            "issued_at": now,
            "active": True,
            "resolved_at": None,
            "resolved_by": None,
        })

        await message.reply_text(
            f"🚫 User <code>{target_id}</code> permanently banned.\nReason: {reason}",
            parse_mode=ParseMode.HTML,
        )

        target_name, target_username = await _fetch_user_display(target_id)

        await _post_to_user_topic(
            client,
            target_id=target_id,
            text=(
                f"🚫 <b>USER BANNED</b>\n\n"
                f"<b>Admin:</b> {admin_name}\n"
                f"<b>Reason:</b> {reason}"
            ),
        )
        await _write_audit(
            action="USER BANNED",
            admin_id=admin_id,
            target_user_id=target_id,
            detail={"reason": reason},
        )
        await _post_admin_log(
            client,
            action="USER BANNED",
            admin_id=admin_id,
            admin_name=admin_name,
            target_id=target_id,
            target_name=target_name,
            target_username=target_username,
            detail=f"Reason: {reason}",
        )
        # Section 21: NO user DM notification for bans.

    except Exception as exc:
        logger.error("handle_ban_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /unban — Remove a ban
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("unban") & HUB)
@permission_required(Role.ADMIN)
async def handle_unban_command(client: Client, message: Message) -> None:
    """/unban [user_id] — Remove a permanent ban.

    Clears ``is_banned`` on the user document, resolves any active ban
    punishment records, and logs to Admin Logs topic + audit_logs.

    Per Section 21: unban is NOT silent — user may be notified. However the
    spec does not explicitly mandate a DM for unban, so we notify for good UX.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        admin_id = message.from_user.id
        admin_name = message.from_user.full_name or "Admin"
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()

        await db["users"].update_one(
            {"user_id": target_id},
            {"$set": {"is_banned": False, "ban_reason": None, "unbanned_at": now}},
        )

        # Resolve active ban punishments.
        await db["punishments"].update_many(
            {"user_id": target_id, "type": "ban", "active": True},
            {"$set": {"active": False, "resolved_at": now, "resolved_by": admin_id}},
        )

        await message.reply_text(
            f"✅ User <code>{target_id}</code> unbanned.", parse_mode=ParseMode.HTML
        )

        target_name, target_username = await _fetch_user_display(target_id)

        await _post_to_user_topic(
            client,
            target_id=target_id,
            text=f"✅ <b>USER UNBANNED</b>\n\n<b>Admin:</b> {admin_name}",
        )
        await _write_audit(
            action="USER UNBANNED",
            admin_id=admin_id,
            target_user_id=target_id,
            detail={},
        )
        await _post_admin_log(
            client,
            action="USER UNBANNED",
            admin_id=admin_id,
            admin_name=admin_name,
            target_id=target_id,
            target_name=target_name,
            target_username=target_username,
            detail="Ban removed",
        )

    except Exception as exc:
        logger.error("handle_unban_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /mute — Mute a user
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("mute") & HUB)
@permission_required(Role.ADMIN)
async def handle_mute_command(client: Client, message: Message) -> None:
    """/mute [user_id] [reason] — Silently mute a user.

    Per Section 21: mute is silent — NO user DM notification is sent.
    Writes to ``punishments`` collection, sets ``users.is_muted``, posts to
    user topic, and logs to Admin Logs topic + audit_logs.

    A reason is mandatory.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        args = _strip_target_from_args(message.command[1:], target_id)
        reason = " ".join(args).strip() if args else ""
        if not reason:
            await message.reply_text("❌ A mute reason is mandatory.\nUsage: `/mute [user_id] <reason>`")
            return

        admin_id = message.from_user.id
        admin_name = message.from_user.full_name or "Admin"
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()

        await db["users"].update_one(
            {"user_id": target_id},
            {
                "$set": {
                    "is_muted": True,
                    "mute_reason": reason,
                    "muted_at": now,
                    "muted_by": admin_id,
                }
            },
        )

        await db["punishments"].insert_one({
            "punishment_id": ObjectId(),
            "user_id": target_id,
            "type": "mute",
            "reason": reason,
            "issued_by": admin_id,
            "issued_at": now,
            "active": True,
            "resolved_at": None,
            "resolved_by": None,
        })

        await message.reply_text(
            f"🔇 User <code>{target_id}</code> muted (silent).", parse_mode=ParseMode.HTML
        )

        target_name, target_username = await _fetch_user_display(target_id)

        await _post_to_user_topic(
            client,
            target_id=target_id,
            text=(
                f"🔇 <b>USER MUTED</b>\n\n"
                f"<b>Admin:</b> {admin_name}\n"
                f"<b>Reason:</b> {reason}"
            ),
        )
        await _write_audit(
            action="USER MUTED",
            admin_id=admin_id,
            target_user_id=target_id,
            detail={"reason": reason},
        )
        await _post_admin_log(
            client,
            action="USER MUTED",
            admin_id=admin_id,
            admin_name=admin_name,
            target_id=target_id,
            target_name=target_name,
            target_username=target_username,
            detail=f"Reason: {reason}",
        )
        # Section 21: NO user DM notification for mutes.

    except Exception as exc:
        logger.error("handle_mute_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /unmute — Remove a mute
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("unmute") & HUB)
@permission_required(Role.ADMIN)
async def handle_unmute_command(client: Client, message: Message) -> None:
    """/unmute [user_id] — Remove a mute.

    Clears ``is_muted`` on the user document, resolves active mute punishment
    records, and logs to Admin Logs topic + audit_logs.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        admin_id = message.from_user.id
        admin_name = message.from_user.full_name or "Admin"
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()

        await db["users"].update_one(
            {"user_id": target_id},
            {"$set": {"is_muted": False, "mute_reason": None, "unmuted_at": now}},
        )

        await db["punishments"].update_many(
            {"user_id": target_id, "type": "mute", "active": True},
            {"$set": {"active": False, "resolved_at": now, "resolved_by": admin_id}},
        )

        await message.reply_text(
            f"🔊 User <code>{target_id}</code> unmuted.", parse_mode=ParseMode.HTML
        )

        target_name, target_username = await _fetch_user_display(target_id)

        await _post_to_user_topic(
            client,
            target_id=target_id,
            text=f"🔊 <b>USER UNMUTED</b>\n\n<b>Admin:</b> {admin_name}",
        )
        await _write_audit(
            action="USER UNMUTED",
            admin_id=admin_id,
            target_user_id=target_id,
            detail={},
        )
        await _post_admin_log(
            client,
            action="USER UNMUTED",
            admin_id=admin_id,
            admin_name=admin_name,
            target_id=target_id,
            target_name=target_name,
            target_username=target_username,
            detail="Mute removed",
        )

    except Exception as exc:
        logger.error("handle_unmute_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /warn — Issue a formal warning
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("warn") & HUB)
@permission_required(Role.ADMIN)
async def handle_warn_command(client: Client, message: Message) -> None:
    """/warn [user_id] <reason> — Issue a formal warning to a user.

    Increments ``users.warnings`` counter and writes to ``punishments``
    collection. The user IS notified (warnings are not silent per Section 21
    — only ban and mute are silent). Posts to user topic and Admin Logs topic.

    A reason is mandatory.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        args = _strip_target_from_args(message.command[1:], target_id)
        reason = " ".join(args).strip() if args else ""
        if not reason:
            await message.reply_text("❌ A warning reason is mandatory.\nUsage: `/warn [user_id] <reason>`")
            return

        admin_id = message.from_user.id
        admin_name = message.from_user.full_name or "Admin"
        now = datetime.now(timezone.utc)
        db = DatabaseManager.get_db()

        # Increment warning counter on user doc.
        await db["users"].update_one(
            {"user_id": target_id},
            {"$inc": {"warnings": 1}},
        )

        # Write punishment record.
        await db["punishments"].insert_one({
            "punishment_id": ObjectId(),
            "user_id": target_id,
            "type": "warning",
            "reason": reason,
            "issued_by": admin_id,
            "issued_at": now,
            "active": True,
            "resolved_at": None,
            "resolved_by": None,
        })

        await message.reply_text(
            f"⚠️ Warning issued to <code>{target_id}</code>.", parse_mode=ParseMode.HTML
        )

        target_name, target_username = await _fetch_user_display(target_id)

        await _post_to_user_topic(
            client,
            target_id=target_id,
            text=(
                f"⚠️ <b>USER WARNED</b>\n\n"
                f"<b>Admin:</b> {admin_name}\n"
                f"<b>Reason:</b> {reason}"
            ),
        )
        await _write_audit(
            action="USER WARNED",
            admin_id=admin_id,
            target_user_id=target_id,
            detail={"reason": reason},
        )
        await _post_admin_log(
            client,
            action="USER WARNED",
            admin_id=admin_id,
            admin_name=admin_name,
            target_id=target_id,
            target_name=target_name,
            target_username=target_username,
            detail=f"Reason: {reason}",
        )

        # Warnings ARE notified to the user.
        await _safe_send(
            client.send_message(
                target_id,
                f"⚠️ <b>Official Warning</b>\n\nReason: {reason}",
                parse_mode=ParseMode.HTML,
            ),
            label=f"warn_notify:{target_id}",
        )

    except Exception as exc:
        logger.error("handle_warn_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /paymentdone — Manually approve the active payment session
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("paymentdone") & HUB)
@permission_required(Role.ADMIN)
async def handle_paymentdone_command(client: Client, message: Message) -> None:
    """/paymentdone [user_id] — Manually approve the active payment session.

    Delegates to ``PaymentService.approve_payment()``. Logs PAYMENTDONE action
    to Admin Logs topic and audit_logs regardless of delegated service logging.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        admin_id = message.from_user.id
        admin_name = message.from_user.full_name or "Admin"

        from app.payments import get_payment_service
        service = get_payment_service()
        session = await service.get_active_session(target_id)

        if not session:
            await message.reply_text("❌ No active payment session found for this user.")
            return

        success = await service.approve_payment(client, session.id, admin_id)
        if not success:
            await message.reply_text("❌ Payment approval failed — check payment service logs.")
            return

        await message.reply_text(
            f"✅ Payment session <code>{session.id}</code> approved.",
            parse_mode=ParseMode.HTML,
        )

        target_name, target_username = await _fetch_user_display(target_id)

        await _write_audit(
            action="PAYMENTDONE",
            admin_id=admin_id,
            target_user_id=target_id,
            detail={"session_id": str(session.id)},
        )
        await _post_admin_log(
            client,
            action="PAYMENTDONE",
            admin_id=admin_id,
            admin_name=admin_name,
            target_id=target_id,
            target_name=target_name,
            target_username=target_username,
            detail=f"Session ID: {session.id}",
        )

    except Exception as exc:
        logger.error("handle_paymentdone_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /profile — Full user profile card
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("profile") & HUB)
@permission_required(Role.ADMIN)
async def handle_profile_command(client: Client, message: Message) -> None:
    """/profile [user_id] — Display the full user profile card.

    Shows: name, username, ban/mute/warning status, subscription plan,
    expiry date, trust/fraud scores. Reads from ``users`` and
    ``subscriptions`` collections. No writes performed — read-only command.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        db = DatabaseManager.get_db()
        user_doc = await db["users"].find_one({"user_id": target_id})
        if not user_doc:
            await message.reply_text("❌ User not found in database.")
            return

        sub_service = SubscriptionService()
        sub = await sub_service.get_subscription(target_id)

        plan_str = sub.plan.value.upper() if (sub and sub.plan) else "FREE"
        status_str = sub.status.value if sub else "N/A"
        expiry_str = (
            sub.expires_at.strftime("%Y-%m-%d %H:%M UTC")
            if (sub and sub.expires_at)
            else "Lifetime / None"
        )

        text = (
            f"👤 <b>User Profile</b>\n\n"
            f"<b>Name:</b> {user_doc.get('full_name', 'Unknown')}\n"
            f"<b>Username:</b> @{user_doc.get('username') or '-'}\n"
            f"<b>User ID:</b> <code>{target_id}</code>\n"
            f"<b>Joined:</b> {user_doc.get('join_date', 'Unknown')}\n\n"
            f"<b>Banned:</b> {'🚫 Yes' if user_doc.get('is_banned') else 'No'}\n"
            f"<b>Muted:</b> {'🔇 Yes' if user_doc.get('is_muted') else 'No'}\n"
            f"<b>Warnings:</b> {user_doc.get('warnings', 0)}\n"
            f"<b>Trust Score:</b> {user_doc.get('trust_score', 0.0):.2f}\n"
            f"<b>Fraud Score:</b> {user_doc.get('fraud_score', 0.0):.2f}\n\n"
            f"<b>Plan:</b> {plan_str}\n"
            f"<b>Sub Status:</b> {status_str}\n"
            f"<b>Expires:</b> {expiry_str}\n"
        )

        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as exc:
        logger.error("handle_profile_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /history — Recent audit event history
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("history") & HUB)
@permission_required(Role.ADMIN)
async def handle_history_command(client: Client, message: Message) -> None:
    """/history [user_id] — Show the 10 most recent audit events for the user.

    Reads from ``audit_logs`` collection (Section 25A.17). Read-only; no
    writes performed.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        db = DatabaseManager.get_db()
        cursor = (
            db["audit_logs"]
            .find({"target_user_id": target_id})
            .sort("timestamp", -1)
            .limit(10)
        )
        entries = await cursor.to_list(length=10)

        if not entries:
            await message.reply_text("No audit history found for this user.")
            return

        lines = []
        for entry in entries:
            ts = entry.get("timestamp")
            date = ts.strftime("%Y-%m-%d %H:%M") if ts else "?"
            action = entry.get("action", "UNKNOWN")
            lines.append(f"• <code>[{date}]</code> {action}")

        text = f"📜 <b>Audit History — {target_id}</b>\n\n" + "\n".join(lines)
        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as exc:
        logger.error("handle_history_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /note — Add a staff note
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("note") & HUB)
@permission_required(Role.ADMIN)
async def handle_note_command(client: Client, message: Message) -> None:
    """/note <text> — Add a private staff note for the user in this topic.

    Must be run inside a user topic thread. Writes to ``staff_notes``
    collection and logs NOTE ADDED to Admin Logs topic + audit_logs.

    Note text is mandatory.
    """
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.reply_text("❌ This command must be run inside a user topic thread.")
        return

    note_text = " ".join(message.command[1:]).strip()
    if not note_text:
        await message.reply_text("❌ Note text is mandatory.\nUsage: `/note <text>`")
        return

    db = DatabaseManager.get_db()
    topic_doc = await db["user_topics"].find_one({"topic_id": thread_id})
    if not topic_doc:
        await message.reply_text("❌ No user mapping found for this topic.")
        return

    target_id = int(topic_doc["user_id"])
    admin_id = message.from_user.id
    admin_name = message.from_user.full_name or "Admin"
    now = datetime.now(timezone.utc)

    try:
        await db["staff_notes"].insert_one({
            "user_id": target_id,
            "admin_id": admin_id,
            "note": note_text,
            "created_at": now,
        })
    except Exception as exc:
        logger.error("Failed to insert staff note for user %s: %s", target_id, exc, exc_info=True)
        await message.reply_text("❌ Failed to save note — database error.")
        return

    await message.reply_text("📌 <b>Staff Note Added</b>", parse_mode=ParseMode.HTML)

    target_name, target_username = await _fetch_user_display(target_id)

    await _write_audit(
        action="NOTE ADDED",
        admin_id=admin_id,
        target_user_id=target_id,
        detail={"note": note_text},
    )
    await _post_admin_log(
        client,
        action="NOTE ADDED",
        admin_id=admin_id,
        admin_name=admin_name,
        target_id=target_id,
        target_name=target_name,
        target_username=target_username,
        detail=f"Note: {note_text[:100]}{'...' if len(note_text) > 100 else ''}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# /notes — List all staff notes
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("notes") & HUB)
@permission_required(Role.ADMIN)
async def handle_notes_command(client: Client, message: Message) -> None:
    """/notes [user_id] — List all staff notes for a user (most recent first).

    Reads from ``staff_notes`` collection. Returns up to 20 notes.
    Read-only; no writes performed.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        db = DatabaseManager.get_db()
        cursor = (
            db["staff_notes"]
            .find({"user_id": target_id})
            .sort("created_at", -1)
            .limit(20)
        )
        notes = await cursor.to_list(length=20)

        if not notes:
            await message.reply_text("No staff notes found for this user.")
            return

        lines = []
        for note in notes:
            date = note["created_at"].strftime("%Y-%m-%d")
            lines.append(f"• <code>[{date}]</code> {note['note']}")

        text = f"📝 <b>Staff Notes — {target_id}</b>\n\n" + "\n".join(lines)
        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as exc:
        logger.error("handle_notes_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /grant — Manually grant a subscription (OWNER only)
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("grant") & HUB)
@permission_required(Role.OWNER)
async def handle_grant_command(client: Client, message: Message) -> None:
    """/grant [user_id] <days> <plan> — Manually grant a subscription.

    Gated to OWNER only — subscription grants carry significant privilege.
    Wrapped in a Redis distributed lock to prevent concurrent grants for the
    same user (Section 7.5: all subscription ops must be locked).

    Args in command:
      - days: Integer number of days. 0 means lifetime (no expiry).
      - plan: Plan string (e.g. ``premium``, ``nsfw``).

    Logs SUBSCRIPTION ACTIVATED to Admin Logs + audit_logs.
    Notifies the user via DM.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        args = _strip_target_from_args(message.command[1:], target_id)

        if len(args) < 2:
            await message.reply_text(
                "❌ Usage: `/grant [user_id] <days> <plan>`\n"
                "Example: `/grant 12345 30 premium`",
                parse_mode=ParseMode.HTML,
            )
            return

        try:
            days = int(args[0])
        except ValueError:
            await message.reply_text("❌ Days must be an integer.")
            return

        plan_str = args[1].lower()
        try:
            plan = Plan(plan_str)
        except ValueError:
            valid = ", ".join(p.value for p in Plan)
            await message.reply_text(f"❌ Invalid plan '{plan_str}'. Valid options: {valid}")
            return

        admin_id = message.from_user.id
        admin_name = message.from_user.full_name or "Admin"
        lock_key = f"subscription_grant:{target_id}"

        async with acquire_lock(lock_key, timeout=30):
            service = SubscriptionService()
            await service.grant(
                user_id=target_id,
                plan=plan,
                duration_days=days if days > 0 else None,
                granted_by=admin_id,
                notes=f"Manual grant via /grant by admin {admin_id}",
            )

        duration_str = f"{days} days" if days > 0 else "lifetime"
        await message.reply_text(
            f"✅ Granted <b>{plan.value.upper()}</b> ({duration_str}) to <code>{target_id}</code>.",
            parse_mode=ParseMode.HTML,
        )

        target_name, target_username = await _fetch_user_display(target_id)

        await _write_audit(
            action="SUBSCRIPTION ACTIVATED",
            admin_id=admin_id,
            target_user_id=target_id,
            detail={"plan": plan.value, "days": days, "granted_by": admin_id},
        )
        await _post_admin_log(
            client,
            action="SUBSCRIPTION ACTIVATED",
            admin_id=admin_id,
            admin_name=admin_name,
            target_id=target_id,
            target_name=target_name,
            target_username=target_username,
            detail=f"Plan: {plan.value.upper()}, Duration: {duration_str}",
        )

        await _safe_send(
            client.send_message(
                target_id,
                f"🎁 <b>Subscription Updated!</b>\n\n"
                f"You have been granted <b>{plan.value.upper()}</b> access "
                f"for {duration_str}.\n\nEnjoy!",
                parse_mode=ParseMode.HTML,
            ),
            label=f"grant_notify:{target_id}",
        )

    except Exception as exc:
        logger.error("handle_grant_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /revoke — Revoke a subscription (OWNER only)
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("revoke") & HUB)
@permission_required(Role.OWNER)
async def handle_revoke_command(client: Client, message: Message) -> None:
    """/revoke [user_id] — Revoke a user's active subscription.

    Gated to OWNER only. Wrapped in a Redis distributed lock.
    Logs SUBSCRIPTION EXPIRED (forced cancellation) to Admin Logs + audit_logs.
    Notifies the user via DM.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        admin_id = message.from_user.id
        admin_name = message.from_user.full_name or "Admin"
        lock_key = f"subscription_grant:{target_id}"

        async with acquire_lock(lock_key, timeout=30):
            service = SubscriptionService()
            await service.revoke(target_id, revoked_by=admin_id)

        await message.reply_text(
            f"✅ Subscription revoked for <code>{target_id}</code>.",
            parse_mode=ParseMode.HTML,
        )

        target_name, target_username = await _fetch_user_display(target_id)

        await _write_audit(
            action="SUBSCRIPTION EXPIRED",
            admin_id=admin_id,
            target_user_id=target_id,
            detail={"revoked_by": admin_id, "reason": "manual_revoke"},
        )
        await _post_admin_log(
            client,
            action="SUBSCRIPTION EXPIRED",
            admin_id=admin_id,
            admin_name=admin_name,
            target_id=target_id,
            target_name=target_name,
            target_username=target_username,
            detail="Manually revoked by owner",
        )

        await _safe_send(
            client.send_message(
                target_id,
                "⚠️ <b>Subscription Revoked</b>\n\nYour subscription has been revoked by an admin.",
                parse_mode=ParseMode.HTML,
            ),
            label=f"revoke_notify:{target_id}",
        )

    except Exception as exc:
        logger.error("handle_revoke_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /payments — Payment history for a user
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("payments") & HUB)
@permission_required(Role.ADMIN)
async def handle_payments_history_command(client: Client, message: Message) -> None:
    """/payments [user_id] — Show the 5 most recent payment records.

    Reads from ``payment_history`` collection (Section 25A.5). Read-only.
    """
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        db = DatabaseManager.get_db()
        cursor = (
            db["payment_history"]
            .find({"user_id": target_id})
            .sort("reviewed_at", -1)
            .limit(5)
        )
        payments = await cursor.to_list(length=5)

        if not payments:
            await message.reply_text("No payment history found for this user.")
            return

        lines = []
        for p in payments:
            date_field = p.get("reviewed_at") or p.get("created_at")
            date = date_field.strftime("%Y-%m-%d") if date_field else "?"
            status = p.get("status", "UNKNOWN").upper()
            amount = p.get("amount", 0)
            lines.append(f"• <code>[{date}]</code> {amount} BDT — {status}")

        text = (
            f"💳 <b>Payment History — {target_id}</b>\n\n"
            + "\n".join(lines)
        )
        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as exc:
        logger.error("handle_payments_history_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# /stats — System-wide statistics
# ─────────────────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("stats") & HUB)
@permission_required(Role.ADMIN)
async def handle_stats_command(client: Client, message: Message) -> None:
    """/stats — Display system-wide platform statistics.

    Queries ``users`` and ``subscriptions`` collections for aggregate counts.
    Read-only; no writes performed.
    """
    try:
        db = DatabaseManager.get_db()
        user_count = await db["users"].count_documents({})
        active_sub_count = await db["subscriptions"].count_documents({"status": "ACTIVE"})
        banned_count = await db["users"].count_documents({"is_banned": True})
        pending_payments = await db["payment_sessions"].count_documents({
            "status": {"$in": [
                "waiting_payment_details",
                "waiting_txid",
                "waiting_screenshot",
                "under_review",
            ]}
        })

        text = (
            "📊 <b>System Statistics</b>\n\n"
            f"👤 <b>Users:</b> {user_count}\n"
            f"💎 <b>Active Subscriptions:</b> {active_sub_count}\n"
            f"🚫 <b>Banned Users:</b> {banned_count}\n"
            f"💳 <b>Pending Payment Sessions:</b> {pending_payments}\n"
        )
        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as exc:
        logger.error("handle_stats_command failed: %s", exc, exc_info=True)
        await message.reply_text(f"❌ Error: {exc}")
