from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import Role, permission_required
from app.services.subscription_service import SubscriptionService
from app.models.subscription import Plan
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ── Subscription management ───────────────────────────────────────────────────

@Client.on_message(
    filters.command("grant") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_grant_command(client: Client, message: Message) -> None:
    """/grant {user_id} {days} {plan}"""
    try:
        if len(message.command) < 4:
            await message.reply_text(
                "❌ Usage: `/grant {user_id} {days} {plan}`\nPlan: premium, free",
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        target_id = int(message.command[1])
        days = int(message.command[2])
        plan_str = message.command[3].lower()

        try:
            plan = Plan(plan_str)
        except ValueError:
            await message.reply_text("❌ Invalid plan. Use 'premium' or 'free'.")
            return

        service = SubscriptionService()
        await service.grant(
            user_id=target_id,
            plan=plan,
            duration_days=days if days > 0 else None,
            granted_by=message.from_user.id,
            notes=f"Manually granted via /grant by {message.from_user.id}",
        )

        await message.reply_text(
            f"✅ Granted <b>{plan.value}</b> to <code>{target_id}</code> for {days} days.",
            parse_mode=ParseMode.HTML,
        )

        try:
            await client.send_message(
                target_id,
                f"🎁 <b>Subscription Updated!</b>\n\nYou have been granted "
                f"<b>{plan.value.upper()}</b> access for {days} days.\nEnjoy!",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("revoke") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_revoke_command(client: Client, message: Message) -> None:
    """/revoke {user_id}"""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/revoke {user_id}`")
            return

        target_id = int(message.command[1])
        service = SubscriptionService()
        await service.revoke(target_id, revoked_by=message.from_user.id)

        await message.reply_text(
            f"✅ Subscription revoked for <code>{target_id}</code>.",
            parse_mode=ParseMode.HTML,
        )
        try:
            await client.send_message(
                target_id,
                "⚠️ Your premium subscription has been revoked by an admin.",
            )
        except Exception:
            pass

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


# ── User moderation ───────────────────────────────────────────────────────────

@Client.on_message(
    filters.command("ban") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_ban_command(client: Client, message: Message) -> None:
    """/ban {user_id} [reason] — Permanent bot ban (Section 21: silent)."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/ban {user_id} [reason]`")
            return

        target_id = int(message.command[1])
        reason = (
            " ".join(message.command[2:])
            if len(message.command) > 2
            else "Banned by admin"
        )

        from app.repositories.user_repository import UserRepository
        user_repo = UserRepository()
        await user_repo.ban_user(target_id, reason)

        await message.reply_text(
            f"🚫 User <code>{target_id}</code> has been permanently banned.\n"
            f"Reason: {reason}",
            parse_mode=ParseMode.HTML,
        )
        # Section 21: Bot ban = silent. No notification to user.

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("unban") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_unban_command(client: Client, message: Message) -> None:
    """/unban {user_id} — Removes bot ban."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/unban {user_id}`")
            return

        target_id = int(message.command[1])
        from app.repositories.user_repository import UserRepository
        user_repo = UserRepository()
        result = await user_repo.collection.update_one(
            {"_id": target_id},
            {"$set": {"is_banned": False, "unbanned_at": datetime.now(timezone.utc)}}
        )

        if result.modified_count:
            await message.reply_text(
                f"✅ User <code>{target_id}</code> has been unbanned.",
                parse_mode=ParseMode.HTML,
            )
            try:
                await client.send_message(
                    target_id,
                    "✅ <b>Ban Removed</b>\n\nYour access to the bot has been restored.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        else:
            await message.reply_text("❌ User not found in database.")

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("kick") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_kick_command(client: Client, message: Message) -> None:
    """/kick {user_id} — Removes user from premium groups."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/kick {user_id}`")
            return

        target_id = int(message.command[1])
        premium_chat_id = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(
            settings, "PREMIUM_GROUP_ID", None
        )
        if not premium_chat_id:
            await message.reply_text("❌ Premium channel not configured.")
            return

        await client.ban_chat_member(int(premium_chat_id), target_id)
        await client.unban_chat_member(int(premium_chat_id), target_id)
        await message.reply_text(
            f"✅ User <code>{target_id}</code> kicked from premium chat.",
            parse_mode=ParseMode.HTML,
        )

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("mute") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_mute_command(client: Client, message: Message) -> None:
    """/mute {user_id} — Silent mute (Section 21: no notification)."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/mute {user_id}`")
            return

        target_id = int(message.command[1])
        db = DatabaseManager.get_db()
        await db["users"].update_one(
            {"_id": target_id},
            {
                "$set": {
                    "is_muted": True,
                    "muted_at": datetime.now(timezone.utc),
                    "muted_by": message.from_user.id,
                }
            },
        )
        # Section 21: Mute is silent — no user notification.
        await message.reply_text(
            f"🔇 User <code>{target_id}</code> has been muted (silent).",
            parse_mode=ParseMode.HTML,
        )

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("warn") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_warn_command(client: Client, message: Message) -> None:
    """/warn {user_id} {reason}"""
    try:
        if len(message.command) < 3:
            await message.reply_text("❌ Usage: `/warn {user_id} {reason}`")
            return

        target_id = int(message.command[1])
        reason = " ".join(message.command[2:])

        db = DatabaseManager.get_db()
        await db["users"].update_one(
            {"_id": target_id},
            {
                "$push": {
                    "warnings": {
                        "reason": reason,
                        "warned_by": message.from_user.id,
                        "warned_at": datetime.now(timezone.utc),
                    }
                },
                "$inc": {"warning_count": 1},
            },
            upsert=True,
        )

        admin_name = message.from_user.first_name or "Admin"
        from app.services.audit_service import get_audit
        await get_audit().log(
            action="member_warn",
            performed_by=message.from_user.id,
            target_user_id=target_id,
            details={"reason": reason}
        )

        try:
            await client.send_message(
                target_id,
                f"⚠️ <b>Official Warning</b>\n\nReason: {reason}\n\n"
                "Please follow community rules to avoid a ban.",
                parse_mode=ParseMode.HTML,
            )
            await message.reply_text(
                f"✅ Warning sent to <code>{target_id}</code>.\nAdmin: {admin_name}",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            await message.reply_text(
                f"✅ Warning logged for <code>{target_id}</code>, "
                "but could not DM user.\n"
                f"Admin: {admin_name}",
                parse_mode=ParseMode.HTML,
            )

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("userinfo") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUPPORT_ADMIN)
async def handle_userinfo_command(client: Client, message: Message) -> None:
    """/userinfo {user_id}"""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/userinfo {user_id}`")
            return

        target_id = int(message.command[1])
        db = DatabaseManager.get_db()
        user_doc = await db["users"].find_one({"_id": target_id})
        if not user_doc:
            await message.reply_text("❌ User not found in database.")
            return

        sub_service = SubscriptionService()
        sub = await sub_service.get_subscription(target_id)

        join_date = user_doc.get("join_date") or user_doc.get("created_at")
        join_str = (
            join_date.strftime("%Y-%m-%d")
            if hasattr(join_date, "strftime")
            else str(join_date or "Unknown")
        )

        plan_str = sub.plan.value.upper() if sub else "FREE"
        status_str = sub.status.value.upper() if sub else "N/A"
        banned_str = "Yes ⛔" if user_doc.get("is_banned") else "No ✅"
        warning_count = user_doc.get("warning_count", 0)

        text = (
            f"👤 <b>User Profile</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"<b>Name:</b> {user_doc.get('name', user_doc.get('first_name', 'Unknown'))}\n"
            f"<b>Username:</b> @{user_doc.get('username') or 'N/A'}\n"
            f"<b>User ID:</b> <code>{target_id}</code>\n"
            f"<b>Joined:</b> {join_str}\n"
            f"<b>Banned:</b> {banned_str}\n"
            f"<b>Warnings:</b> {warning_count}\n"
            f"<b>Plan:</b> {plan_str}\n"
            f"<b>Sub Status:</b> {status_str}\n"
        )

        if sub and sub.expires_at:
            text += f"<b>Expires:</b> {sub.expires_at.strftime('%Y-%m-%d')}\n"

        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("newlink") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUPPORT_ADMIN)
async def handle_newlink_command(client: Client, message: Message) -> None:
    """/newlink {user_id} — Generates a new 30-min invite link."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/newlink {user_id}`")
            return

        target_id = int(message.command[1])
        from app.services.invite_service import InviteService

        invite_service = InviteService()

        premium_chat_id = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(
            settings, "PREMIUM_GROUP_ID", None
        )
        if not premium_chat_id:
            await message.reply_text("❌ Premium channel not configured.")
            return

        invite = await invite_service.generate_premium_invite(
            client=client,
            user_id=target_id,
            chat_id=int(premium_chat_id),
            granted_by=message.from_user.id,
            plan="manual_refresh",
        )

        await message.reply_text(
            f"✅ <b>New Link Generated</b>\n\n"
            f"User: <code>{target_id}</code>\n"
            f"Link: <code>{invite.telegram_link}</code>\n\n"
            "This link expires in 30 minutes and is single-use.",
            parse_mode=ParseMode.HTML,
        )

        try:
            await client.send_message(
                target_id,
                f"🔗 <b>New Invite Link</b>\n\nAn admin has generated a new one-time "
                f"invite link for you:\n{invite.telegram_link}\n\n"
                "<i>Expires in 30 minutes.</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("stats") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUPPORT_ADMIN)
async def handle_stats_command(client: Client, message: Message) -> None:
    """/stats — System-wide statistics."""
    try:
        db = DatabaseManager.get_db()

        user_count = await db["users"].count_documents({})
        sub_count = await db["subscriptions"].count_documents({"status": "active"})
        premium_count = await db["subscriptions"].count_documents(
            {"status": "active", "plan": "premium"}
        )
        vault_count = await db[settings.VAULT_COLLECTION].count_documents({})
        queue_count = await db[settings.QUEUE_COLLECTION].count_documents(
            {"status": {"$in": ["pending", "ready"]}}
        )

        text = (
            "📊 <b>System Statistics</b>\n\n"
            f"👤 <b>Users:</b> {user_count}\n"
            f"💎 <b>Active Subs:</b> {sub_count} ({premium_count} Premium)\n\n"
            f"🗄 <b>Vault Items:</b> {vault_count}\n"
            f"⏳ <b>Queued Jobs:</b> {queue_count}\n"
        )

        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("approve") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.PAYMENT_ADMIN)
async def handle_approve_command(client: Client, message: Message) -> None:
    """/approve {payment_id}"""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/approve {payment_id}`")
            return

        session_id = message.command[1]
        from app.payments.service import get_payment_service
        service = get_payment_service()

        admin_name = message.from_user.first_name or "Admin"
        success = await service.approve_payment(client, session_id, message.from_user.id)
        if success:
            await message.reply_text(f"✅ Payment <code>{session_id}</code> approved.\nAdmin: {admin_name}", parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("❌ Failed to approve payment. Check session status.")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("reject") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.PAYMENT_ADMIN)
async def handle_reject_command(client: Client, message: Message) -> None:
    """/reject {payment_id} {reason}"""
    try:
        if len(message.command) < 3:
            await message.reply_text("❌ Usage: `/reject {payment_id} {reason}`")
            return

        session_id = message.command[1]
        reason = " ".join(message.command[2:])
        from app.payments.service import get_payment_service
        service = get_payment_service()

        admin_name = message.from_user.first_name or "Admin"
        success = await service.reject_payment(session_id, reason, message.from_user.id)
        if success:
            await message.reply_text(f"❌ Payment <code>{session_id}</code> rejected.\nReason: {reason}\nAdmin: {admin_name}", parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("❌ Failed to reject payment.")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


# ── Broadcast system ──────────────────────────────────────────────────────────

_pending_broadcasts: dict[int, dict] = {}
_broadcast_album_buffer: dict[str, dict] = {}
_broadcast_album_tasks: dict[str, asyncio.Task] = {}


async def _safe_send_broadcast(
    client: Client,
    user_id: int,
    source_chat_id: int,
    message_id: int,
    caption: Optional[str] = None,
) -> bool:
    from pyrogram.errors import UserIsBlocked, PeerIdInvalid

    kwargs: dict = {
        "chat_id": user_id,
        "from_chat_id": source_chat_id,
        "message_id": message_id,
    }
    if caption is not None:
        kwargs["caption"] = caption

    for attempt in range(2):
        try:
            await client.copy_message(**kwargs)
            return True
        except FloodWait as e:
            await asyncio.sleep(e.value + settings.FLOODWAIT_EXTRA_BUFFER)
        except (UserIsBlocked, PeerIdInvalid):
            return False
        except Exception:
            return False
    return False


async def _execute_broadcast(
    client: Client,
    source_chat_id: int,
    message_ids: list[int],
    caption: Optional[str],
    admin_id: int,
    target: str = "all"
) -> None:
    from app.services.audit_service import get_audit

    db = DatabaseManager.get_db()
    sent_count = 0
    fail_count = 0
    start_time = datetime.now(timezone.utc)

    # ── 1. Determine targets ──────────────────────────────────────────────────
    target_ids: list[int] = []

    if target == "all":
        cursor = db["users"].find({"is_banned": False}, {"_id": 1})
        async for u in cursor:
            target_ids.append(u["_id"])
    elif target == "premium":
        cursor = db["subscriptions"].find({"status": "ACTIVE", "plan": {"$ne": "free"}}, {"user_id": 1})
        async for s in cursor:
            target_ids.append(s["user_id"])
    elif target == "group":
        if settings.PREMIUM_GROUP_ID:
            target_ids.append(int(settings.PREMIUM_GROUP_ID))
    elif target == "channel":
        if settings.PREMIUM_CHANNEL_ID:
            target_ids.append(int(settings.PREMIUM_CHANNEL_ID))

    total_targets = len(target_ids)

    await get_audit().log(
        action="broadcast_started",
        performed_by=admin_id,
        details={"total_targets": total_targets, "message_count": len(message_ids), "target_type": target},
    )

    for target_id in target_ids:
        try:
            if len(message_ids) > 1:
                await client.copy_media_group(
                    chat_id=target_id,
                    from_chat_id=source_chat_id,
                    message_id=message_ids[0],
                )
                sent_count += 1
            else:
                success = await _safe_send_broadcast(
                    client, target_id, source_chat_id, message_ids[0], caption
                )
                if success:
                    sent_count += 1
                else:
                    fail_count += 1
        except Exception:
            fail_count += 1

        if (sent_count + fail_count) % 100 == 0:
            logger.info(
                "Broadcast progress",
                extra={"ctx_sent": sent_count, "ctx_failed": fail_count, "ctx_total": total_targets},
            )

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    summary = (
        f"✅ <b>Broadcast Complete</b>\n\n"
        f"┣ 👤 <b>Targets:</b> {total_targets}\n"
        f"┣ ✨ <b>Delivered:</b> {sent_count}\n"
        f"┣ ❌ <b>Failed:</b> {fail_count}\n"
        f"┗ ⏱ <b>Duration:</b> {duration:.1f}s"
    )

    hub_topic_audit = getattr(settings, "HUB_TOPIC_AUDIT", None)
    if hub_topic_audit:
        try:
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=summary,
                message_thread_id=hub_topic_audit,
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    await get_audit().log(
        action="broadcast_complete",
        performed_by=admin_id,
        details={"sent": sent_count, "failed": fail_count, "duration": duration},
    )


@Client.on_message(
    filters.command(
        ["broadcast", "broadcast_premium", "broadcast_group", "broadcast_channel"]
    )
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_broadcast_init(client: Client, message: Message) -> None:
    admin_id = message.from_user.id
    cmd = message.command[0]

    target = "all"
    if "premium" in cmd:
        target = "premium"
    elif "group" in cmd:
        target = "group"
    elif "channel" in cmd:
        target = "channel"

    _pending_broadcasts[admin_id] = {
        "type": cmd,
        "target": target,
        "messages": [],
        "started_at": datetime.now(timezone.utc),
    }

    await message.reply_text(
        f"📢 <b>Broadcast Initialized [Target: {target.upper()}]</b>\n\n"
        "Send the content you want to broadcast now. "
        "Albums are supported — send all items and wait 2 seconds.\n\n"
        "<i>To cancel, type /cancel_broadcast</i>",
        parse_mode=ParseMode.HTML,
    )


@Client.on_message(
    filters.command("cancel_broadcast")
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_broadcast_cancel_cmd(client: Client, message: Message) -> None:
    admin_id = message.from_user.id
    _pending_broadcasts.pop(admin_id, None)
    await message.reply_text("❌ Broadcast cancelled.")


async def _flush_broadcast_album(
    admin_id: int, group_id: str, client: Client
) -> None:
    await asyncio.sleep(2.0)

    buffer_data = _broadcast_album_buffer.pop(group_id, None)
    _broadcast_album_tasks.pop(group_id, None)

    if not buffer_data or not buffer_data.get("messages"):
        return

    messages = sorted(buffer_data["messages"], key=lambda m: m.id)
    broadcast_data = _pending_broadcasts.get(admin_id)
    if not broadcast_data:
        return

    broadcast_data["messages"] = messages
    broadcast_data["is_album"] = True

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Confirm & Send",
                    callback_data=f"bc_confirm:{admin_id}",
                ),
                InlineKeyboardButton(
                    "❌ Cancel",
                    callback_data=f"bc_cancel:{admin_id}",
                ),
            ]
        ]
    )

    try:
        await client.send_message(
            chat_id=settings.VERIFICATION_GROUP_ID,
            text=f"📝 <b>Album Received ({len(messages)} items)</b>\n\n"
            "Click confirm to broadcast to ALL users.",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID))
async def handle_broadcast_content_capture(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    admin_id = message.from_user.id
    broadcast_data = _pending_broadcasts.get(admin_id)

    if not broadcast_data or broadcast_data.get("messages"):
        return

    if message.text and message.text.startswith("/"):
        return

    if message.media_group_id:
        group_id = f"bc_{admin_id}_{message.media_group_id}"

        if group_id not in _broadcast_album_buffer:
            _broadcast_album_buffer[group_id] = {
                "messages": [],
                "admin_id": admin_id,
            }

            existing_task = _broadcast_album_tasks.get(group_id)
            if existing_task and not existing_task.done():
                existing_task.cancel()

            task = asyncio.create_task(
                _flush_broadcast_album(admin_id, group_id, client),
                name=f"bc-album-{group_id}",
            )
            _broadcast_album_tasks[group_id] = task

        _broadcast_album_buffer[group_id]["messages"].append(message)
        return

    broadcast_data["messages"] = [message]
    broadcast_data["is_album"] = False

    caption = message.caption or message.text or None

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Confirm & Send",
                    callback_data=f"bc_confirm:{admin_id}",
                ),
                InlineKeyboardButton(
                    "❌ Cancel",
                    callback_data=f"bc_cancel:{admin_id}",
                ),
            ]
        ]
    )

    content_type = "Text"
    if message.photo:
        content_type = "Photo"
    elif message.video:
        content_type = "Video"
    elif message.audio:
        content_type = "Audio"
    elif message.voice:
        content_type = "Voice"
    elif message.document:
        content_type = "Document"
    elif message.animation:
        content_type = "GIF/Animation"
    elif message.sticker:
        content_type = "Sticker"
    elif message.video_note:
        content_type = "Video Note"

    await message.reply_text(
        f"📝 <b>{content_type} Received</b>\n\n"
        "Click confirm to broadcast to ALL users.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^bc_confirm:(?P<aid>\d+)$"))
async def handle_broadcast_confirm(client: Client, callback: CallbackQuery) -> None:
    admin_id = int(callback.matches[0].group("aid"))

    if admin_id != callback.from_user.id:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    broadcast_data = _pending_broadcasts.pop(admin_id, None)
    if not broadcast_data or not broadcast_data.get("messages"):
        await callback.answer("Broadcast session expired.", show_alert=True)
        return

    await callback.message.edit_text(
        "🚀 <b>Broadcast Started</b>\n\nProgress will be logged to the Audit thread.",
        parse_mode=ParseMode.HTML,
    )

    messages = broadcast_data["messages"]
    source_chat_id = messages[0].chat.id
    message_ids = [m.id for m in messages]
    caption = messages[0].caption or messages[0].text or None
    target = broadcast_data.get("target", "all")

    admin_name = callback.from_user.first_name or "Admin"
    await callback.answer(f"🚀 Broadcast started by {admin_name}!", show_alert=True)

    from app.services.audit_service import get_audit, AuditAction
    await get_audit().log(
        action=AuditAction.BROADCAST,
        performed_by=admin_id,
        details={"target": target, "count": len(message_ids)}
    )

    asyncio.create_task(
        _execute_broadcast(client, source_chat_id, message_ids, caption, admin_id, target),
        name=f"broadcast-{admin_id}",
    )


@Client.on_callback_query(filters.regex(r"^bc_cancel:(?P<aid>\d+)$"))
async def handle_broadcast_cancel_cb(client: Client, callback: CallbackQuery) -> None:
    admin_id = int(callback.matches[0].group("aid"))

    if admin_id != callback.from_user.id:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    _pending_broadcasts.pop(admin_id, None)

    try:
        await callback.message.edit_text("❌ <b>Broadcast Cancelled</b>", parse_mode=ParseMode.HTML)
    except Exception:
        pass