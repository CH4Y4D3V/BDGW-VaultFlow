from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import Role, permission_required
from app.services.subscription_service import SubscriptionService
from app.models.subscription import Plan
from app.utils.logger import get_logger

logger = get_logger(__name__)


@Client.on_message(filters.command("grant") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.SUDO)
async def handle_grant_command(client: Client, message: Message) -> None:
    """/grant {user_id} {days} {plan}"""
    try:
        if len(message.command) < 4:
            await message.reply_text("❌ Usage: `/grant {user_id} {days} {plan}`\nPlan: premium, free", parse_mode=ParseMode.MARKDOWN)
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
            notes=f"Manually granted via /grant by {message.from_user.id}"
        )

        await message.reply_text(f"✅ Granted <b>{plan.value}</b> to <code>{target_id}</code> for {days} days.")
        
        try:
            await client.send_message(
                target_id,
                f"🎁 <b>Subscription Updated!</b>\n\nYou have been granted <b>{plan.value.upper()}</b> access for {days} days.\nEnjoy!"
            )
        except:
            pass

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("revoke") & filters.chat(settings.VERIFICATION_GROUP_ID))
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

        await message.reply_text(f"✅ Subscription revoked for <code>{target_id}</code>.")
        try:
            await client.send_message(target_id, "⚠️ Your premium subscription has been revoked by an admin.")
        except:
            pass

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("ban") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.MODERATOR)
async def handle_ban_command(client: Client, message: Message) -> None:
    """/ban {user_id}"""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/ban {user_id}`")
            return

        target_id = int(message.command[1])
        db = DatabaseManager.get_db()
        await db["users"].update_one(
            {"_id": target_id},
            {"$set": {"is_banned": True, "banned_at": datetime.now(timezone.utc), "banned_by": message.from_user.id}},
            upsert=True
        )

        await message.reply_text(f"🚫 User <code>{target_id}</code> has been banned.")
        
    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("unban") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.MODERATOR)
async def handle_unban_command(client: Client, message: Message) -> None:
    """/unban {user_id}"""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/unban {user_id}`")
            return

        target_id = int(message.command[1])
        db = DatabaseManager.get_db()
        await db["users"].update_one(
            {"_id": target_id},
            {"$set": {"is_banned": False, "unbanned_at": datetime.now(timezone.utc)}},
        )

        await message.reply_text(f"✅ User <code>{target_id}</code> has been unbanned.")
        
    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("userinfo") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.MODERATOR)
async def handle_userinfo_command(client: Client, message: Message) -> None:
    """/userinfo {user_id} — Detailed user profile."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/userinfo {user_id}`")
            return

        target_id = int(message.command[1])
        from app.repositories.user_repository import UserRepository
        from app.services.subscription_service import SubscriptionService
        from app.repositories.activity_repository import ActivityRepository
        from app.models.activity import ActivityAction
        
        user_repo = UserRepository()
        user_doc = await user_repo.get_user(target_id)
        if not user_doc:
            await message.reply_text("❌ User not found in database.")
            return

        sub_service = SubscriptionService()
        sub = await sub_service.get_subscription(target_id)
        
        activity_repo = ActivityRepository()
        total_subs = await activity_repo.count_user_actions(target_id, ActivityAction.UPLOAD)
        
        from app.ui.common import format_header, format_info_block
        header = format_header("User Profile", "👤")
        
        text = (
            f"{header}\n"
            f"┣ {format_info_block('Name', user_doc.get('name', 'Unknown'))}\n"
            f"┣ {format_info_block('Username', '@' + user_doc.get('username', 'N/A'))}\n"
            f"┣ {format_info_block('User ID', target_id, code=True)}\n"
            f"┣ {format_info_block('Joined', user_doc.get('join_date', 'Unknown'))}\n"
            f"┣ {format_info_block('Banned', 'Yes' if user_doc.get('is_banned') else 'No')}\n"
            f"┣ {format_info_block('Plan', sub.plan.value.upper() if sub else 'FREE')}\n"
            f"┗ {format_info_block('Submissions', total_subs)}\n"
        )
        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("warn") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.MODERATOR)
async def handle_warn_command(client: Client, message: Message) -> None:
    """/warn {user_id} {reason}"""
    try:
        if len(message.command) < 3:
            await message.reply_text("❌ Usage: `/warn {user_id} {reason}`")
            return

        target_id = int(message.command[1])
        reason = " ".join(message.command[2:])
        
        # Log to activity and notify user
        from app.repositories.activity_repository import ActivityRepository
        from app.models.activity import ActivityAction
        activity_repo = ActivityRepository()
        await activity_repo.log_activity(
            user_id=target_id,
            action=ActivityAction.AUDIT,
            performed_by=message.from_user.id,
            metadata={"type": "warning", "reason": reason}
        )

        try:
            await client.send_message(
                target_id,
                f"⚠️ <b>Official Warning</b>\n\nReason: {reason}\n\nPlease follow community rules to avoid a ban."
            )
            await message.reply_text(f"✅ Warning sent to <code>{target_id}</code>.")
        except:
            await message.reply_text(f"✅ Warning logged, but could not DM user <code>{target_id}</code>.")

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("kick") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.MODERATOR)
async def handle_kick_command(client: Client, message: Message) -> None:
    """/kick {user_id} — Removes user from premium groups."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/kick {user_id}`")
            return

        target_id = int(message.command[1])
        
        premium_chat_id = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(settings, "PREMIUM_GROUP_ID", None)
        if not premium_chat_id:
            await message.reply_text("❌ Premium channel not configured.")
            return

        await client.ban_chat_member(int(premium_chat_id), target_id)
        await client.unban_chat_member(int(premium_chat_id), target_id)
        
        await message.reply_text(f"✅ User <code>{target_id}</code> kicked from premium chat.")

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("mute") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.MODERATOR)
async def handle_mute_command(client: Client, message: Message) -> None:
    """/mute {user_id} — Silent mute in the bot (F-21 rule)."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/mute {user_id}`")
            return

        target_id = int(message.command[1])
        db = DatabaseManager.get_db()
        await db["users"].update_one(
            {"_id": target_id},
            {"$set": {"is_muted": True, "muted_at": datetime.now(timezone.utc), "muted_by": message.from_user.id}}
        )
        await message.reply_text(f"🔇 User <code>{target_id}</code> has been muted (Silent).")

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("newlink") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.MODERATOR)
async def handle_newlink_command(client: Client, message: Message) -> None:
    """/newlink {user_id} — Generates a new 30-min invite link."""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Usage: `/newlink {user_id}`")
            return

        target_id = int(message.command[1])
        from app.services.invite_service import InviteService
        invite_service = InviteService()
        
        premium_chat_id = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(settings, "PREMIUM_GROUP_ID", None)
        if not premium_chat_id:
            await message.reply_text("❌ Premium channel not configured.")
            return

        invite = await invite_service.generate_premium_invite(
            client=client,
            user_id=target_id,
            chat_id=int(premium_chat_id),
            granted_by=message.from_user.id,
            plan="manual_refresh"
        )
        
        await message.reply_text(
            f"✅ <b>New Link Generated</b>\n\n"
            f"User: <code>{target_id}</code>\n"
            f"Link: <code>{invite.telegram_link}</code>\n\n"
            "This link expires in 30 minutes."
        )
        
        try:
            await client.send_message(
                target_id,
                f"🔗 <b>New Invite Link</b>\n\nAn admin has generated a new one-time invite link for you:\n{invite.telegram_link}\n\n<i>Expires in 30 minutes.</i>"
            )
        except:
            pass

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


@Client.on_message(filters.command("stats") & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.MODERATOR)
async def handle_stats_command(client: Client, message: Message) -> None:
    """/stats — System-wide statistics."""
    try:
        db = DatabaseManager.get_db()
        
        user_count = await db["users"].count_documents({})
        sub_count = await db["subscriptions"].count_documents({"status": "active"})
        premium_count = await db["subscriptions"].count_documents({"status": "active", "plan": "premium"})
        
        vault_count = await db[settings.VAULT_COLLECTION].count_documents({})
        queue_count = await db[settings.QUEUE_COLLECTION].count_documents({"status": {"$in": ["pending", "ready"]}})
        
        text = (
            "📊 <b>System Statistics</b>\n\n"
            f"👤 <b>Users:</b> {user_count}\n"
            f"💎 <b>Active Subs:</b> {sub_count} ({premium_count} Premium)\n\n"
            f"🗄 <b>Vault Items:</b> {vault_count}\n"
            f"⏳ <b>Queued Jobs:</b> {queue_count}\n"
        )
        
        await message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")


# ── SYSTEM 9: BROADCAST SYSTEM ────────────────────────────────────────────────

from app.services.audit_service import get_audit, AuditAction

# simple in-memory state for the broadcast flow.
_pending_broadcasts: dict[int, dict] = {}


async def _safe_send_broadcast(client: Client, user_id: int, message: Message) -> bool:
    """Send a copy of a message with FloodWait handling (System 9 FIX)."""
    try:
        # --- GAP 4 FIX: Use client.copy_message with full metadata ---
        # pyrogram.Client.copy_message preserves caption and all media types
        await client.copy_message(
            chat_id=user_id,
            from_chat_id=message.chat.id,
            message_id=message.id
        )
        return True
    except FloodWait as e:
        await asyncio.sleep(e.value + settings.FLOODWAIT_EXTRA_BUFFER)
        try:
            await client.copy_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.id
            )
            return True
        except Exception:
            return False
    except Exception:
        return False


@Client.on_message(
    filters.command(["broadcast", "broadcast_media", "broadcast_album", "broadcast_file"])
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_broadcast_init(client: Client, message: Message) -> None:
    admin_id = message.from_user.id
    cmd = message.command[0]
    
    _pending_broadcasts[admin_id] = {
        "type": cmd,
        "content": None,
        "started_at": datetime.now(timezone.utc)
    }
    
    await message.reply_text(
        f"📢 <b>Broadcast Initialized [{cmd}]</b>\n\n"
        "Please send the message (text, photo, video, or album) you want to broadcast now.\n\n"
        "<i>To cancel, type /cancel</i>",
        parse_mode=ParseMode.HTML
    )


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID) & ~filters.command(["cancel", "start", "help", "takedown", "userinfo", "warn", "kick", "mute", "newlink", "stats"]))
async def handle_broadcast_content(client: Client, message: Message) -> None:
    admin_id = message.from_user.id if message.from_user else None
    if not admin_id or admin_id not in _pending_broadcasts or _pending_broadcasts[admin_id]["content"] is not None:
        return

    _pending_broadcasts[admin_id]["content"] = message
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm & Send", callback_data=f"bc_confirm:{admin_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"bc_cancel:{admin_id}")
        ]
    ])
    
    await message.reply_text(
        "📝 <b>Content Received</b>\n\n"
        "Please review the content above. If it looks correct, click confirm to begin the broadcast to ALL users.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        reply_to_message_id=message.id
    )


@Client.on_callback_query(filters.regex(r"^bc_confirm:(?P<aid>\d+)$"))
async def handle_broadcast_confirm(client: Client, callback: CallbackQuery) -> None:
    admin_id = int(callback.matches[0].group("aid"))
    if admin_id != callback.from_user.id:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    broadcast_data = _pending_broadcasts.pop(admin_id, None)
    if not broadcast_data or not broadcast_data["content"]:
        await callback.answer("Broadcast session expired.", show_alert=True)
        return

    await callback.message.edit_text("🚀 <b>Broadcast Started</b>\n\nProgress will be logged to the Audit thread.")
    
    asyncio.create_task(_execute_broadcast(client, broadcast_data["content"], admin_id))


async def _execute_broadcast(client: Client, content_message: Message, admin_id: int) -> None:
    db = DatabaseManager.get_db()
    users_cursor = db["users"].find({"is_banned": False})
    
    total_users = await db["users"].count_documents({"is_banned": False})
    sent_count = 0
    fail_count = 0
    
    start_time = datetime.now(timezone.utc)
    
    await get_audit().log(
        action="broadcast_started",
        performed_by=admin_id,
        details={"total_targets": total_users}
    )

    async for user_doc in users_cursor:
        user_id = user_doc["_id"]
        success = await _safe_send_broadcast(client, user_id, content_message)
        if success:
            sent_count += 1
        else:
            fail_count += 1
            
        if (sent_count + fail_count) % 50 == 0:
            logger.info("Broadcast in progress", extra={"sent": sent_count, "failed": fail_count, "total": total_users})

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    
    summary = (
        f"✅ <b>Broadcast Complete</b>\n\n"
        f"┣ 👤 <b>Targets:</b> {total_users}\n"
        f"┣ ✨ <b>Delivered:</b> {sent_count}\n"
        f"┣ ❌ <b>Failed:</b> {fail_count}\n"
        f"┗ ⏱ <b>Duration:</b> {duration:.1f}s"
    )
    
    if settings.HUB_TOPIC_AUDIT:
        try:
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=f"[BROADCAST_COMPLETE] | Admin: {admin_id} | Details: {summary}",
                message_thread_id=settings.HUB_TOPIC_AUDIT,
                parse_mode=ParseMode.HTML
            )
        except:
            pass

    await get_audit().log(
        action="broadcast_complete",
        performed_by=admin_id,
        details={"sent": sent_count, "failed": fail_count, "duration": duration}
    )


@Client.on_callback_query(filters.regex(r"^bc_cancel:(?P<aid>\d+)$"))
async def handle_broadcast_cancel_cb(client: Client, callback: CallbackQuery) -> None:
    admin_id = int(callback.matches[0].group("aid"))
    if admin_id != callback.from_user.id:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    _pending_broadcasts.pop(admin_id, None)
    await callback.message.edit_text("❌ <b>Broadcast Cancelled</b>")
