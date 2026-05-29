from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

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
