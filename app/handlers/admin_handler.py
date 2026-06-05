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


# ── Internal helpers ──────────────────────────────────────────────────────────

async def _resolve_target_user(message: Message) -> Optional[int]:
    """Resolves target user_id from command args or forum topic context."""
    # 1. Direct argument: /command 12345
    if len(message.command) > 1 and message.command[1].isdigit():
        return int(message.command[1])
    
    # 2. Forum topic context: /command inside a user thread
    thread_id = getattr(message, "message_thread_id", None)
    if thread_id:
        from app.services.topic_manager import get_topic_manager
        topic_doc = await get_topic_manager().get_user_by_topic(thread_id)
        if topic_doc:
            return topic_doc["user_id"]
            
    return None


# ── User thread management ───────────────────────────────────────────────────

@Client.on_message(
    filters.command("accept") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUPPORT_ADMIN)
async def handle_accept_command(client: Client, message: Message) -> None:
    """/accept — Marks the current user topic as accepted by admin."""
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.reply_text("❌ This command must be used inside a user topic.")
        return

    from app.services.topic_manager import get_topic_manager
    topic_doc = await get_topic_manager().get_user_by_topic(thread_id)
    if not topic_doc:
        await message.reply_text("❌ User mapping not found for this topic.")
        return

    user_id = topic_doc["user_id"]
    admin_id = message.from_user.id
    admin_name = message.from_user.first_name or "Admin"

    db = DatabaseManager.get_db()
    await db["user_topics"].update_one(
        {"topic_id": thread_id},
        {
            "$set": {
                "status": "accepted",
                "accepted_by": admin_id,
                "accepted_at": datetime.now(timezone.utc),
                "last_activity_at": datetime.now(timezone.utc)
            }
        }
    )

    await message.reply_text(
        f"✅ <b>Topic Accepted</b>\nAdmin: {admin_name}",
        parse_mode=ParseMode.HTML
    )
    
    from app.services.audit_service import get_audit
    await get_audit().log(
        action="topic_accepted",
        performed_by=admin_id,
        target_user_id=user_id,
        details={"topic_id": thread_id}
    )

    try:
        await client.send_message(
            user_id,
            "✅ An admin has accepted your request. You can now chat freely.",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass


@Client.on_message(
    filters.command("close") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUPPORT_ADMIN)
async def handle_close_command(client: Client, message: Message) -> None:
    """/close — Closes the user session in the topic."""
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.reply_text("❌ This command must be used inside a user topic.")
        return

    from app.services.topic_manager import get_topic_manager
    topic_doc = await get_topic_manager().get_user_by_topic(thread_id)
    if not topic_doc:
        await message.reply_text("❌ User mapping not found for this topic.")
        return

    user_id = topic_doc["user_id"]
    admin_id = message.from_user.id

    db = DatabaseManager.get_db()
    await db["user_topics"].update_one(
        {"topic_id": thread_id},
        {
            "$set": {
                "status": "closed",
                "closed_at": datetime.now(timezone.utc),
                "last_activity_at": datetime.now(timezone.utc)
            }
        }
    )

    await message.reply_text("✅ <b>Session Closed</b>", parse_mode=ParseMode.HTML)
    
    from app.services.audit_service import get_audit
    await get_audit().log(
        action="topic_closed",
        performed_by=admin_id,
        target_user_id=user_id,
        details={"topic_id": thread_id}
    )

    try:
        await client.send_message(
            user_id,
            "✅ Your support session has been closed. Thank you.",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass


# ── Subscription management ───────────────────────────────────────────────────

@Client.on_message(
    filters.command("grant") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_grant_command(client: Client, message: Message) -> None:
    """/grant [user_id] {days} {plan}"""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        args = message.command[1:]
        if args and args[0].isdigit() and int(args[0]) == target_id:
            args = args[1:]
        
        if len(args) < 2:
            await message.reply_text("❌ Usage: `/grant [user_id] {days} {plan}`")
            return

        days = int(args[0])
        plan_str = args[1].lower()

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
    """/revoke [user_id]"""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        service = SubscriptionService()
        await service.revoke(target_id, revoked_by=message.from_user.id)

        await message.reply_text(
            f"✅ Subscription revoked for <code>{target_id}</code>.",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


# ── User moderation ───────────────────────────────────────────────────────────

@Client.on_message(
    filters.command("ban") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_ban_command(client: Client, message: Message) -> None:
    """/ban [user_id] [reason]"""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        args = message.command[1:]
        if args and args[0].isdigit() and int(args[0]) == target_id:
            args = args[1:]
            
        reason = " ".join(args) if args else "Banned by admin"

        from app.repositories.user_repository import UserRepository
        user_repo = UserRepository()
        await user_repo.ban_user(target_id, reason)

        admin_id = message.from_user.id
        admin_name = message.from_user.first_name or "Admin"

        # ── LOG TO USER TOPIC ──
        try:
            from app.services.topic_manager import get_topic_manager
            topic_id = await get_topic_manager().get_or_create_user_topic(client, target_id)
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=f"🚫 <b>USER BANNED</b>\n\n<b>Admin:</b> {admin_name}\n<b>Reason:</b> {reason}",
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

        # ── LOG TO ADMIN LOGS ──
        try:
            from app.services.admin_logger import get_admin_logger
            await get_admin_logger().log(
                client=client,
                action="USER BANNED",
                admin_id=admin_id,
                admin_name=admin_name,
                target_user_id=target_id,
                details=f"Reason: {reason}"
            )
        except Exception:
            pass

        await message.reply_text(
            f"🚫 User <code>{target_id}</code> has been permanently banned.\n"
            f"Reason: {reason}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("unban") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_unban_command(client: Client, message: Message) -> None:
    """/unban [user_id]"""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        from app.repositories.user_repository import UserRepository
        user_repo = UserRepository()
        await user_repo.collection.update_one(
            {"_id": target_id},
            {"$set": {"is_banned": False, "unbanned_at": datetime.now(timezone.utc)}}
        )

        await message.reply_text(f"✅ User <code>{target_id}</code> unbanned.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("mute") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_mute_command(client: Client, message: Message) -> None:
    """/mute [user_id]"""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

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

        admin_id = message.from_user.id
        admin_name = message.from_user.first_name or "Admin"

        # ── LOG TO USER TOPIC ──
        try:
            from app.services.topic_manager import get_topic_manager
            topic_id = await get_topic_manager().get_or_create_user_topic(client, target_id)
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=f"🔇 <b>USER MUTED</b>\n\n<b>Admin:</b> {admin_name}",
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

        # ── LOG TO ADMIN LOGS ──
        try:
            from app.services.admin_logger import get_admin_logger
            await get_admin_logger().log(
                client=client,
                action="USER MUTED",
                admin_id=admin_id,
                admin_name=admin_name,
                target_user_id=target_id
            )
        except Exception:
            pass

        await message.reply_text(f"🔇 User <code>{target_id}</code> has been muted (silent).", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("unmute") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_unmute_command(client: Client, message: Message) -> None:
    """/unmute [user_id]"""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        db = DatabaseManager.get_db()
        await db["users"].update_one(
            {"_id": target_id},
            {"$set": {"is_muted": False, "unmuted_at": datetime.now(timezone.utc)}}
        )

        admin_id = message.from_user.id
        admin_name = message.from_user.first_name or "Admin"

        # ── LOG TO USER TOPIC ──
        try:
            from app.services.topic_manager import get_topic_manager
            topic_id = await get_topic_manager().get_or_create_user_topic(client, target_id)
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=f"🔊 <b>USER UNMUTED</b>\n\n<b>Admin:</b> {admin_name}",
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

        # ── LOG TO ADMIN LOGS ──
        try:
            from app.services.admin_logger import get_admin_logger
            await get_admin_logger().log(
                client=client,
                action="USER UNMUTED",
                admin_id=admin_id,
                admin_name=admin_name,
                target_user_id=target_id
            )
        except Exception:
            pass

        await message.reply_text(f"🔊 User <code>{target_id}</code> unmuted.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("warn") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_warn_command(client: Client, message: Message) -> None:
    """/warn [user_id] {reason}"""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        args = message.command[1:]
        if args and args[0].isdigit() and int(args[0]) == target_id:
            args = args[1:]
            
        if not args:
            await message.reply_text("❌ Usage: `/warn [user_id] {reason}`")
            return
            
        reason = " ".join(args)

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

        admin_id = message.from_user.id
        admin_name = message.from_user.first_name or "Admin"

        # ── LOG TO USER TOPIC ──
        try:
            from app.services.topic_manager import get_topic_manager
            topic_id = await get_topic_manager().get_or_create_user_topic(client, target_id)
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=f"⚠️ <b>USER WARNED</b>\n\n<b>Admin:</b> {admin_name}\n<b>Reason:</b> {reason}",
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

        # ── LOG TO ADMIN LOGS ──
        try:
            from app.services.admin_logger import get_admin_logger
            await get_admin_logger().log(
                client=client,
                action="USER WARNED",
                admin_id=admin_id,
                admin_name=admin_name,
                target_user_id=target_id,
                details=f"Reason: {reason}"
            )
        except Exception:
            pass

        await message.reply_text(f"✅ Warning logged for <code>{target_id}</code>.", parse_mode=ParseMode.HTML)
        try:
            await client.send_message(target_id, f"⚠️ <b>Official Warning</b>\n\nReason: {reason}")
        except Exception:
            pass
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


# ── Payment commands ──────────────────────────────────────────────────────────

@Client.on_message(
    filters.command("paymentdone") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.PAYMENT_ADMIN)
async def handle_paymentdone_command(client: Client, message: Message) -> None:
    """/paymentdone [user_id] — Shortcut to approve the active payment session."""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        from app.payments import get_payment_service
        service = get_payment_service()
        session = await service.get_active_session(target_id)

        if not session:
            await message.reply_text("❌ No active payment session found for this user.")
            return

        success = await service.approve_payment(client, session.id, message.from_user.id)
        if success:
            await message.reply_text(f"✅ Payment session <code>{session.id}</code> approved.")
        else:
            await message.reply_text("❌ Failed to approve payment.")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("payments") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.PAYMENT_ADMIN)
async def handle_payments_history_command(client: Client, message: Message) -> None:
    """/payments [user_id] — Payment session history."""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        db = DatabaseManager.get_db()
        cursor = db["payments"].find({"user_id": target_id}).sort("created_at", -1).limit(5)
        payments = await cursor.to_list(length=5)

        if not payments:
            await message.reply_text("No payment history found.")
            return

        text = f"💳 <b>Payment History for {target_id}</b>\n\n"
        for p in payments:
            date = p["created_at"].strftime("%Y-%m-%d")
            status = p.get("status", "unknown").upper()
            amount = p.get("locked_amount", 0)
            text += f"• <code>[{date}]</code> {amount} BDT - {status}\n"

        await message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


# ── Information commands ──────────────────────────────────────────────────────

@Client.on_message(
    filters.command("profile") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUPPORT_ADMIN)
async def handle_profile_command(client: Client, message: Message) -> None:
    """/profile [user_id] — Detailed user profile."""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        db = DatabaseManager.get_db()
        user_doc = await db["users"].find_one({"_id": target_id})
        if not user_doc:
            await message.reply_text("❌ User not found.")
            return

        sub_service = SubscriptionService()
        sub = await sub_service.get_subscription(target_id)
        
        text = (
            f"👤 <b>User Profile: {target_id}</b>\n\n"
            f"<b>Name:</b> {user_doc.get('full_name', 'Unknown')}\n"
            f"<b>Username:</b> @{user_doc.get('username', '-')}\n"
            f"<b>Banned:</b> {'Yes 🚫' if user_doc.get('is_banned') else 'No'}\n"
            f"<b>Muted:</b> {'Yes 🔇' if user_doc.get('is_muted') else 'No'}\n"
            f"<b>Warnings:</b> {user_doc.get('warning_count', 0)}\n"
            f"<b>Subscription:</b> {sub.plan.value.upper() if sub else 'FREE'} ({sub.status.value if sub else 'N/A'})\n"
        )
        if sub and sub.expires_at:
            text += f"<b>Expires:</b> {sub.expires_at.strftime('%Y-%m-%d')}\n"

        await message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("history") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUPPORT_ADMIN)
async def handle_history_command(client: Client, message: Message) -> None:
    """/history [user_id] — Recent activity history."""
    try:
        target_id = await _resolve_target_user(message)
        if not target_id:
            await message.reply_text("❌ Could not resolve target user.")
            return

        db = DatabaseManager.get_db()
        # Activity collection usually stores audit trail
        cursor = db["activity"].find({"user_id": target_id}).sort("timestamp", -1).limit(10)
        activities = await cursor.to_list(length=10)

        if not activities:
            await message.reply_text("No recent activity found.")
            return

        text = f"📜 <b>Recent Activity for {target_id}</b>\n\n"
        for a in activities:
            ts = a.get("timestamp")
            date = ts.strftime("%Y-%m-%d %H:%M") if ts else "Unknown"
            action = a.get("action", "unknown").upper()
            text += f"• <code>[{date}]</code> {action}\n"

        await message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@Client.on_message(
    filters.command("note") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_note_command(client: Client, message: Message) -> None:
    """/note <text> — Adds a private staff note to the user topic."""
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.reply_text("❌ This command must be used inside a user topic.")
        return

    text = " ".join(message.command[1:])
    if not text:
        await message.reply_text("❌ Usage: `/note <text>`")
        return

    from app.services.topic_manager import get_topic_manager
    topic_doc = await get_topic_manager().get_user_by_topic(thread_id)
    if not topic_doc:
        await message.reply_text("❌ User mapping not found.")
        return

    user_id = topic_doc["user_id"]
    db = DatabaseManager.get_db()
    await db["staff_notes"].insert_one({
        "user_id": user_id,
        "admin_id": message.from_user.id,
        "note": text,
        "created_at": datetime.now(timezone.utc)
    })

    await message.reply_text("📌 <b>Staff Note Added</b>", parse_mode=ParseMode.HTML)


@Client.on_message(
    filters.command("notes") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.MODERATOR)
async def handle_notes_command(client: Client, message: Message) -> None:
    """/notes [user_id] — Lists all staff notes for the user."""
    target_id = await _resolve_target_user(message)
    if not target_id:
        await message.reply_text("❌ Could not resolve target user.")
        return

    db = DatabaseManager.get_db()
    cursor = db["staff_notes"].find({"user_id": target_id}).sort("created_at", -1)
    notes = await cursor.to_list(length=20)

    if not notes:
        await message.reply_text("No notes found for this user.")
        return

    text = f"📝 <b>Staff Notes for {target_id}</b>\n\n"
    for n in notes:
        date = n["created_at"].strftime("%Y-%m-%d")
        text += f"• <code>[{date}]</code> {n['note']}\n"

    await message.reply_text(text, parse_mode=ParseMode.HTML)


# ── Stats & Utility ───────────────────────────────────────────────────────────

@Client.on_message(
    filters.command("stats") & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUPPORT_ADMIN)
async def handle_stats_command(client: Client, message: Message) -> None:
    """/stats — System-wide statistics."""
    try:
        db = DatabaseManager.get_db()
        user_count = await db["users"].count_documents({})
        sub_count = await db["subscriptions"].count_documents({"status": "ACTIVE"})
        text = (
            "📊 <b>System Statistics</b>\n\n"
            f"👤 <b>Users:</b> {user_count}\n"
            f"💎 <b>Active Subs:</b> {sub_count}\n"
        )
        await message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")
