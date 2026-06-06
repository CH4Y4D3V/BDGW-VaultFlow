from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import Role, permission_required
from app.models.subscription import Plan
from app.services.support_service import send_admin_log_entry
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Handlers (Section 9.5) ────────────────────────────────────────────────

@Client.on_message(filters.command("accept") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_accept_command(client: Client, message: Message) -> None:
    """Handles the /accept command: Alias for clicking the Accept button."""
    # This usually requires tracking which session is in which thread
    await message.reply_text("ℹ️ Please use the <b>✅ Accept Support</b> button on the request card.", parse_mode=ParseMode.HTML)

@Client.on_message(filters.command("close") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_close_redirect(client: Client, message: Message) -> None:
    """Redirects to support_handler.handle_close_command."""
    from app.handlers.support_handler import handle_close_command
    await handle_close_command(client, message)

@Client.on_message(filters.command("ban") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_ban(client: Client, message: Message) -> None:
    """Handles the /ban <user_id> <reason> command."""
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/ban <user_id> <reason>`")
        return

    try:
        target_id = int(message.command[1])
        reason = " ".join(message.command[2:])
    except ValueError:
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    # 1. Update DB
    await db["users"].update_one(
        {"user_id": target_id},
        {"$set": {"is_banned": True, "ban_reason": reason, "updated_at": datetime.now(timezone.utc)}}
    )
    
    # Kick from groups
    try:
        await client.ban_chat_member(settings.NSFW_GROUP_ID, target_id)
        await client.ban_chat_member(settings.PREMIUM_GROUP_ID, target_id)
        if settings.PREMIUM_CHANNEL_ID:
            await client.ban_chat_member(settings.PREMIUM_CHANNEL_ID, target_id)
    except Exception as e:
        logger.warning(f"Failed to kick banned user {target_id}: {e}")

    await message.reply_text(f"✅ User <code>{target_id}</code> has been banned.", parse_mode=ParseMode.HTML)
    
    # 4. Audit
    await send_admin_log_entry(
        client=client,
        action_type="USER BANNED",
        admin_user_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        target_name=None,
        target_username=None,
        detail=f"Reason: {reason}"
    )

@Client.on_message(filters.command("unban") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_unban(client: Client, message: Message) -> None:
    """Handles the /unban <user_id> command."""
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/unban <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except ValueError:
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    await db["users"].update_one(
        {"user_id": target_id},
        {"$set": {"is_banned": False, "updated_at": datetime.now(timezone.utc)}}
    )
    
    try:
        await client.unban_chat_member(settings.NSFW_GROUP_ID, target_id)
        await client.unban_chat_member(settings.PREMIUM_GROUP_ID, target_id)
        if settings.PREMIUM_CHANNEL_ID:
            await client.unban_chat_member(settings.PREMIUM_CHANNEL_ID, target_id)
    except Exception as e:
        logger.warning(f"Failed to unban user {target_id}: {e}")

    await message.reply_text(f"✅ User <code>{target_id}</code> unbanned.", parse_mode=ParseMode.HTML)
    
    await send_admin_log_entry(
        client=client,
        action_type="USER UNBANNED",
        admin_user_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        target_name=None,
        target_username=None,
        detail="Manual unban"
    )

@Client.on_message(filters.command("mute") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_mute(client: Client, message: Message) -> None:
    """Handles the /mute <user_id> <minutes> <reason> command."""
    if len(message.command) < 4:
        await message.reply_text("❌ Usage: `/mute <user_id> <minutes> <reason>`")
        return

    try:
        target_id = int(message.command[1])
        minutes = int(message.command[2])
        reason = " ".join(message.command[3:])
    except ValueError:
        await message.reply_text("❌ Invalid input.")
        return

    # Implement mute logic in DB
    db = DatabaseManager.get_db()
    mute_until = datetime.now(timezone.utc) + asyncio.timedelta(minutes=minutes)
    await db["users"].update_one(
        {"user_id": target_id},
        {"$set": {"is_muted": True, "mute_until": mute_until}}
    )

    await message.reply_text(f"✅ User <code>{target_id}</code> muted for {minutes}m.", parse_mode=ParseMode.HTML)
    
    await send_admin_log_entry(
        client=client,
        action_type="USER MUTED",
        admin_user_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        target_name=None,
        target_username=None,
        detail=f"Muted for {minutes}m. Reason: {reason}"
    )

@Client.on_message(filters.command("paymentdone") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_paymentdone(client: Client, message: Message) -> None:
    """Handles the /paymentdone <user_id> command: Alias for Approve."""
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/paymentdone <user_id>`")
        return
    
    try:
        target_id = int(message.command[1])
    except ValueError:
        await message.reply_text("❌ Invalid User ID.")
        return
        
    from app.payments import get_payment_service
    service = get_payment_service()
    session = await service.get_active_session(target_id)
    
    if not session:
        await message.reply_text("❌ No active payment session found for this user.")
        return
        
    success = await service.approve_payment(client, session.id, message.from_user.id)
    if success:
        await message.reply_text(f"✅ Payment for user <code>{target_id}</code> approved.", parse_mode=ParseMode.HTML)
    else:
        await message.reply_text("❌ Approval failed. Session might be already processed.")

@Client.on_message(filters.command("profile") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_profile(client: Client, message: Message) -> None:
    """Shows user profile and stats."""
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/profile <user_id>`")
        return
    
    try:
        target_id = int(message.command[1])
    except ValueError:
        await message.reply_text("❌ Invalid User ID.")
        return
        
    db = DatabaseManager.get_db()
    user = await db["users"].find_one({"user_id": target_id})
    if not user:
        await message.reply_text("❌ User not found in database.")
        return
        
    from app.services.subscription_service import SubscriptionService
    sub_service = SubscriptionService()
    sub = await sub_service.get_subscription(target_id)
    
    plan_label = "None"
    expiry = "N/A"
    if sub:
        plan_label = sub.plan.value.upper()
        expiry = sub.expires_at.strftime("%Y-%m-%d %H:%M") if sub.expires_at else "Lifetime"
        
    from app.services.trust_service import TrustService
    trust_service = TrustService()
    metrics = await trust_service.get_user_metrics(target_id)
    
    text = (
        f"👤 <b>User Profile:</b> <code>{target_id}</code>\n"
        f"📛 Name: {user.get('full_name', 'N/A')}\n"
        f"🔗 Username: @{user.get('username', 'N/A')}\n\n"
        f"💎 <b>Plan:</b> {plan_label}\n"
        f"⏰ Expiry: {expiry}\n\n"
        f"🛡 <b>Trust Level:</b> {metrics.get('level', 'NEW')}\n"
        f"📊 Trust Score: {metrics.get('trust_score', 0)}\n"
        f"🚩 Fraud Score: {metrics.get('fraud_score', 0)}\n\n"
        f"🚫 Banned: {'Yes' if user.get('is_banned') else 'No'}\n"
        f"🔇 Muted: {'Yes' if user.get('is_muted') else 'No'}"
    )
    await message.reply_text(text, parse_mode=ParseMode.HTML)

@Client.on_message(filters.command("grant") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_grant(client: Client, message: Message) -> None:
    """Handles manual subscription grant: /grant <user_id> <plan_id> [days]"""
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/grant <user_id> <plan_id> [days]`")
        return
        
    try:
        target_id = int(message.command[1])
        plan_str = message.command[2].lower()
        days = int(message.command[3]) if len(message.command) > 3 else None
    except ValueError:
        await message.reply_text("❌ Invalid input. Days must be a number.")
        return

    from app.models.subscription import Plan
    try:
        plan = Plan(plan_str)
    except ValueError:
        await message.reply_text(f"❌ Invalid plan. Choose from: {[p.value for p in Plan]}")
        return

    from app.services.subscription_service import SubscriptionService
    service = SubscriptionService()
    await service.grant(
        user_id=target_id,
        plan=plan,
        duration_days=days,
        granted_by=message.from_user.id,
        notes="Manual grant by admin"
    )
    
    await message.reply_text(f"✅ Granted <b>{plan.value.upper()}</b> to <code>{target_id}</code>.", parse_mode=ParseMode.HTML)
    
    await send_admin_log_entry(
        client=client,
        action_type="SUBSCRIPTION GRANTED",
        admin_user_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        target_name=None,
        target_username=None,
        detail=f"Manually granted {plan.value} ({days or 'lifetime'} days)"
    )

@Client.on_message(filters.command("revoke") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_revoke(client: Client, message: Message) -> None:
    """Handles subscription revocation: /revoke <user_id>"""
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/revoke <user_id>`")
        return
        
    try:
        target_id = int(message.command[1])
    except ValueError:
        await message.reply_text("❌ Invalid User ID.")
        return

    from app.services.subscription_service import SubscriptionService
    service = SubscriptionService()
    await service.revoke(target_id, revoked_by=message.from_user.id)
    
    await message.reply_text(f"✅ Subscription revoked for <code>{target_id}</code>.", parse_mode=ParseMode.HTML)
    
    await send_admin_log_entry(
        client=client,
        action_type="SUBSCRIPTION REVOKED",
        admin_user_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        target_name=None,
        target_username=None,
        detail="Manual revocation by admin"
    )
