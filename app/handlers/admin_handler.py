# app/handlers/admin_handler.py — COMPLETE FIXED FILE
"""
Admin-only command handlers for the verification hub.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import Role, permission_required
from app.services.admin_logger import get_admin_logger
from app.services.subscription_service import SubscriptionService
from app.services.trust_service import TrustService
from app.ui.admin_cards import format_user_profile_card
from app.utils.logger import get_logger

logger = get_logger(__name__)


@Client.on_message(
    filters.command("accept")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_accept_command(client: Client, message: Message) -> None:
    await message.reply_text(
        "ℹ️ Please use the <b>✅ Accept Support</b> button on the request card.",
        parse_mode=ParseMode.HTML,
    )


@Client.on_message(
    filters.command("close")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_close_redirect(client: Client, message: Message) -> None:
    from app.handlers.support_handler import handle_close_command
    await handle_close_command(client, message)


@Client.on_message(
    filters.command("ban")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_ban(client: Client, message: Message) -> None:
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/ban <user_id> <reason>`")
        return

    try:
        target_id = int(message.command[1])
        reason = " ".join(message.command[2:])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    await db["users"].update_one(
        {"_id": target_id},
        {
            "$set": {
                "is_banned": True,
                "ban_reason": reason,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    premium_channel = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(settings, "PREMIUM_GROUP_ID", None)
    chats_to_kick = [
        c for c in [settings.NSFW_GROUP_ID, settings.PREMIUM_GROUP_ID, premium_channel]
        if c
    ]
    for group in set(chats_to_kick):
        try:
            await client.ban_chat_member(group, target_id)
        except Exception as e:
            logger.warning("ban_kick_failed", extra={"ctx_user_id": target_id, "ctx_chat": group, "ctx_error": str(e)})

    await message.reply_text(
        f"✅ User <code>{target_id}</code> has been banned.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="USER BANNED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details=f"Reason: {reason}",
    )


@Client.on_message(
    filters.command("unban")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_unban(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/unban <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    await db["users"].update_one(
        {"_id": target_id},
        {"$set": {"is_banned": False, "updated_at": datetime.now(timezone.utc)}},
    )

    premium_channel = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(settings, "PREMIUM_GROUP_ID", None)
    chats_to_unban = [
        c for c in [settings.NSFW_GROUP_ID, settings.PREMIUM_GROUP_ID, premium_channel]
        if c
    ]
    for group in set(chats_to_unban):
        try:
            await client.unban_chat_member(group, target_id)
        except Exception as e:
            logger.warning("unban_failed", extra={"ctx_user_id": target_id, "ctx_chat": group, "ctx_error": str(e)})

    await message.reply_text(
        f"✅ User <code>{target_id}</code> unbanned.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="USER UNBANNED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details="Manual unban by admin",
    )


@Client.on_message(
    filters.command("mute")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_mute(client: Client, message: Message) -> None:
    if len(message.command) < 4:
        await message.reply_text("❌ Usage: `/mute <user_id> <minutes> <reason>`")
        return

    try:
        target_id = int(message.command[1])
        minutes = int(message.command[2])
        reason = " ".join(message.command[3:])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid input.")
        return

    db = DatabaseManager.get_db()
    mute_until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    await db["users"].update_one(
        {"_id": target_id},
        {"$set": {"is_muted": True, "mute_until": mute_until}},
    )

    await message.reply_text(
        f"✅ User <code>{target_id}</code> muted for {minutes}m.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="USER MUTED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details=f"Muted for {minutes}m. Reason: {reason}",
    )


@Client.on_message(
    filters.command("paymentdone")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_paymentdone(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/paymentdone <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
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
        await message.reply_text(
            f"✅ Payment for user <code>{target_id}</code> approved.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await message.reply_text("❌ Approval failed. Session might be already processed.")


@Client.on_message(
    filters.command("profile")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_profile(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/profile <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    user_doc = await db["users"].find_one({"_id": target_id})
    if not user_doc:
        await message.reply_text("❌ User not found in database.")
        return

    sub_service = SubscriptionService()
    sub = await sub_service.get_subscription(target_id)

    trust_service = TrustService()
    metrics = await trust_service.get_user_metrics(target_id)

    card = format_user_profile_card(user_doc, sub, metrics)
    await message.reply_text(card, parse_mode=ParseMode.HTML)


@Client.on_message(
    filters.command("grant")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_grant(client: Client, message: Message) -> None:
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/grant <user_id> <plan_id> [days]`")
        return

    try:
        target_id = int(message.command[1])
        plan_str = message.command[2].lower()
        days = int(message.command[3]) if len(message.command) > 3 else None
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid input. Days must be a number.")
        return

    from app.models.subscription import Plan
    try:
        plan = Plan(plan_str)
    except ValueError:
        valid_plans = ", ".join([p.value for p in Plan])
        await message.reply_text(f"❌ Invalid plan. Choose from: {valid_plans}")
        return

    service = SubscriptionService()
    await service.grant(
        user_id=target_id,
        plan=plan,
        duration_days=days,
        granted_by=message.from_user.id,
        notes="Manual grant by admin",
    )

    await message.reply_text(
        f"✅ Granted <b>{plan.value.upper()}</b> to <code>{target_id}</code>.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="SUBSCRIPTION GRANTED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details=f"Manually granted {plan.value} ({days or 'lifetime'} days)",
    )


@Client.on_message(
    filters.command("revoke")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_revoke(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/revoke <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    service = SubscriptionService()
    await service.revoke(target_id, revoked_by=message.from_user.id)

    await message.reply_text(
        f"✅ Subscription revoked for <code>{target_id}</code>.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="SUBSCRIPTION REVOKED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details="Manual revocation by admin",
    )