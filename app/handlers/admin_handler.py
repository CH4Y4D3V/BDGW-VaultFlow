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
    
    # 2. Kick from groups (optional, but good practice)
    # 3. Notify User (silent per spec §21? No, §21 says silent for BOT ban, 
    # but manual admin ban usually notifies. Spec §21: "Bot Ban: Silent").
    # We follow §21 and stay silent.

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
    await db["users"].update_one(
        {"user_id": target_id},
        {"$set": {"is_muted": True, "mute_until": datetime.now(timezone.utc) + asyncio.timedelta(minutes=minutes)}}
    )

    await message.reply_text(f"✅ User <code>{target_id}</code> muted for {minutes}m.", parse_mode=ParseMode.HTML)
    # Audit...

@Client.on_message(filters.command("paymentdone") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_paymentdone(client: Client, message: Message) -> None:
    """Handles the /paymentdone command: Alias for Approve."""
    # Logic to find the active payment session in this thread
    await message.reply_text("ℹ️ Please use the <b>✅ Approve Payment</b> button on the session card.", parse_mode=ParseMode.HTML)

@Client.on_message(filters.command("profile") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_profile(client: Client, message: Message) -> None:
    """Shows user profile and stats."""
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/profile <user_id>`")
        return
    # TODO: Implement profile card build
    await message.reply_text("Profile card coming soon.")

@Client.on_message(filters.command("grant") & filters.group & filters.chat(settings.VERIFICATION_GROUP_ID))
@permission_required(Role.ADMIN)
async def handle_grant(client: Client, message: Message) -> None:
    """Handles manual subscription grant."""
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/grant <user_id> <plan_id> [days]`")
        return
    # TODO: Implement manual grant logic
    await message.reply_text("Manual grant coming soon.")
