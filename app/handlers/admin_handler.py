from __future__ import annotations

from pyrogram import Client, filters
from pyrogram.types import Message

from app.core.permissions import require_role
from app.models.subscription import Plan
from app.utils.logger import get_logger

logger = get_logger(__name__)


@Client.on_message(filters.command("ban") & filters.private)
@require_role(Plan.ADMIN)
async def handle_ban(client: Client, message: Message) -> None:
    """Handles the /ban command."""
    logger.info("handle_ban", extra={"ctx_admin_id": message.from_user.id})
    await message.reply_text("Banning user... (not implemented)")


@Client.on_message(filters.command("unban") & filters.private)
@require_role(Plan.ADMIN)
async def handle_unban(client: Client, message: Message) -> None:
    """Handles the /unban command."""
    logger.info("handle_unban", extra={"ctx_admin_id": message.from_user.id})
    await message.reply_text("Unbanning user... (not implemented)")


@Client.on_message(filters.command("mute") & filters.private)
@require_role(Plan.ADMIN)
async def handle_mute(client: Client, message: Message) -> None:
    """Handles the /mute command."""
    logger.info("handle_mute", extra={"ctx_admin_id": message.from_user.id})
    await message.reply_text("Muting user... (not implemented)")


@Client.on_message(filters.command("unmute") & filters.private)
@require_role(Plan.ADMIN)
async def handle_unmute(client: Client, message: Message) -> None:
    """Handles the /unmute command."""
    logger.info("handle_unmute", extra={"ctx_admin_id": message.from_user.id})
    await message.reply_text("Unmuting user... (not implemented)")


@Client.on_message(filters.command("grant") & filters.private)
@require_role(Plan.ADMIN)
async def handle_grant(client: Client, message: Message) -> None:
    """Handles the /grant command."""
    logger.info("handle_grant", extra={"ctx_admin_id": message.from_user.id})
    await message.reply_text("Granting subscription... (not implemented)")


@Client.on_message(filters.command("revoke") & filters.private)
@require_role(Plan.ADMIN)
async def handle_revoke(.py
    """Handles the /revoke command."""
    logger.info("handle_revoke", extra={"ctx_admin_id": message.from_user.id})
    await message.reply_text("Revoking subscription... (not implemented)")


@Client.on_message(filters.command("whois") & filters.private)
@require_role(Plan.ADMIN)
async def handle_whois(client: Client, message: Message) -> None:
    """Handles the /whois command."""
    logger.info("handle_whois", extra={"ctx_admin_id": message.from_user.id})
    await message.reply_text("Looking up user... (not implemented)")
