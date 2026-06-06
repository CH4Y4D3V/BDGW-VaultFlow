from __future__ import annotations

from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, Message

from app.services.support_service import get_support_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

@Client.on_message(filters.command("help") & filters.private)
async def handle_help_command(client: Client, message: Message) -> None:
    """Handles the /help command, which opens a support request."""
    logger.info("handle_help_command", extra={"ctx_user_id": message.from_user.id})
    # TODO: Implement full support request creation
    await message.reply_text("Support system is under construction.")


@Client.on_callback_query(filters.regex(r"^support_accept:(\d+)$"))
async def handle_accept_callback(client: Client, callback_query: CallbackQuery) -> None:
    """Handles the 'support_accept' callback."""
    user_id = int(callback_query.matches[0].group(1))
    admin_id = callback_query.from_user.id
    logger.info("handle_accept_callback", extra={"ctx_user_id": user_id, "ctx_admin_id": admin_id})
    # TODO: Implement support session locking and notification
    await callback_query.answer("Accepting support session... (not implemented)")


@Client.on_message(filters.command("close") & filters.private) # This filter is probably wrong, it should be in the hub
async def handle_close_command(client: Client, message: Message) -> None:
    """Handles the /close command for support sessions."""
    logger.info("handle_close_command", extra={"ctx_user_id": message.from_user.id})
    # TODO: Implement support session closing
    await message.reply_text("Closing support session... (not implemented)")


# This function is for other handlers to call
async def handle_support_entry(client: Client, callback_query: CallbackQuery) -> None:
    """Handles the 'open_support' callback, opening a support request."""
    user_id = callback_query.from_user.id
    logger.info("handle_support_entry", extra={"ctx_user_id": user_id})
    # TODO: Implement full support request creation
    await callback_query.answer("Opening support... (not implemented)")
