from __future__ import annotations

import asyncio

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import ChatMemberUpdated, Message

from app.config import settings
from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

_WELCOME_DELETE_SECONDS = 60
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _get_welcome_config(chat_id: int) -> dict | None:
    """Fetch per-chat welcome config from bot_config collection."""
    try:
        db = DatabaseManager.get_db()
        return await db["bot_config"].find_one({"key": f"welcome:{chat_id}"})
    except Exception as e:
        logger.warning("Failed to fetch welcome config", extra={"ctx_error": str(e)})
        return None


async def _set_welcome_config(chat_id: int, text: str, enabled: bool = True) -> None:
    db = DatabaseManager.get_db()
    await db["bot_config"].update_one(
        {"key": f"welcome:{chat_id}"},
        {"$set": {"key": f"welcome:{chat_id}", "value": text, "enabled": enabled, "chat_id": chat_id}},
        upsert=True,
    )


# ── Auto-delete helper ────────────────────────────────────────────────────────

async def _delete_after(message: Message, delay: float = _WELCOME_DELETE_SECONDS) -> None:
    """Delete a message after `delay` seconds. Best-effort, never raises."""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass


# ── Welcome sender ────────────────────────────────────────────────────────────

async def _send_welcome(client: Client, update: ChatMemberUpdated) -> None:
    config = await _get_welcome_config(update.chat.id)
    if not config or not config.get("enabled"):
        return

    welcome_text = config.get("value", "")
    if not welcome_text:
        return

    user = update.new_chat_member.user
    name = user.first_name or "there"
    username_mention = f"@{user.username}" if user.username else f"<a href='tg://user?id={user.id}'>{name}</a>"

    # Substitute placeholders
    text = (
        welcome_text
        .replace("{name}", name)
        .replace("{mention}", username_mention)
        .replace("{chat}", update.chat.title or "")
    )

    for attempt in range(3):
        try:
            sent = await client.send_message(
                chat_id=update.chat.id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            # Schedule auto-delete — runs as a background task
            asyncio.create_task(
                _delete_after(sent, _WELCOME_DELETE_SECONDS),
                name=f"welcome-delete-{sent.id}",
            )
            logger.info(
                "Welcome sent",
                extra={
                    "ctx_user_id": user.id,
                    "ctx_chat_id": update.chat.id,
                    "ctx_msg_id": sent.id,
                },
            )
            return
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except RPCError as e:
            logger.warning("Failed to send welcome", extra={"ctx_error": str(e), "ctx_attempt": attempt + 1})
            if attempt == 2:
                return
            await asyncio.sleep(2 ** attempt)


# ── Membership update handler ─────────────────────────────────────────────────

@Client.on_chat_member_updated()
async def handle_new_member_welcome(client: Client, update: ChatMemberUpdated) -> None:
    """Trigger welcome message when a new member joins a tracked group."""
    # Only act on joins (not leaves, kicks, or role changes)
    if not update.new_chat_member or not update.old_chat_member:
        return

    from pyrogram.enums import ChatMemberStatus

    old_status = update.old_chat_member.status
    new_status = update.new_chat_member.status

    inactive = {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED}
    active = {ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED, ChatMemberStatus.ADMINISTRATOR}

    is_join = old_status in inactive and new_status in active
    if not is_join:
        return

    # Don't welcome in the verification hub or vault
    managed_internal = {settings.VERIFICATION_GROUP_ID, settings.VAULT_CHANNEL_ID}
    if update.chat.id in managed_internal:
        return

    await _send_welcome(client, update)


# ── Admin command: /setwelcome ────────────────────────────────────────────────

def _is_admin(user_id: int) -> bool:
    return (
        user_id == settings.OWNER_ID
        or user_id in settings.ADMIN_IDS
        or user_id in settings.SUDO_IDS
    )


@Client.on_message(filters.command("setwelcome") & (filters.group | filters.private))
async def handle_set_welcome(client: Client, message: Message) -> None:
    if not message.from_user or not _is_admin(message.from_user.id):
        return

    # /setwelcome <text>   OR   reply to a message with /setwelcome
    parts = message.text.split(None, 1)
    welcome_text = parts[1].strip() if len(parts) > 1 else ""

    if not welcome_text and message.reply_to_message:
        welcome_text = message.reply_to_message.text or message.reply_to_message.caption or ""

    if not welcome_text:
        await message.reply_text(
            "Usage: <code>/setwelcome Your welcome message here</code>\n\n"
            "Placeholders: <code>{name}</code> <code>{mention}</code> <code>{chat}</code>\n\n"
            "Example:\n"
            "<code>/setwelcome Welcome {mention}! 👋</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    chat_id = message.chat.id
    await _set_welcome_config(chat_id, welcome_text, enabled=True)

    confirm = await message.reply_text(
        f"✅ Welcome message set.\n\n"
        f"<b>Preview:</b>\n{welcome_text}\n\n"
        f"<i>Auto-deletes after {_WELCOME_DELETE_SECONDS}s.</i>",
        parse_mode=ParseMode.HTML,
    )
    # Clean up the confirm message too
    asyncio.create_task(_delete_after(confirm, 15.0))
    asyncio.create_task(_delete_after(message, 15.0))

    logger.info(
        "/setwelcome configured",
        extra={"ctx_chat_id": chat_id, "ctx_admin": message.from_user.id},
    )


@Client.on_message(filters.command("delwelcome") & (filters.group | filters.private))
async def handle_del_welcome(client: Client, message: Message) -> None:
    if not message.from_user or not _is_admin(message.from_user.id):
        return

    db = DatabaseManager.get_db()
    await db["bot_config"].update_one(
        {"key": f"welcome:{message.chat.id}"},
        {"$set": {"enabled": False}},
    )
    confirm = await message.reply_text("✅ Welcome message disabled.")
    asyncio.create_task(_delete_after(confirm, 10.0))
    asyncio.create_task(_delete_after(message, 10.0))