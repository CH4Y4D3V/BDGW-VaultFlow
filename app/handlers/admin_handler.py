from __future__ import annotations

import asyncio
import time

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import is_moderator
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3


async def _safe_reply(
    message: Message,
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
) -> bool:
    """
    RC-2 fix: catches ALL exception types.
    Returns True on success.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await message.reply_text(text, parse_mode=parse_mode)
            return True
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "_safe_reply: RPCError",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            # RC-2 fix
            logger.error(
                "_safe_reply: unexpected exception",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
                exc_info=True,
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
    return False


async def _probe_mongodb() -> tuple[str, float | None]:
    try:
        db = DatabaseManager.get_db()
        t0 = time.monotonic()
        await db.command("ping")
        latency_ms = round((time.monotonic() - t0) * 1000, 2)
        return "🟢 Connected", latency_ms
    except RuntimeError as e:
        return f"🔴 Not initialised: {e}", None
    except Exception as e:
        logger.warning("MongoDB ping failed", extra={"ctx_error": str(e)})
        return f"🔴 Error: {e}", None


@Client.on_message(filters.command("ping"))
async def handle_ping(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_ping entered",
        extra={
            "ctx_from_user": (
                message.from_user.id if message.from_user else None
            ),
            "ctx_chat_id": message.chat.id if message.chat else None,
        },
    )

    try:
        if not message.from_user or not is_moderator(message.from_user.id):
            return

        db_status, latency_ms = await _probe_mongodb()
        latency_line = (
            f" <code>({latency_ms} ms)</code>" if latency_ms is not None else ""
        )

        text = (
            f"🏓 <b>Pong!</b>\n\n"
            f"<b>Database:</b> {db_status}{latency_line}"
        )

        sent = await _safe_reply(message, text)
        logger.info(
            "/ping executed",
            extra={
                "ctx_user_id": message.from_user.id,
                "ctx_db_status": db_status,
                "ctx_latency_ms": latency_ms,
                "ctx_reply_sent": sent,
            },
        )

    except Exception as e:
        logger.error(
            "HANDLER: handle_ping unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )


@Client.on_message(filters.command("status"))
async def handle_status(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_status entered",
        extra={
            "ctx_from_user": (
                message.from_user.id if message.from_user else None
            ),
            "ctx_chat_id": message.chat.id if message.chat else None,
        },
    )

    try:
        if not message.from_user or not is_moderator(message.from_user.id):
            return

        try:
            me = await client.get_me()
            bot_display = (
                f"@{me.username}" if me.username else f"ID <code>{me.id}</code>"
            )
            bot_name = me.first_name or "Unknown"
        except Exception as e:
            bot_display = "Unknown"
            bot_name = "Unknown"
            logger.warning(
                "handle_status: failed to fetch bot identity",
                extra={"ctx_error": str(e)},
            )

        from app.services.submission_service import get_pending_count

        pending_count = get_pending_count()
        admin_count = len(set(settings.ADMIN_IDS) | set(settings.SUDO_IDS))

        text = (
            f"📊 <b>VaultFlow — Runtime Status</b>\n\n"
            f"<b>Bot:</b> {bot_name} ({bot_display})\n"
            f"<b>Verification Group:</b> "
            f"<code>{settings.VERIFICATION_GROUP_ID}</code>\n"
            f"<b>Vault Channel:</b> "
            f"<code>{settings.VAULT_CHANNEL_ID}</code>\n"
            f"<b>NSFW Group:</b> <code>{settings.NSFW_GROUP_ID}</code>\n"
            f"<b>Premium Group:</b> <code>{settings.PREMIUM_GROUP_ID}</code>\n"
            f"<b>Owner ID:</b> <code>{settings.OWNER_ID}</code>\n"
            f"<b>Privileged users:</b> {admin_count}\n"
            f"<b>Pending submissions:</b> {pending_count}\n"
            f"<b>Log level:</b> {settings.LOG_LEVEL}\n"
            f"<b>Debug mode:</b> "
            f"{'✅ On' if settings.DEBUG else '❌ Off'}"
        )

        sent = await _safe_reply(message, text)
        logger.info(
            "/status executed",
            extra={
                "ctx_user_id": message.from_user.id,
                "ctx_reply_sent": sent,
            },
        )

    except Exception as e:
        logger.error(
            "HANDLER: handle_status unhandled exception",
            extra={"ctx_error": str(e)},
            exc_info=True,
        )
