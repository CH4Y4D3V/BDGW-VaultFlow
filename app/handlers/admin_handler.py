from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import Role, permission_required, is_moderator
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3


async def _safe_reply(
    message: Message,
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
) -> bool:
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
@permission_required(Role.MODERATOR)
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


@Client.on_message(filters.command("handlers"))
@permission_required(Role.MODERATOR)
async def handle_list_handlers(client: Client, message: Message) -> None:
    """
    Runtime handler inventory check.
    Returns the full breakdown of registered Pyrogram handlers by group.
    Use this to confirm handler loading without a restart.
    """
    logger.info(
        "HANDLER: handle_list_handlers entered",
        extra={"ctx_from_user": message.from_user.id if message.from_user else None},
    )

    try:
        dispatcher = getattr(client, "dispatcher", None)
        if dispatcher is None:
            await message.reply_text(
                "⚠️ <code>client.dispatcher</code> is None — plugin system may not have initialised.",
                parse_mode="html",
            )
            return

        groups = getattr(dispatcher, "groups", None)
        if groups is None:
            await message.reply_text(
                "⚠️ <code>dispatcher.groups</code> is None.",
                parse_mode="html",
            )
            return

        lines = ["📋 <b>Registered Handler Groups</b>\n"]
        total = 0

        for group_id in sorted(groups.keys()):
            handlers = groups[group_id]
            group_total = len(handlers)
            total += group_total
            lines.append(f"\n<b>Group {group_id}</b> — {group_total} handler(s):")
            for h in handlers:
                cb = getattr(h, "callback", None)
                if cb:
                    name = getattr(cb, "__name__", "?")
                    mod = getattr(cb, "__module__", "?")
                    lines.append(f"  • <code>{mod}.{name}</code>")

        lines.append(f"\n<b>Total: {total} handlers across {len(groups)} group(s)</b>")

        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3990] + "\n<i>…truncated</i>"

        await message.reply_text(text, parse_mode="html")

    except Exception as e:
        logger.error("handle_list_handlers: error", exc_info=True)
        await message.reply_text(f"⚠️ Error: <code>{e}</code>", parse_mode="html")


@Client.on_message(filters.command("status"))
@permission_required(Role.MODERATOR)
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


@Client.on_callback_query(filters.regex(r"^admin:(dashboard|moderation)$"))
async def handle_admin_menu_callbacks(client: Client, callback_query: CallbackQuery) -> None:
    """Handles admin menu callbacks."""
    action = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id

    if not is_moderator(user_id):
        await callback_query.answer("⛔ Unauthorised.", show_alert=True)
        return

    await callback_query.answer()

    try:
        if action == "dashboard":
            from app.services.submission_service import get_pending_count
            pending_count = get_pending_count()
            admin_count = len(set(settings.ADMIN_IDS) | set(settings.SUDO_IDS))

            text = (
                "🛡 <b>ADMIN DASHBOARD</b>\n\n"
                f"<b>System Status:</b> 🟢 Operational\n"
                f"<b>Privileged Users:</b> {admin_count}\n"
                f"<b>Pending Review:</b> {pending_count}\n\n"
                "<i>Use commands /status or /handlers for deep diagnostics.</i>"
            )
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data="menu:home")
            ]])

            await callback_query.message.edit_text(
                text, reply_markup=keyboard, parse_mode=ParseMode.HTML
            )
            await callback_query.answer()

        elif action == "moderation":
            await callback_query.answer(
                "Check the verification group for active submissions.",
                show_alert=True,
            )

    except Exception as e:
        logger.error(
            "Error in admin menu callback",
            extra={"ctx_user_id": user_id, "ctx_action": action, "ctx_error": str(e)},
            exc_info=True,
        )
        await callback_query.answer("An error occurred.", show_alert=True)
