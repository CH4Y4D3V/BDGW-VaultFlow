from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError, UserIsBlocked, InputUserDeactivated, PeerIdInvalid
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import Role, permission_required, is_moderator
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3

# ── Broadcast state tracking ──────────────────────────────────────────────────
# Key: admin_id → {"step": str, "content": list[Message]}
_broadcast_states: dict[int, dict] = {}

# Album buffering for broadcast content collection
_broadcast_album_buffer: dict[str, list[Message]] = defaultdict(list)
_broadcast_album_tasks: dict[str, asyncio.Task] = {}
_broadcast_album_lock = asyncio.Lock()

_ALBUM_FLUSH_DELAY = 3.0  # seconds to wait for album completion


# ── Helpers ───────────────────────────────────────────────────────────────────

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
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except RPCError as e:
            logger.warning("_safe_reply RPCError", extra={"ctx_error": str(e)})
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error("_safe_reply unexpected", extra={"ctx_error": str(e)}, exc_info=True)
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
    return False


async def _probe_mongodb() -> tuple[str, Optional[float]]:
    try:
        db = DatabaseManager.get_db()
        t0 = time.monotonic()
        await db.command("ping")
        latency_ms = round((time.monotonic() - t0) * 1000, 2)
        return "🟢 Connected", latency_ms
    except RuntimeError as e:
        return f"🔴 Not initialised: {e}", None
    except Exception as e:
        return f"🔴 Error: {e}", None


async def _get_all_broadcast_user_ids() -> list[int]:
    """
    Collect all unique user IDs known to the bot.
    Queries subscriptions + memberships + activity collections.
    Deduplicates. Excludes owner and sudo users (they receive it anyway).
    """
    db = DatabaseManager.get_db()
    user_ids: set[int] = set()

    for collection_name in ("subscriptions", "memberships", "activity"):
        try:
            col = db[collection_name]
            docs = await col.distinct("user_id")
            user_ids.update(int(uid) for uid in docs if uid)
        except Exception as e:
            logger.warning(
                "broadcast: failed to query collection",
                extra={"ctx_collection": collection_name, "ctx_error": str(e)},
            )

    return list(user_ids)


async def _send_broadcast_to_user(
    client: Client,
    user_id: int,
    messages: list[Message],
) -> bool:
    """
    Send broadcast content to a single user.
    Handles all content types: text, photo, video, audio, voice,
    document, animation, sticker, video_note, and media_group (album).
    Returns True on success, False if user is unreachable.
    """
    if not messages:
        return False

    # Album (media_group) — use copy_media_group
    if len(messages) > 1:
        first = messages[0]
        try:
            await client.copy_media_group(
                chat_id=user_id,
                from_chat_id=first.chat.id,
                message_id=first.id,
            )
            return True
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
            try:
                await client.copy_media_group(
                    chat_id=user_id,
                    from_chat_id=first.chat.id,
                    message_id=first.id,
                )
                return True
            except Exception:
                return False
        except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
            return False
        except Exception as e:
            logger.debug("broadcast album failed", extra={"ctx_user": user_id, "ctx_error": str(e)})
            return False

    # Single message — use copy_message (preserves all types + captions)
    msg = messages[0]
    for attempt in range(2):
        try:
            await client.copy_message(
                chat_id=user_id,
                from_chat_id=msg.chat.id,
                message_id=msg.id,
            )
            return True
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
        except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
            return False
        except RPCError as e:
            if attempt == 1:
                logger.debug(
                    "broadcast single failed",
                    extra={"ctx_user": user_id, "ctx_error": str(e)},
                )
            await asyncio.sleep(1)
        except Exception as e:
            logger.debug("broadcast unexpected", extra={"ctx_user": user_id, "ctx_error": str(e)})
            return False
    return False


# ── Broadcast content filter ──────────────────────────────────────────────────
# Only matches private messages from admins who are in broadcast state.

def _admin_in_broadcast_state(_, __, message: Message) -> bool:
    if not message.from_user:
        return False
    admin_id = message.from_user.id
    state = _broadcast_states.get(admin_id)
    return state is not None and state.get("step") == "waiting_content"


_broadcast_content_filter = filters.create(_admin_in_broadcast_state)


# ── Handlers ──────────────────────────────────────────────────────────────────

@filters.command("ping")
@permission_required(Role.MODERATOR)
async def handle_ping(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_ping entered",
        extra={"ctx_from_user": message.from_user.id if message.from_user else None},
    )
    db_status, latency_ms = await _probe_mongodb()
    latency_line = f" <code>({latency_ms} ms)</code>" if latency_ms is not None else ""
    await _safe_reply(message, f"🏓 <b>Pong!</b>\n\n<b>Database:</b> {db_status}{latency_line}")


@Client.on_message(filters.command("ping"))
@permission_required(Role.MODERATOR)
async def _handle_ping(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_ping entered",
        extra={"ctx_from_user": message.from_user.id if message.from_user else None},
    )
    try:
        db_status, latency_ms = await _probe_mongodb()
        latency_line = f" <code>({latency_ms} ms)</code>" if latency_ms is not None else ""
        await _safe_reply(message, f"🏓 <b>Pong!</b>\n\n<b>Database:</b> {db_status}{latency_line}")
    except Exception as e:
        logger.error("handle_ping unhandled", extra={"ctx_error": str(e)}, exc_info=True)


@Client.on_message(filters.command("status"))
@permission_required(Role.MODERATOR)
async def handle_status(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_status entered",
        extra={"ctx_from_user": message.from_user.id if message.from_user else None},
    )
    try:
        try:
            me = await client.get_me()
            bot_display = f"@{me.username}" if me.username else f"ID <code>{me.id}</code>"
            bot_name = me.first_name or "Unknown"
        except Exception:
            bot_display = bot_name = "Unknown"

        from app.services.submission_service import get_pending_count
        pending_count = get_pending_count()
        admin_count = len(set(settings.ADMIN_IDS) | set(settings.SUDO_IDS))

        text = (
            f"📊 <b>VaultFlow — Runtime Status</b>\n\n"
            f"<b>Bot:</b> {bot_name} ({bot_display})\n"
            f"<b>Verification Group:</b> <code>{settings.VERIFICATION_GROUP_ID}</code>\n"
            f"<b>Vault Channel:</b> <code>{settings.VAULT_CHANNEL_ID}</code>\n"
            f"<b>NSFW Group:</b> <code>{settings.NSFW_GROUP_ID}</code>\n"
            f"<b>Premium Group:</b> <code>{settings.PREMIUM_GROUP_ID}</code>\n"
            f"<b>Owner ID:</b> <code>{settings.OWNER_ID}</code>\n"
            f"<b>Privileged users:</b> {admin_count}\n"
            f"<b>Pending submissions:</b> {pending_count}\n"
            f"<b>Log level:</b> {settings.LOG_LEVEL}\n"
            f"<b>Debug mode:</b> {'✅ On' if settings.DEBUG else '❌ Off'}"
        )
        await _safe_reply(message, text)
    except Exception as e:
        logger.error("handle_status unhandled", extra={"ctx_error": str(e)}, exc_info=True)


@Client.on_message(filters.command("handlers"))
@permission_required(Role.MODERATOR)
async def handle_list_handlers(client: Client, message: Message) -> None:
    logger.info(
        "HANDLER: handle_list_handlers entered",
        extra={"ctx_from_user": message.from_user.id if message.from_user else None},
    )
    try:
        dispatcher = getattr(client, "dispatcher", None)
        if dispatcher is None:
            await message.reply_text("⚠️ dispatcher is None")
            return
        groups = getattr(dispatcher, "groups", None)
        if groups is None:
            await message.reply_text("⚠️ dispatcher.groups is None")
            return

        lines = ["📋 <b>Registered Handler Groups</b>\n"]
        total = 0
        for group_id in sorted(groups.keys()):
            handlers = groups[group_id]
            total += len(handlers)
            lines.append(f"\n<b>Group {group_id}</b> — {len(handlers)} handler(s):")
            for h in handlers:
                cb = getattr(h, "callback", None)
                if cb:
                    name = getattr(cb, "__name__", "?")
                    mod = getattr(cb, "__module__", "?")
                    lines.append(f"  • <code>{mod}.{name}</code>")
        lines.append(f"\n<b>Total: {total} handlers</b>")
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3990] + "\n<i>…truncated</i>"
        await message.reply_text(text, parse_mode="html")
    except Exception as e:
        logger.error("handle_list_handlers error", exc_info=True)
        await message.reply_text(f"⚠️ Error: <code>{e}</code>", parse_mode="html")


# ── /broadcast ────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("broadcast") & filters.private)
@permission_required(Role.MODERATOR)
async def handle_broadcast_start(client: Client, message: Message) -> None:
    """Step 1: Enter broadcast mode."""
    logger.info(
        "HANDLER: handle_broadcast_start entered",
        extra={"ctx_from_user": message.from_user.id if message.from_user else None},
    )
    admin_id = message.from_user.id
    _broadcast_states[admin_id] = {"step": "waiting_content", "content": []}

    await message.reply_text(
        "📢 <b>Broadcast Mode</b>\n\n"
        "Send the content you want to broadcast to all users.\n\n"
        "Supports all types: text, photo, video, audio, voice,\n"
        "document, GIF, sticker, video note, and albums.\n\n"
        "Send /cancel to abort.",
        parse_mode=ParseMode.HTML,
    )


@Client.on_message(filters.command("cancel") & filters.private)
async def handle_broadcast_cancel(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    admin_id = message.from_user.id
    if admin_id in _broadcast_states:
        del _broadcast_states[admin_id]
        await message.reply_text("✅ Broadcast cancelled.")


@Client.on_message(filters.private & _broadcast_content_filter)
async def handle_broadcast_content(client: Client, message: Message) -> None:
    """
    Step 2: Collect the content the admin wants to broadcast.
    Supports single messages and albums (media_group).
    After collection, shows a preview and asks for confirmation.
    """
    if not message.from_user:
        return

    admin_id = message.from_user.id
    state = _broadcast_states.get(admin_id)
    if not state or state.get("step") != "waiting_content":
        return

    media_group_id = message.media_group_id

    if media_group_id:
        # Buffer album messages and flush after timeout
        buffer_key = f"bcast_{admin_id}_{media_group_id}"
        async with _broadcast_album_lock:
            _broadcast_album_buffer[buffer_key].append(message)
            existing = _broadcast_album_tasks.get(buffer_key)
            if existing and not existing.done():
                existing.cancel()
            task = asyncio.create_task(
                _flush_broadcast_album(client, admin_id, buffer_key),
                name=f"bcast-album-{buffer_key}",
            )
            _broadcast_album_tasks[buffer_key] = task
    else:
        # Single message — go straight to preview
        await _show_broadcast_preview(client, admin_id, [message])


async def _flush_broadcast_album(
    client: Client,
    admin_id: int,
    buffer_key: str,
) -> None:
    """Wait for album completion then show preview."""
    try:
        await asyncio.sleep(_ALBUM_FLUSH_DELAY)
    except asyncio.CancelledError:
        return

    async with _broadcast_album_lock:
        messages = _broadcast_album_buffer.pop(buffer_key, [])
        _broadcast_album_tasks.pop(buffer_key, None)

    if not messages:
        return

    messages.sort(key=lambda m: m.id)
    await _show_broadcast_preview(client, admin_id, messages)


async def _show_broadcast_preview(
    client: Client,
    admin_id: int,
    messages: list[Message],
) -> None:
    """Step 3: Show broadcast preview and confirmation buttons."""
    state = _broadcast_states.get(admin_id)
    if not state:
        return

    # Store content in state
    state["step"] = "confirm"
    state["content"] = messages

    count = len(messages)
    content_type = _describe_content(messages)

    await client.send_message(
        chat_id=admin_id,
        text=(
            f"📋 <b>Broadcast Preview</b>\n\n"
            f"Content: {content_type} ({count} message{'s' if count > 1 else ''})\n\n"
            "This will be sent to all known users.\n\n"
            "<b>Confirm broadcast?</b>"
        ),
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Send Now", callback_data="broadcast:confirm"),
            InlineKeyboardButton("❌ Cancel", callback_data="broadcast:cancel"),
        ]]),
        parse_mode=ParseMode.HTML,
    )


def _describe_content(messages: list[Message]) -> str:
    if not messages:
        return "unknown"
    if len(messages) > 1:
        return f"album ({len(messages)} items)"
    msg = messages[0]
    if msg.text:
        return "text"
    if msg.photo:
        return "photo"
    if msg.video:
        return "video"
    if msg.audio:
        return "audio"
    if msg.voice:
        return "voice"
    if msg.document:
        return "document"
    if msg.animation:
        return "GIF/animation"
    if msg.sticker:
        return "sticker"
    if msg.video_note:
        return "video note"
    return "media"


@Client.on_callback_query(filters.regex(r"^broadcast:(confirm|cancel)$"))
async def handle_broadcast_confirm(client: Client, callback: CallbackQuery) -> None:
    """Step 4: Execute or cancel the broadcast."""
    action = callback.data.split(":")[1]
    admin_id = callback.from_user.id

    if not is_moderator(admin_id):
        await callback.answer("Unauthorized.", show_alert=True)
        return

    state = _broadcast_states.get(admin_id)

    if action == "cancel" or not state:
        _broadcast_states.pop(admin_id, None)
        await callback.answer("Broadcast cancelled.")
        try:
            await callback.message.edit_text("❌ Broadcast cancelled.")
        except Exception:
            pass
        return

    messages = state.get("content", [])
    if not messages:
        _broadcast_states.pop(admin_id, None)
        await callback.answer("No content to broadcast.", show_alert=True)
        return

    # Clear state before long operation
    _broadcast_states.pop(admin_id, None)
    await callback.answer("Broadcasting...")

    try:
        await callback.message.edit_text("📡 Broadcast in progress...")
    except Exception:
        pass

    # Run broadcast in background so callback doesn't time out
    asyncio.create_task(
        _run_broadcast(client, admin_id, messages, callback.message),
        name=f"broadcast-{admin_id}",
    )


async def _run_broadcast(
    client: Client,
    admin_id: int,
    messages: list[Message],
    status_message: Message,
) -> None:
    """Execute the actual broadcast to all users."""
    user_ids = await _get_all_broadcast_user_ids()

    if not user_ids:
        try:
            await status_message.edit_text("⚠️ No users found to broadcast to.")
        except Exception:
            pass
        return

    total = len(user_ids)
    sent = 0
    failed = 0
    blocked = 0

    logger.info(
        "Broadcast started",
        extra={"ctx_admin": admin_id, "ctx_total": total},
    )

    for i, user_id in enumerate(user_ids):
        # Skip the admin who initiated the broadcast
        if user_id == admin_id:
            continue

        success = await _send_broadcast_to_user(client, user_id, messages)
        if success:
            sent += 1
        else:
            failed += 1

        # Progress update every 50 users
        if (i + 1) % 50 == 0:
            try:
                await status_message.edit_text(
                    f"📡 Broadcast in progress...\n"
                    f"Progress: {i + 1}/{total}\n"
                    f"Sent: {sent} | Failed: {failed}"
                )
            except Exception:
                pass

        # Throttle: 25 messages/second max (Telegram limit ~30/s globally)
        await asyncio.sleep(0.04)

    # Final report
    report = (
        f"✅ <b>Broadcast Complete</b>\n\n"
        f"Total users: {total}\n"
        f"Successfully sent: {sent}\n"
        f"Failed/unreachable: {failed}"
    )
    try:
        await status_message.edit_text(report, parse_mode=ParseMode.HTML)
    except Exception:
        try:
            await client.send_message(
                chat_id=admin_id,
                text=report,
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    logger.info(
        "Broadcast completed",
        extra={"ctx_admin": admin_id, "ctx_sent": sent, "ctx_failed": failed, "ctx_total": total},
    )


# ── Admin menu callbacks ──────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^admin:(dashboard|moderation)$"))
async def handle_admin_menu_callbacks(client: Client, callback: CallbackQuery) -> None:
    action = callback.data.split(":")[1]
    user_id = callback.from_user.id

    if not is_moderator(user_id):
        await callback.answer("⛔ Unauthorised.", show_alert=True)
        return

    await callback.answer()

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
                "<i>Use /status or /handlers for diagnostics.</i>"
            )
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data="menu:home"),
            ]])
            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

        elif action == "moderation":
            await callback.answer(
                "Check the verification group for active submissions.",
                show_alert=True,
            )
    except Exception as e:
        logger.error(
            "admin_menu_callback error",
            extra={"ctx_user_id": user_id, "ctx_action": action, "ctx_error": str(e)},
            exc_info=True,
        )
        await callback.answer("An error occurred.", show_alert=True)
