"""
support_handler.py
──────────
Support chat handler.
"""

import logging
from html import escape
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from pyrogram.errors import PeerIdInvalid

from app.config import settings
from app.core.database import DatabaseManager
from app.services.support_topics import build_accept_markup, forward_to_topic, notify_to_topic
from app.utils.logger import get_logger
from locales import get_user_lang, get_text


log = get_logger(__name__)

# In-memory FSM for support state. A more robust solution would use Redis.
user_states: Dict[int, str] = {}
SUPPORT_STATE_ACTIVE = "active"


def _support_session_minutes() -> int:
    import os
    raw = os.getenv("SUPPORT_SESSION_MINUTES", "")
    try:
        return max(1, int(raw)) if raw else 5
    except ValueError:
        return 5


@Client.on_message(filters.command(["admin", "support"]) & filters.private)
async def cmd_support(client: Client, message: Message):
    """
    User command: /support or /admin
    Opens a live support session. Admins are excluded.
    If a session is already active, reminds the user instead of opening a duplicate.

    On new session:
      1. DB: open_support_session (resets accepted_at — fresh gate)
      2. FSM: set support state to active
      3. Topic: post accept button so admin must explicitly accept before replying
      4. Admin DMs: notify as fallback (non-topic setups)
    """
    db = DatabaseManager.get_db()
    user_id = message.from_user.id

    if user_id in settings.admin_ids:
        await message.reply(
            "ℹ️ You are an admin. Use <code>/closesupport &lt;user_id&gt;</code> "
            "to manage user sessions.",
            parse_mode=ParseMode.HTML,
        )
        return

    lang = (await get_user_lang(db, user_id)) or "en"

    already_active = await db.is_support_session_active(user_id)
    if already_active:
        await message.reply(get_text("support_already_active", lang), parse_mode=ParseMode.HTML)
        return

    minutes = _support_session_minutes()
    expires_at = (
        datetime.now(timezone.utc) + timedelta(minutes=minutes)
    ).isoformat()
    await db.open_support_session(user_id, expires_at)
    user_states[user_id] = SUPPORT_STATE_ACTIVE

    log.info("[SUPPORT] User %d opened a support session.", user_id)

    await message.reply(get_text("support_session_started", lang), parse_mode=ParseMode.HTML)

    user = message.from_user
    user_link = f"tg://user?id={user_id}"
    accept_notice_text = (
        f"🔔 <b>New Support Request</b>

"
        f"👤 <a href='{user_link}'>{escape(user.full_name)}</a> "
        f"(@{escape(user.username or 'no_username')}) "
        f"[<code>{user_id}</code>]

"
        f"User is waiting for help.

"
        f"⚠️ <b>Accept the session before replying — "
        f"your messages will NOT be forwarded until you do.</b>

"
        f"👇 Click below or type <code>/accept</code>:"
    )

    if settings.admin_group_id:
        try:
            await notify_to_topic(
                bot=client,
                db=db,
                settings=settings,
                user_id=user_id,
                text=accept_notice_text,
                reply_markup=build_accept_markup(user_id),
            )
        except Exception as exc:
            log.warning(
                "[SUPPORT] Could not post accept notice to topic for user %d: %s",
                user_id, exc,
            )

    user_info = (
        f"{escape(user.full_name)} "
        f"(@{escape(user.username or 'no_username')}) "
        f"[<code>{user_id}</code>]"
    )

    targets: list[int] = list(settings.admin_ids)
    if (
        settings.admin_group_id is not None
        and settings.admin_group_id not in targets
    ):
        targets.append(settings.admin_group_id)

    for chat_id in targets:
        try:
            await client.send_message(
                chat_id=chat_id,
                text=(
                    f"🔔 <b>New Support Session</b>

"
                    f"👤 {user_info}

"
                    f"User has opened a support session and is waiting for help.
"
                    f"Reply to their messages to respond directly.

"
                    f"To close: <code>/closesupport {user_id}</code>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            log.warning("[SUPPORT] Could not notify admin chat %d: %s", chat_id, exc)


async def msg_support_forward(client: Client, message: Message):
    db = DatabaseManager.get_db()
    user = message.from_user
    user_id = user.id

    lang = (await get_user_lang(db, user_id)) or "en"

    if not await db.is_support_session_active(user_id):
        await message.reply(get_text("support_session_expired", lang), parse_mode=ParseMode.HTML)
        user_states.pop(user_id, None)
        await db.close_support_session(user_id)
        log.info("[SUPPORT] Session expired for user %d — notified, FSM cleared.", user_id)
        return

    topic_ok = await forward_to_topic(client, db, settings, message)

    if topic_ok:
        await message.reply(get_text("support_message_received", lang), parse_mode=ParseMode.HTML)
        return

    log.warning(
        "[SUPPORT] Topic route failed for user %d — falling back to DM notifications",
        user_id,
    )

    user_info = (
        f"{escape(user.full_name)} "
        f"(@{escape(user.username or 'no_username')}) "
        f"[<code>{user_id}</code>]"
    )

    targets: list[int] = list(settings.admin_ids)
    if (
        settings.admin_group_id is not None
        and settings.admin_group_id not in targets
    ):
        targets.append(settings.admin_group_id)

    forwarded_count = 0
    for chat_id in targets:
        try:
            if message.photo:
                caption_text = message.caption or "(photo only)"
                sent = await client.send_photo(
                    chat_id=chat_id,
                    photo=message.photo.file_id,
                    caption=(
                        f"💬 <b>Support Message</b>

"
                        f"👤 From: {user_info}

"
                        f"Caption: {escape(caption_text)}"
                    ),
                    parse_mode=ParseMode.HTML,
                )
            else:
                sent = await client.send_message(
                    chat_id=chat_id,
                    text=(
                        f"💬 <b>Support Message</b>

"
                        f"👤 From: {user_info}

"
                        f"Message:
{escape(message.text or '')}"
                    ),
                    parse_mode=ParseMode.HTML,
                )

            await db.store_support_message(
                user_id=user_id,
                admin_msg_id=sent.id,
                admin_chat_id=chat_id,
            )
            forwarded_count += 1
        except Exception as exc:
            log.error("[SUPPORT] Fallback DM failed to chat %d: %s", chat_id, exc)

    if forwarded_count > 0:
        await message.reply(get_text("support_message_received", lang), parse_mode=ParseMode.HTML)
    else:
        await message.reply(get_text("support_cant_reach", lang), parse_mode=ParseMode.HTML)


@Client.on_message((filters.text | filters.photo) & filters.private & ~filters.command, group=1)
async def private_message_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_states.get(user_id) == SUPPORT_STATE_ACTIVE:
        await msg_support_forward(client, message)


@Client.on_message(filters.command("closesupport") & filters.private)
async def cmd_close_support(client: Client, message: Message):
    """
    Admin command: /closesupport <user_id>
                or /closesupport (as a reply to a forwarded support message)
    """
    db = DatabaseManager.get_db()

    if message.from_user.id not in settings.admin_ids:
        return

    user_id: Optional[int] = None

    parts = message.text.split()
    if len(parts) == 2 and parts[1].isdigit():
        user_id = int(parts[1])
    elif message.reply_to_message:
        replied_msg_id = message.reply_to_message.id
        chat_id = message.chat.id
        user_id = await db.get_support_user_id(
            admin_msg_id=replied_msg_id,
            admin_chat_id=chat_id,
        )
        if user_id is None:
            await message.reply(
                "❌ Could not resolve user from this message.

"
                "Try: <code>/closesupport &lt;user_id&gt;</code>",
                parse_mode=ParseMode.HTML,
            )
            return
    else:
        await message.reply(
            "🔒 <b>Close Support Session</b>

"
            "<b>Options:</b>
"
            "• <code>/closesupport &lt;user_id&gt;</code>
"
            "• Reply to a forwarded support message with <code>/closesupport</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not isinstance(user_id, int) or user_id <= 0:
        await message.reply(f"❌ Invalid user ID: {user_id}")
        return

    user_row = await db.get_user(user_id)
    if user_row is None:
        await message.reply(
            f"⚠️ User {user_id} not found in database.
"
            f"They may never have started the bot."
        )
        return

    await db.close_support_session(user_id)
    user_states.pop(user_id, None)

    log.info(
        "[SUPPORT] Admin %d closed session for user %d",
        message.from_user.id, user_id,
    )
    await message.reply(f"✅ Support session closed for user {user_id}.")

    try:
        lang = (await get_user_lang(db, user_id)) or "en"
        await client.send_message(
            chat_id=user_id,
            text=get_text("support_session_closed_user", lang),
            parse_mode=ParseMode.HTML,
        )
    except PeerIdInvalid:
        log.warning(
            "[SUPPORT] Could not notify user %d of close: user has blocked the bot or account is deleted.", user_id
        )
    except Exception as exc:
        log.warning(
            "[SUPPORT] Could not notify user %d of close: %s", user_id, exc
        )
