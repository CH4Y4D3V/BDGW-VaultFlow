"""
support_handler.py
──────────
Support chat handler.
"""

import logging
from html import escape
from datetime import datetime, timedelta, timezone
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.types import Message

from config import BotConfig
from database.repository import Database
from states import SupportFSM
from services.support_topics import build_accept_markup, forward_to_topic, notify_to_topic
from locales import get_user_lang, get_text, t

log = logging.getLogger(__name__)
router = Router(name="support")


def _support_session_minutes() -> int:
    import os
    raw = os.getenv("SUPPORT_SESSION_MINUTES", "")
    try:
        return max(1, int(raw)) if raw else 5
    except ValueError:
        return 5


# ── /support command — opens a support session ───────────────────────────────

@router.message(Command(commands=["admin", "support"]))
async def cmd_support(
    message: Message, state: FSMContext, db: Database, settings: BotConfig
) -> None:
    """
    User command: /support or /admin
    Opens a live support session. Admins are excluded.
    If a session is already active, reminds the user instead of opening a duplicate.

    On new session:
      1. DB: open_support_session (resets accepted_at — fresh gate)
      2. FSM: set SupportFSM.active
      3. Topic: post accept button so admin must explicitly accept before replying
      4. Admin DMs: notify as fallback (non-topic setups)
    """
    user_id = message.from_user.id

    # Admins do not open support sessions
    if user_id in settings.admin_ids:
        await message.answer(
            "ℹ️ You are an admin. Use <code>/closesupport &lt;user_id&gt;</code> "
            "to manage user sessions.",
            parse_mode="HTML",
        )
        return

    # Fetch lang before any branching so it is always in scope
    lang = (await get_user_lang(db, user_id)) or "en"

    # Check if session already active
    already_active = await db.is_support_session_active(user_id)
    if already_active:
        await message.answer(get_text("support_already_active", lang), parse_mode="HTML")
        return

    # Open session in DB with expiry and set FSM
    # open_support_session MUST reset accepted_at = NULL (see repository_additions.py)
    minutes = _support_session_minutes()
    expires_at = (
        datetime.now(timezone.utc) + timedelta(minutes=minutes)
    ).isoformat()
    await db.open_support_session(user_id, expires_at)
    await state.set_state(SupportFSM.active)

    log.info("[SUPPORT] User %d opened a support session.", user_id)

    await message.answer(get_text("support_session_started", lang), parse_mode="HTML")

    # ── Notify via topic (primary — includes accept gate button) ─────────────
    # This is the critical part of the accept flow: the topic receives the
    # new-session notice with the "✅ Accept Support" button. Until an admin
    # clicks it (or types /accept), their replies are blocked by the accept gate
    # in msg_admin_topic_reply.
    user = message.from_user
    user_link = f"tg://user?id={user_id}"
    accept_notice_text = (
        f"🔔 <b>New Support Request</b>\n\n"
        f"👤 <a href='{user_link}'>{escape(user.full_name)}</a> "
        f"(@{escape(user.username or 'no_username')}) "
        f"[<code>{user_id}</code>]\n\n"
        f"User is waiting for help.\n\n"
        f"⚠️ <b>Accept the session before replying — "
        f"your messages will NOT be forwarded until you do.</b>\n\n"
        f"👇 Click below or type <code>/accept</code>:"
    )
    if settings.admin_group_id:
        try:
            await notify_to_topic(
                bot=message.bot,
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

    # ── Fallback: DM notifications to individual admins ──────────────────────
    # Kept for non-topic setups or when admin_group_id is not configured.
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
            await message.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔔 <b>New Support Session</b>\n\n"
                    f"👤 {user_info}\n\n"
                    f"User has opened a support session and is waiting for help.\n"
                    f"Reply to their messages to respond directly.\n\n"
                    f"To close: <code>/closesupport {user_id}</code>"
                ),
                parse_mode="HTML",
            )
        except Exception as exc:
            log.warning("[SUPPORT] Could not notify admin chat %d: %s", chat_id, exc)


# ── Active session: forward user messages to admins ──────────────────────────

@router.message(SupportFSM.active, F.photo | (F.text & ~F.text.startswith("/")))
async def msg_support_forward(
    message: Message, state: FSMContext, bot: Bot, db: Database, settings: BotConfig
) -> None:
    user = message.from_user
    user_id = user.id

    lang = (await get_user_lang(db, user_id)) or "en"

    # ── Session expiry check — NOTIFY before clearing ──────────────────────────
    # Order matters: if the send fails, the user gets no notification.
    # Clearing FSM first would leave the user silently stuck.
    if not await db.is_support_session_active(user_id):
        await message.answer(get_text("support_session_expired", lang), parse_mode="HTML")
        await state.clear()
        await db.close_support_session(user_id)
        log.info("[SUPPORT] Session expired for user %d — notified, FSM cleared.", user_id)
        return

    # NOTE: No ban check here — BanCheckMiddleware silently blocks banned users
    # before this handler ever runs. Adding a secondary check here would send
    # a message to banned users (contradicting the silent-ban design) and would
    # clear FSM before notifying (wrong order). Middleware is authoritative.

    # ── PRIMARY: forward to forum topic ───────────────────────────────────────
    topic_ok = await forward_to_topic(bot, db, settings, message)

    if topic_ok:
        await message.reply(get_text("support_message_received", lang), parse_mode="HTML")
        return

    # ── FALLBACK: send to admin DMs ───────────────────────────────────────────
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
                sent = await bot.send_photo(
                    chat_id=chat_id,
                    photo=message.photo[-1].file_id,
                    caption=(
                        f"💬 <b>Support Message</b>\n\n"
                        f"👤 From: {user_info}\n\n"
                        f"Caption: {escape(caption_text)}"
                    ),
                    parse_mode="HTML",
                )
            else:
                sent = await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"💬 <b>Support Message</b>\n\n"
                        f"👤 From: {user_info}\n\n"
                        f"Message:\n{escape(message.text or '')}"
                    ),
                    parse_mode="HTML",
                )

            await db.store_support_message(
                user_id=user_id,
                admin_msg_id=sent.message_id,
                admin_chat_id=chat_id,
            )
            forwarded_count += 1
        except Exception as exc:
            log.error("[SUPPORT] Fallback DM failed to chat %d: %s", chat_id, exc)

    if forwarded_count > 0:
        await message.reply(get_text("support_message_received", lang), parse_mode="HTML")
    else:
        await message.reply(get_text("support_cant_reach", lang), parse_mode="HTML")


# ── Admin: close support session ──────────────────────────────────────────────

@router.message(Command("closesupport"))
async def cmd_close_support(
    message: Message, bot: Bot, db: Database, settings: BotConfig
) -> None:
    """
    Admin command: /closesupport <user_id>
                or /closesupport (as a reply to a forwarded support message)
    """
    if message.from_user.id not in settings.admin_ids:
        return

    user_id: Optional[int] = None

    parts = message.text.split()
    if len(parts) == 2 and parts[1].isdigit():
        user_id = int(parts[1])

    elif message.reply_to_message:
        replied_msg_id = message.reply_to_message.message_id
        chat_id = message.chat.id
        user_id = await db.get_support_user_id(
            admin_msg_id=replied_msg_id,
            admin_chat_id=chat_id,
        )
        if user_id is None:
            await message.answer(
                "❌ Could not resolve user from this message.\n\n"
                "Try: <code>/closesupport &lt;user_id&gt;</code>",
                parse_mode="HTML",
            )
            return

    else:
        await message.answer(
            "🔒 <b>Close Support Session</b>\n\n"
            "<b>Options:</b>\n"
            "• <code>/closesupport &lt;user_id&gt;</code>\n"
            "• Reply to a forwarded support message with <code>/closesupport</code>",
            parse_mode="HTML",
        )
        return

    if not isinstance(user_id, int) or user_id <= 0:
        await message.answer(f"❌ Invalid user ID: {user_id}")
        return

    user_row = await db.get_user(user_id)
    if user_row is None:
        await message.answer(
            f"⚠️ User {user_id} not found in database.\n"
            f"They may never have started the bot."
        )
        return

    await db.close_support_session(user_id)

    storage = bot.fsm_storage  # type: ignore[attr-defined]
    if storage:
        key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
        await storage.set_state(key=key, state=None)
        await storage.set_data(key=key, data={})

    log.info(
        "[SUPPORT] Admin %d closed session for user %d",
        message.from_user.id, user_id,
    )
    await message.answer(f"✅ Support session closed for user {user_id}.")

    try:
        lang = (await get_user_lang(db, user_id)) or "en"

        await bot.send_message(
            chat_id=user_id,
            text=get_text("support_session_closed_user", lang),
            parse_mode="HTML",
        )
    except Exception as exc:
        log.warning(
            "[SUPPORT] Could not notify user %d of close: %s", user_id, exc
        )