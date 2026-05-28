"""
support_topics.py

I18N FIX (v2):
  - _SUPPORT_CONNECTED_MSG constant removed from user-facing sends.
    Replaced with get_text("support_connected", lang) after DB lang lookup.
  - /close topic command user notification localized.
  - /ban topic command user notification localized.
  - /warn topic command user notification localized.
  - cb_support_accept_button user notification localized.
  - msg_admin_topic_reply window notice localized.

Admin-group messages (topic thread, delivery confirmations, not-accepted
gate warnings) intentionally remain English — they are admin-operational.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.enums import ContentType
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import BotConfig
from database.repository import Database
from locales import get_text, t
from states import SupportFSM

log = logging.getLogger(__name__)
router = Router(name="support_topics")

_TOPIC_NAME_MAX = 128

_MEDIA_CONTENT_TYPES = frozenset({
    ContentType.TEXT,
    ContentType.PHOTO,
    ContentType.VIDEO,
    ContentType.DOCUMENT,
    ContentType.AUDIO,
    ContentType.VOICE,
    ContentType.STICKER,
    ContentType.ANIMATION,
})

# Admin-facing only — never sent directly to user chat_id
_ADMIN_ACCEPT_GATE_MSG = (
    "⏳ <b>Session not yet accepted.</b>\n\n"
    "Your message was <b>NOT</b> forwarded to the user.\n\n"
    "Click <b>✅ Accept Support</b> above or type "
    "<code>/accept</code> to begin the session."
)


def _support_session_minutes() -> int:
    raw = os.getenv("SUPPORT_SESSION_MINUTES", "")
    try:
        return max(1, int(raw)) if raw else 5
    except ValueError:
        return 5


async def _get_user_lang(db: Database, user_id: int) -> str:
    """Fetch user language from DB. Returns 'en' on any failure."""
    try:
        raw = await db.get_user_language(user_id)
        if raw in ("en", "bn"):
            return raw
    except Exception:
        pass
    return "en"


# ══════════════════════════════════════════════════════════════════════════════
#  ACCEPT MARKUP
# ══════════════════════════════════════════════════════════════════════════════

def build_accept_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✅ Accept Support",
            callback_data=f"support_accept:{user_id}",
        )
    ]])


# ══════════════════════════════════════════════════════════════════════════════
#  ACCEPT BUTTON CALLBACK HANDLER
# ══════════════════════════════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("support_accept:"))
async def cb_support_accept_button(
    callback: CallbackQuery,
    bot: Bot,
    db: Database,
    settings: BotConfig,
) -> None:
    if callback.from_user.id not in settings.admin_ids:
        await callback.answer("❌ Unauthorized.", show_alert=True)
        return

    raw = callback.data or ""
    parts = raw.split(":")

    if len(parts) != 2:
        await callback.answer("❌ Invalid callback data.", show_alert=True)
        return

    try:
        user_id = int(parts[1])
    except ValueError:
        await callback.answer("❌ Invalid user ID.", show_alert=True)
        return

    session = await db.get_support_session(user_id)
    if not session:
        await callback.answer("⚠️ No session found for this user.", show_alert=True)
        return

    if session.get("closed"):
        await callback.answer("⚠️ Session already closed.", show_alert=True)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if session.get("accepted_at"):
        await callback.answer("⚠️ Session already accepted.", show_alert=True)
        return

    claimed = await db.claim_admin_action(
        entity_type="support_session",
        entity_id=user_id,
        action="SUPPORT_ACCEPTED",
        admin_id=callback.from_user.id,
        admin_username=callback.from_user.username,
        target_user_id=user_id,
    )
    if not claimed:
        state_row = await db.get_admin_action_state("support_session", user_id)
        handler = "another admin"
        if state_row:
            u = state_row.get("handled_by_username")
            a = state_row.get("handled_by")
            handler = f"@{u}" if u else (f"Admin {a}" if a else handler)
        await callback.answer(f"⚠️ Already accepted by {handler}.", show_alert=True)
        return

    await db.accept_support_session(user_id)

    admin_name = escape(callback.from_user.full_name)
    log.info(
        "[SUPPORT ACCEPT] Admin %d (%s) accepted support for user %d via button",
        callback.from_user.id, admin_name, user_id,
    )

    try:
        original = callback.message.text or callback.message.caption or ""
        await callback.message.edit_text(
            original + f"\n\n✅ <b>Accepted by {admin_name}</b>",
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

    await callback.answer("✅ Support session accepted!", show_alert=False)

    fsm_storage = getattr(bot, "fsm_storage", None)
    if fsm_storage:
        key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
        await fsm_storage.set_state(key=key, state=SupportFSM.active)
        await fsm_storage.set_data(
            key=key,
            data={"admin_chat_id": settings.admin_group_id},
        )

    # I18N FIX: fetch user language before sending to user
    lang = await _get_user_lang(db, user_id)
    try:
        await bot.send_message(
            chat_id=user_id,
            text=get_text("support_connected", lang),
            parse_mode="HTML",
        )
    except Exception as exc:
        log.warning(
            "[SUPPORT ACCEPT] Could not notify user %d: %s", user_id, exc
        )


# ══════════════════════════════════════════════════════════════════════════════
#  TOPIC NAME BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _build_topic_name(user) -> str:
    parts = []
    if user.first_name:
        parts.append(user.first_name)
    if user.last_name:
        parts.append(user.last_name)
    name = " ".join(parts).strip() or f"User {user.id}"
    if user.username:
        suffix = f" (@{user.username})"
        name = name[:_TOPIC_NAME_MAX - len(suffix)] + suffix
    else:
        suffix = f" [#{user.id}]"
        name = name[:_TOPIC_NAME_MAX - len(suffix)] + suffix
    return name[:_TOPIC_NAME_MAX]


def _build_topic_name_from_db(
    user_id: int,
    full_name: Optional[str],
    username: Optional[str],
) -> str:
    name = (full_name or f"User {user_id}").strip()
    if username:
        suffix = f" (@{username})"
        name = name[:_TOPIC_NAME_MAX - len(suffix)] + suffix
    else:
        suffix = f" [#{user_id}]"
        name = name[:_TOPIC_NAME_MAX - len(suffix)] + suffix
    return name[:_TOPIC_NAME_MAX]


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC API: TOPIC MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

async def get_or_create_forum_topic(
    bot: Bot,
    db: Database,
    settings: BotConfig,
    user_id: int,
    tg_user=None,
) -> Optional[int]:
    if not settings.admin_group_id:
        return None

    user_row = await db.get_user(user_id)
    if user_row and user_row.get("forum_topic_id"):
        return int(user_row["forum_topic_id"])

    if tg_user is not None:
        topic_name = _build_topic_name(tg_user)
        full_name = tg_user.full_name
        username_str = tg_user.username
    elif user_row:
        full_name = user_row.get("full_name") or f"User {user_id}"
        username_str = user_row.get("username")
        topic_name = _build_topic_name_from_db(user_id, full_name, username_str)
    else:
        full_name = f"User {user_id}"
        username_str = None
        topic_name = f"👤 User {user_id}"

    try:
        topic = await bot.create_forum_topic(
            chat_id=settings.admin_group_id,
            name=topic_name,
        )
        topic_id: int = topic.message_thread_id
        await db.set_forum_topic_id(user_id, topic_id)
        log.info("[TOPICS] Created topic %d for user %d (%r)", topic_id, user_id, topic_name)
    except TelegramForbiddenError:
        log.error(
            "[TOPICS] Bot lacks 'Manage Topics' permission in group %d",
            settings.admin_group_id,
        )
        return None
    except TelegramBadRequest as exc:
        log.error("[TOPICS] BadRequest creating topic for user %d: %s", user_id, exc)
        return None
    except Exception as exc:
        log.error("[TOPICS] Unexpected error creating topic for user %d: %s", user_id, exc)
        return None

    user_link = f"tg://user?id={user_id}"
    info_card = (
        f"👤 <b>User Support Thread</b>\n\n"
        f"Name: <a href='{user_link}'>{escape(full_name)}</a>\n"
        f"Username: {'@' + username_str if username_str else '—'}\n"
        f"User ID: <code>{user_id}</code>\n\n"
        f"All messages from this user appear here.\n"
        f"<b>Accept the session below before replying.</b>\n\n"
        f"<i>Topic commands (no user_id needed):</i>\n"
        f"<code>/accept</code> — accept support session\n"
        f"<code>/close</code> — end support session\n"
        f"<code>/ban</code> — ban this user\n"
        f"<code>/warn</code> — issue a warning\n"
        f"<code>/mute</code> / <code>/unmute</code>\n"
        f"<code>/paymentdone</code> — clear payment intent\n"
        f"<code>/note &lt;text&gt;</code> — add a note"
    )
    try:
        pinned = await bot.send_message(
            chat_id=settings.admin_group_id,
            message_thread_id=topic_id,
            text=info_card,
            parse_mode="HTML",
        )
        await bot.pin_chat_message(
            chat_id=settings.admin_group_id,
            message_id=pinned.message_id,
            disable_notification=True,
        )
    except Exception as exc:
        log.warning("[TOPICS] Could not pin info card for user %d: %s", user_id, exc)

    return topic_id


async def notify_to_topic(
    bot: Bot,
    db: Database,
    settings: BotConfig,
    user_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    photo: Optional[str] = None,
    parse_mode: str = "HTML",
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    _retry: bool = True,
) -> Optional[Message]:
    if not settings.admin_group_id:
        return None

    topic_id = await get_or_create_forum_topic(bot, db, settings, user_id)
    if topic_id is None:
        return None

    try:
        if photo:
            sent = await bot.send_photo(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                photo=photo,
                caption=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            message_kind = "photo"
        else:
            sent = await bot.send_message(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            message_kind = "text"

        if db and entity_type and entity_id is not None:
            await db.register_admin_notification(
                entity_type=entity_type,
                entity_id=entity_id,
                chat_id=settings.admin_group_id,
                message_id=sent.message_id,
                message_kind=message_kind,
                message_text=text,
            )

        log.info(
            "[TOPICS] Notification → user %d topic %d (entity=%s:%s)",
            user_id, topic_id, entity_type, entity_id,
        )
        return sent

    except TelegramBadRequest as exc:
        if "thread not found" in str(exc).lower() and _retry:
            log.warning(
                "[TOPICS] Topic %d for user %d was deleted — clearing and retrying once",
                topic_id, user_id,
            )
            await db.set_forum_topic_id(user_id, None)
            return await notify_to_topic(
                bot, db, settings, user_id, text,
                reply_markup=reply_markup, photo=photo, parse_mode=parse_mode,
                entity_type=entity_type, entity_id=entity_id, _retry=False,
            )
        log.error("[TOPICS] BadRequest sending to topic %d: %s", topic_id, exc)
        return None
    except Exception as exc:
        log.error("[TOPICS] Failed to send to user %d topic %d: %s", user_id, topic_id, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  USER → TOPIC
# ══════════════════════════════════════════════════════════════════════════════

async def forward_to_topic(
    bot: Bot,
    db: Database,
    settings: BotConfig,
    message: Message,
    _retry: bool = True,
) -> bool:
    if not settings.admin_group_id:
        return False

    tg_user = message.from_user
    topic_id = await get_or_create_forum_topic(
        bot, db, settings, tg_user.id, tg_user=tg_user
    )
    if topic_id is None:
        return False

    user_label = f"<b>{escape(tg_user.full_name)}</b>"
    ct = message.content_type

    try:
        if ct == ContentType.TEXT:
            sent = await bot.send_message(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                text=f"💬 {user_label}:\n{escape(message.text or '')}",
                parse_mode="HTML",
            )
        elif ct == ContentType.PHOTO:
            caption_raw = message.caption or ""
            caption = f"📸 {user_label}:\n{escape(caption_raw)}" if caption_raw else f"📸 {user_label}"
            sent = await bot.send_photo(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                photo=message.photo[-1].file_id,
                caption=caption,
                parse_mode="HTML",
            )
        elif ct == ContentType.VIDEO:
            caption_raw = message.caption or ""
            caption = f"🎥 {user_label}:\n{escape(caption_raw)}" if caption_raw else f"🎥 {user_label}"
            sent = await bot.send_video(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                video=message.video.file_id,
                caption=caption,
                parse_mode="HTML",
            )
        elif ct == ContentType.DOCUMENT:
            caption_raw = message.caption or ""
            caption = f"📄 {user_label}:\n{escape(caption_raw)}" if caption_raw else f"📄 {user_label}"
            sent = await bot.send_document(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                document=message.document.file_id,
                caption=caption,
                parse_mode="HTML",
            )
        elif ct == ContentType.AUDIO:
            caption_raw = message.caption or ""
            caption = f"🎵 {user_label}:\n{escape(caption_raw)}" if caption_raw else f"🎵 {user_label}"
            sent = await bot.send_audio(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                audio=message.audio.file_id,
                caption=caption,
                parse_mode="HTML",
            )
        elif ct == ContentType.VOICE:
            sent = await bot.send_voice(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                voice=message.voice.file_id,
            )
            await bot.send_message(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                text=f"🎤 Voice message from {user_label}",
                parse_mode="HTML",
            )
        elif ct == ContentType.STICKER:
            sent = await bot.send_sticker(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                sticker=message.sticker.file_id,
            )
        elif ct == ContentType.ANIMATION:
            caption_raw = message.caption or ""
            caption = f"🎞 {user_label}:\n{escape(caption_raw)}" if caption_raw else f"🎞 {user_label}"
            sent = await bot.send_animation(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                animation=message.animation.file_id,
                caption=caption,
                parse_mode="HTML",
            )
        else:
            sent = await message.forward(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
            )

        await db.store_support_message(
            user_id=tg_user.id,
            admin_msg_id=sent.message_id,
            admin_chat_id=settings.admin_group_id,
        )
        log.info("[TOPICS] Forwarded user %d → topic %d (ct=%s)", tg_user.id, topic_id, ct)
        return True

    except TelegramBadRequest as exc:
        if "thread not found" in str(exc).lower() and _retry:
            log.warning(
                "[TOPICS] Topic %d deleted for user %d — clearing and retrying once",
                topic_id, tg_user.id,
            )
            await db.set_forum_topic_id(tg_user.id, None)
            return await forward_to_topic(bot, db, settings, message, _retry=False)
        log.error("[TOPICS] BadRequest forwarding to topic %d: %s", topic_id, exc)
        return False
    except Exception as exc:
        log.error("[TOPICS] Failed to forward to topic %d: %s", topic_id, exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  ADMIN TOPIC COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

@router.message(
    F.chat.type == "supergroup",
    F.message_thread_id.is_not(None),
    F.text.startswith("/"),
)
async def msg_admin_topic_command(
    message: Message,
    bot: Bot,
    db: Database,
    settings: BotConfig,
) -> None:
    """
    Handle contextual admin commands inside user forum topics.
    Admin-facing replies stay English. User-facing sends use user's stored lang.
    """
    if not settings.admin_group_id or message.chat.id != settings.admin_group_id:
        raise SkipHandler()

    if message.from_user.id not in settings.admin_ids:
        raise SkipHandler()

    text = (message.text or "").strip()
    parts = text.split()
    if not parts:
        raise SkipHandler()

    cmd = parts[0].lower().lstrip("/").split("@")[0]
    args = parts[1:]

    topic_id = message.message_thread_id
    user_row = await db.get_user_by_forum_topic_id(topic_id)
    if user_row is None:
        raise SkipHandler()

    user_id: int = user_row["user_id"]

    passthrough_with_args = {
        "approve", "reject", "ban", "unban", "mute", "unmute",
        "warn", "unwarn", "warnings", "userstatus", "broadcast",
        "pending", "services", "reloadconfig", "setdiscount",
        "cleardiscount", "discount", "offer", "closesupport",
    }
    if cmd in passthrough_with_args and args:
        raise SkipHandler()

    # Pre-fetch user language once for commands that send to user
    user_lang = await _get_user_lang(db, user_id)

    # ── /accept ────────────────────────────────────────────────────────────────
    if cmd == "accept":
        session_active = await db.is_support_session_active(user_id)
        if not session_active:
            await message.reply(
                f"⚠️ No active support session for user <code>{user_id}</code>.\n\n"
                "Ask the user to send <code>/support</code> first.",
                parse_mode="HTML",
            )
            return

        claimed = await db.claim_admin_action(
            entity_type="support_session",
            entity_id=user_id,
            action="SUPPORT_ACCEPTED",
            admin_id=message.from_user.id,
            admin_username=message.from_user.username,
            target_user_id=user_id,
        )
        if not claimed:
            state_row = await db.get_admin_action_state("support_session", user_id)
            handler = "another admin"
            if state_row:
                u = state_row.get("handled_by_username")
                a = state_row.get("handled_by")
                handler = f"@{u}" if u else (f"Admin {a}" if a else handler)
            await message.reply(
                f"⚠️ Support session for user <code>{user_id}</code> is already accepted by {handler}.",
                parse_mode="HTML",
            )
            return

        await db.accept_support_session(user_id)
        admin_name = escape(message.from_user.full_name)
        log.info(
            "[TOPICS CMD] Admin %d (%s) accepted support for user %d via /accept",
            message.from_user.id, message.from_user.full_name, user_id,
        )
        await message.reply(
            f"✅ Support session accepted for user <code>{user_id}</code>.\n"
            f"Accepted by: <b>{admin_name}</b>\n\n"
            "Admin replies in this topic will now be forwarded to the user.",
            parse_mode="HTML",
        )
        # I18N FIX: user notification in their language
        try:
            await bot.send_message(
                chat_id=user_id,
                text=get_text("support_connected", user_lang),
                parse_mode="HTML",
            )
        except Exception as exc:
            log.warning(
                "[TOPICS CMD] Could not notify user %d of accept: %s", user_id, exc
            )
        return

    # ── /close ─────────────────────────────────────────────────────────────────
    if cmd == "close":
        await db.close_support_session(user_id)
        fsm_storage = getattr(bot, "fsm_storage", None)
        if fsm_storage:
            key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
            await fsm_storage.set_state(key=key, state=None)
            await fsm_storage.set_data(key=key, data={})
        await message.reply(
            f"✅ Support session closed for user <code>{user_id}</code>.",
            parse_mode="HTML",
        )
        # I18N FIX: user notification in their language
        try:
            await bot.send_message(
                chat_id=user_id,
                text=get_text("support_session_closed_user", user_lang),
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    # ── /ban (no args) ─────────────────────────────────────────────────────────
    if cmd == "ban" and not args:
        from handlers.admin import kick_from_all_groups
        await db.ban_user(user_id)
        await kick_from_all_groups(bot, settings, db, user_id)
        await db.clear_payment_intent(user_id)
        log.warning(
            "[TOPICS CMD] Admin %d banned user %d from topic context",
            message.from_user.id, user_id,
        )
        await message.reply(
            f"🚫 User <code>{user_id}</code> banned and removed from all groups.",
            parse_mode="HTML",
        )
        # I18N FIX: user notification in their language
        try:
            await bot.send_message(
                chat_id=user_id,
                text=get_text("banned_message", user_lang),
            )
        except Exception:
            pass
        return

    # ── /warn (no args) ────────────────────────────────────────────────────────
    if cmd == "warn" and not args:
        from services.bot_upgrade import MAX_WARNINGS
        count = await db.add_warning(user_id)
        if count >= MAX_WARNINGS:
            await db.ban_user(user_id)
            await message.reply(
                f"🚫 User <code>{user_id}</code> auto-banned after {count}/{MAX_WARNINGS} warnings.",
                parse_mode="HTML",
            )
            # I18N FIX: ban notification in user's language
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=get_text("warn_banned", user_lang),
                    parse_mode="HTML",
                )
            except Exception:
                pass
        else:
            remaining = MAX_WARNINGS - count
            s = "s" if remaining != 1 else ""
            await message.reply(
                f"⚠️ Warning {count}/{MAX_WARNINGS} issued to user <code>{user_id}</code>. "
                f"{remaining} remaining before ban.",
                parse_mode="HTML",
            )
            # I18N FIX: warning notification in user's language
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=t("warn_received", user_lang,
                           count=count,
                           max=MAX_WARNINGS,
                           remaining=remaining,
                           s=s),
                    parse_mode="HTML",
                )
            except Exception:
                pass
        return

    # ── /mute (no args) ────────────────────────────────────────────────────────
    if cmd == "mute" and not args:
        mute_minutes = settings.mute_duration_seconds // 60
        mute_until = datetime.now(timezone.utc) + timedelta(minutes=mute_minutes)
        await db.mute_user(user_id, mute_until)
        await message.reply(
            f"🔇 User <code>{user_id}</code> muted for {mute_minutes} minutes.",
            parse_mode="HTML",
        )
        return

    # ── /unmute (no args) ──────────────────────────────────────────────────────
    if cmd == "unmute" and not args:
        await db.unmute_user(user_id)
        await message.reply(
            f"🔊 User <code>{user_id}</code> unmuted.",
            parse_mode="HTML",
        )
        return

    # ── /paymentdone ───────────────────────────────────────────────────────────
    if cmd == "paymentdone":
        await db.clear_payment_intent(user_id)
        await message.reply(
            f"✅ Payment intent cleared for user <code>{user_id}</code>.",
            parse_mode="HTML",
        )
        return

    # ── /note <text> ───────────────────────────────────────────────────────────
    if cmd == "note":
        note_text = " ".join(args) if args else "(no text)"
        await message.reply(
            f"📝 Note logged for user <code>{user_id}</code>:\n"
            f"<i>{escape(note_text)}</i>",
            parse_mode="HTML",
        )
        log.info(
            "[TOPICS CMD] Note by admin %d for user %d: %s",
            message.from_user.id, user_id, note_text,
        )
        return

    raise SkipHandler()


# ══════════════════════════════════════════════════════════════════════════════
#  ADMIN → USER REPLY HANDLER
# ══════════════════════════════════════════════════════════════════════════════

@router.message(
    F.chat.type == "supergroup",
    F.message_thread_id.is_not(None),
    F.content_type.in_(_MEDIA_CONTENT_TYPES),
)
async def msg_admin_topic_reply(
    message: Message,
    bot: Bot,
    db: Database,
    settings: BotConfig,
    state: FSMContext,
) -> None:
    """
    Admin types anything in a user's forum topic → delivered to user privately.

    ACCEPT GATE: unchanged — still checks is_support_session_accepted().
    I18N FIX: window notice sent to user now uses user's stored language.
    Admin-group confirmations ("✅ Delivered", "⚠️ not accepted") remain English.
    """
    if not settings.admin_group_id or message.chat.id != settings.admin_group_id:
        return

    if message.from_user.id not in settings.admin_ids:
        return

    if message.text and message.text.startswith("/"):
        return

    current_state = await state.get_state()
    if current_state is not None:
        raise SkipHandler()

    topic_id = message.message_thread_id
    user_row = await db.get_user_by_forum_topic_id(topic_id)
    if user_row is None:
        return

    user_id: int = user_row["user_id"]

    if await db.is_banned(user_id):
        try:
            await bot.send_message(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                text=f"⚠️ User <code>{user_id}</code> is banned — message not delivered.",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    # ── ACCEPT GATE ────────────────────────────────────────────────────────────
    session_accepted = await db.is_support_session_accepted(user_id)
    if not session_accepted:
        session_active = await db.is_support_session_active(user_id)
        if session_active:
            try:
                await bot.send_message(
                    chat_id=settings.admin_group_id,
                    message_thread_id=topic_id,
                    text=_ADMIN_ACCEPT_GATE_MSG,  # admin-facing, stays English
                    parse_mode="HTML",
                )
            except Exception:
                pass
        return

    # ── Deliver to user ────────────────────────────────────────────────────────
    reply_header = get_text("support_reply_header", await _get_user_lang(db, user_id))
    delivered = False
    ct = message.content_type

    try:
        if ct == ContentType.TEXT:
            await bot.send_message(
                chat_id=user_id,
                text=reply_header + escape(message.text or ""),
                parse_mode="HTML",
            )
            delivered = True
        elif ct == ContentType.PHOTO:
            caption = reply_header + (escape(message.caption) if message.caption else "")
            await bot.send_photo(
                chat_id=user_id,
                photo=message.photo[-1].file_id,
                caption=caption or None,
                parse_mode="HTML",
            )
            delivered = True
        elif ct == ContentType.VIDEO:
            caption = reply_header + (escape(message.caption) if message.caption else "")
            await bot.send_video(
                chat_id=user_id,
                video=message.video.file_id,
                caption=caption or None,
                parse_mode="HTML",
            )
            delivered = True
        elif ct == ContentType.DOCUMENT:
            caption = reply_header + (escape(message.caption) if message.caption else "")
            await bot.send_document(
                chat_id=user_id,
                document=message.document.file_id,
                caption=caption or None,
                parse_mode="HTML",
            )
            delivered = True
        elif ct == ContentType.AUDIO:
            caption = reply_header + (escape(message.caption) if message.caption else "")
            await bot.send_audio(
                chat_id=user_id,
                audio=message.audio.file_id,
                caption=caption or None,
                parse_mode="HTML",
            )
            delivered = True
        elif ct == ContentType.VOICE:
            await bot.send_voice(chat_id=user_id, voice=message.voice.file_id)
            delivered = True
        elif ct == ContentType.STICKER:
            await bot.send_sticker(chat_id=user_id, sticker=message.sticker.file_id)
            delivered = True
        elif ct == ContentType.ANIMATION:
            caption = reply_header + (escape(message.caption) if message.caption else "")
            await bot.send_animation(
                chat_id=user_id,
                animation=message.animation.file_id,
                caption=caption or None,
                parse_mode="HTML",
            )
            delivered = True

    except TelegramForbiddenError:
        log.warning("[TOPICS] User %d has blocked the bot.", user_id)
        try:
            await bot.send_message(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                text=f"⚠️ User <code>{user_id}</code> has blocked the bot — cannot deliver.",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return
    except Exception as exc:
        log.error("[TOPICS] Failed to deliver to user %d: %s", user_id, exc)
        try:
            await bot.send_message(
                chat_id=settings.admin_group_id,
                message_thread_id=topic_id,
                text=f"❌ Delivery failed: {escape(str(exc))}",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    if not delivered:
        return

    # Use refresh_support_session (NOT open_support_session) to preserve accepted_at
    was_active = await db.is_support_session_active(user_id)
    minutes = _support_session_minutes()
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()
    await db.refresh_support_session(user_id, expires_at)

    fsm_storage = getattr(bot, "fsm_storage", None)
    if fsm_storage:
        key = StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id)
        await fsm_storage.set_state(key=key, state=SupportFSM.active)
        await fsm_storage.set_data(
            key=key, data={"admin_chat_id": settings.admin_group_id}
        )

    if not was_active:
        # I18N FIX: window notice in user's language
        user_lang = await _get_user_lang(db, user_id)
        try:
            await bot.send_message(
                chat_id=user_id,
                text=get_text("support_connected", user_lang),
                parse_mode="HTML",
            )
        except Exception as exc:
            log.warning(
                "[TOPICS] Could not send window notice to user %d: %s", user_id, exc
            )

    try:
        await bot.send_message(
            chat_id=settings.admin_group_id,
            message_thread_id=topic_id,
            text="✅ <i>Delivered</i>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    log.info(
        "[TOPICS] Admin %d → user %d via topic %d (ct=%s, delivered)",
        message.from_user.id, user_id, topic_id, ct,
    )