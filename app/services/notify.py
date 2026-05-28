"""
notify.py
─────────
Centralised notification helper.

CHANGES IN THIS VERSION:
  - notify_admins() and notify_admins_photo() accept optional user_id.
  - When user_id + db + admin_group_id are all set, the notification is
    routed to the user's dedicated forum topic instead of flat group messages.
  - Falls back to admin DMs ONLY if topic routing fails (REQ-2 fix).
  - admin_notifications are registered correctly in both paths so that
    sync_admin_entity_messages() can edit topic messages as before.

BUG FIX (REQ-2 — GENERAL monitoring only):
  - Previous fallback used _build_targets() which includes admin_group_id.
    This caused transactional payment messages (with approve/reject buttons)
    to land in GENERAL as unthreaded messages when a topic failed to route.
    GENERAL must remain monitoring-only (client requirement #2).
  - Fallback now uses _build_dm_targets() — admin DMs only, no group_id.
  - Pure broadcast paths (no user_id) are unchanged.
"""

import asyncio
import logging
from typing import Optional

from aiogram import Bot
from aiogram.types import Message, InlineKeyboardMarkup

from config import BotConfig
from database.repository import Database

log = logging.getLogger(__name__)


def _build_targets(settings: BotConfig) -> list[int]:
    """
    Deduplicated list of admin_ids + admin_group_id.
    Used ONLY for pure broadcast paths where no user_id is involved.
    Do NOT use for user-specific fallback — see _build_dm_targets().
    """
    targets: list[int] = list(settings.admin_ids)
    if (
        settings.admin_group_id is not None
        and settings.admin_group_id not in targets
    ):
        targets.append(settings.admin_group_id)
    return targets


def _build_dm_targets(settings: BotConfig) -> list[int]:
    """
    Admin personal DM targets only — deliberately excludes admin_group_id.

    Used in the fallback path when user-specific topic routing fails.
    Sending admin_group_id without message_thread_id posts to GENERAL
    unthreaded, polluting it with transactional messages (REQ-2 violation).
    DM fallback keeps admins informed without touching GENERAL.
    """
    return list(settings.admin_ids)


async def notify_admins(
    bot: Bot,
    settings: BotConfig,
    text: str,
    db: Optional[Database] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: str = "HTML",
    user_id: Optional[int] = None,
) -> list[Message]:
    """
    Send a text notification to admins.

    With user_id: routes to the user's dedicated forum topic (primary path).
      Fallback on topic failure: admin personal DMs only — NOT admin_group_id,
      so GENERAL is never polluted with transactional messages (REQ-2).

    Without user_id: broadcasts to all targets (admin DMs + admin_group_id).
    """
    # ── Topic routing: user-specific notification ──────────────────────────────
    if user_id is not None and db is not None and settings.admin_group_id:
        from services.support_topics import notify_to_topic

        sent = await notify_to_topic(
            bot, db, settings, user_id, text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            entity_type=entity_type,
            entity_id=entity_id,
        )
        if sent is not None:
            return [sent]

        # ── REQ-2 FIX: topic failed → DM admins only, never GENERAL ──────────
        log.warning(
            "notify_admins: topic routing failed for user %d — "
            "falling back to admin DMs only (GENERAL excluded per REQ-2).",
            user_id,
        )
        targets = _build_dm_targets(settings)
        sent_messages: list[Message] = []

        if not targets:
            log.error(
                "notify_admins: topic failed AND no admin DM targets. "
                "Message for user %d (entity=%s:%s) lost.",
                user_id, entity_type, entity_id,
            )
            return sent_messages

        for chat_id in targets:
            try:
                dm_text = (
                    f"⚠️ <b>[Topic Routing Failed — DM Fallback]</b>\n"
                    f"User: <code>{user_id}</code>\n\n"
                    + text
                )
                sent = await bot.send_message(
                    chat_id=chat_id,
                    text=dm_text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                )
                sent_messages.append(sent)
                if db and entity_type and entity_id is not None:
                    await db.register_admin_notification(
                        entity_type=entity_type,
                        entity_id=entity_id,
                        chat_id=chat_id,
                        message_id=sent.message_id,
                        message_kind="text",
                        message_text=dm_text,
                    )
                await asyncio.sleep(0.035)
            except Exception as exc:
                log.error(
                    "notify_admins: DM fallback to admin %d failed: %s", chat_id, exc
                )

        return sent_messages

    # ── No user_id: pure broadcast (admin DMs + admin group) ──────────────────
    targets = _build_targets(settings)
    sent_messages: list[Message] = []

    if not targets:
        log.warning("notify_admins: no targets configured — message not delivered.")
        return sent_messages

    for chat_id in targets:
        try:
            sent = await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            sent_messages.append(sent)
            if db and entity_type and entity_id is not None:
                await db.register_admin_notification(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    chat_id=chat_id,
                    message_id=sent.message_id,
                    message_kind="text",
                    message_text=text,
                )
            await asyncio.sleep(0.035)
        except Exception as exc:
            log.error("notify_admins: failed to send to chat %d: %s", chat_id, exc)

    return sent_messages


async def notify_admins_photo(
    bot: Bot,
    settings: BotConfig,
    photo: str,
    caption: str,
    db: Optional[Database] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: str = "HTML",
    user_id: Optional[int] = None,
) -> list[Message]:
    """
    Forward a photo notification to admins.

    With user_id: routes to the user's dedicated forum topic.
      Fallback on topic failure: admin DMs only — NOT admin_group_id (REQ-2).

    Without user_id: broadcasts to all targets (admin DMs + admin group).
    """
    # ── Topic routing ──────────────────────────────────────────────────────────
    if user_id is not None and db is not None and settings.admin_group_id:
        from services.support_topics import notify_to_topic

        sent = await notify_to_topic(
            bot, db, settings, user_id, caption,
            reply_markup=reply_markup,
            photo=photo,
            parse_mode=parse_mode,
            entity_type=entity_type,
            entity_id=entity_id,
        )
        if sent is not None:
            return [sent]

        # ── REQ-2 FIX: topic failed → DM admins only, never GENERAL ──────────
        log.warning(
            "notify_admins_photo: topic routing failed for user %d — "
            "falling back to admin DMs only (GENERAL excluded per REQ-2).",
            user_id,
        )
        targets = _build_dm_targets(settings)
        sent_messages: list[Message] = []

        if not targets:
            log.error(
                "notify_admins_photo: topic failed AND no admin DM targets. "
                "Photo for user %d (entity=%s:%s) lost.",
                user_id, entity_type, entity_id,
            )
            return sent_messages

        for chat_id in targets:
            try:
                fallback_caption = (
                    f"⚠️ <b>[Topic Routing Failed — DM Fallback]</b>\n"
                    f"User: <code>{user_id}</code>\n\n"
                    + caption
                )
                sent = await bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=fallback_caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                )
                sent_messages.append(sent)
                if db and entity_type and entity_id is not None:
                    await db.register_admin_notification(
                        entity_type=entity_type,
                        entity_id=entity_id,
                        chat_id=chat_id,
                        message_id=sent.message_id,
                        message_kind="photo",
                        message_text=fallback_caption,
                    )
                await asyncio.sleep(0.035)
            except Exception as exc:
                log.error(
                    "notify_admins_photo: DM fallback to admin %d failed: %s",
                    chat_id, exc,
                )

        return sent_messages

    # ── No user_id: pure broadcast ─────────────────────────────────────────────
    targets = _build_targets(settings)
    sent_messages: list[Message] = []

    if not targets:
        log.warning("notify_admins_photo: no targets configured — photo not delivered.")
        return sent_messages

    for chat_id in targets:
        try:
            sent = await bot.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=caption,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            sent_messages.append(sent)
            if db and entity_type and entity_id is not None:
                await db.register_admin_notification(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    chat_id=chat_id,
                    message_id=sent.message_id,
                    message_kind="photo",
                    message_text=caption,
                )
            log.info(
                "notify_admins_photo: photo sent to chat %d, msg_id=%d",
                chat_id, sent.message_id,
            )
            await asyncio.sleep(0.035)
        except Exception as exc:
            log.error("notify_admins_photo: failed to send to chat %d: %s", chat_id, exc)

    return sent_messages