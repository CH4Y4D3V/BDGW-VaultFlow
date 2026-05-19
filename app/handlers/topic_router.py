from __future__ import annotations

"""
Bidirectional message router between users (private DM) and
admin team (Verification Hub forum topics).

Flow:
  User → bot DM  ──copy──→  user's topic in hub  (handled in submission_handler / support flow)
  Admin reply in topic ──copy──→  user's private DM  (handled HERE)

Guard logic (critical — prevents routing loops and noise):
  1. Message must be in VERIFICATION_GROUP_ID
  2. Message must be inside a forum topic (has message_thread_id)
  3. Sender must NOT be a bot (prevents routing the bot's own notifications back)
  4. The topic must exist in user_topics collection
  5. Skip messages that are the bot's own moderation cards (has reply_markup with mod_ callbacks)

Handler group: 1  (runs AFTER default group 0, which includes support_handler's
persist handler — this ordering ensures the DB record is written before routing
fires, though each is independent and the order does not affect correctness).
"""

import asyncio

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    FloodWait,
    RPCError,
    UserIsBlocked,
    PeerIdInvalid,
    InputUserDeactivated,
)
from pyrogram.types import Message

from app.config import settings
from app.services.topic_service import get_topic_service
from app.utils.logger import get_logger

logger = get_logger(__name__)

_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3


def _get_thread_id(message: Message) -> int | None:
    """
    Extract the forum topic thread ID from a message.
    Pyrogram 2.x exposes this as reply_to_top_message_id or message_thread_id.
    Falls back safely.
    """
    return (
        getattr(message, "message_thread_id", None)
        or getattr(message, "reply_to_top_message_id", None)
    )


def _is_moderation_card(message: Message) -> bool:
    """
    Detect if this message is a bot-generated moderation card.
    We never want to re-route those back to users.

    FIX: previously the try/except silently swallowed AttributeError when
    reply_markup was not an InlineKeyboardMarkup (e.g. ReplyKeyboardMarkup).
    Now we guard the inline_keyboard attribute explicitly before iterating.
    """
    if not message.reply_markup:
        return False
    # Only InlineKeyboardMarkup has inline_keyboard rows
    inline_keyboard = getattr(message.reply_markup, "inline_keyboard", None)
    if not inline_keyboard:
        return False
    try:
        for row in inline_keyboard:
            for btn in row:
                data = getattr(btn, "callback_data", "") or ""
                if data.startswith("mod_"):
                    return True
    except Exception:
        pass
    return False


async def _deliver_to_user(client: Client, user_id: int, message: Message) -> bool:
    """
    Copy the admin's reply to the user's private chat.
    Returns True on success, False if user is unreachable.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await client.copy_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.id,
            )
            return True

        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            logger.warning(
                "User unreachable for admin reply delivery",
                extra={"ctx_user_id": user_id},
            )
            return False

        except FloodWait as e:
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)

        except RPCError as e:
            logger.warning(
                "RPC error delivering admin reply",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

    return False


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID), group=1)
async def route_admin_reply_to_user(client: Client, message: Message) -> None:
    """
    Main routing handler.
    Fires on every message in the verification hub, then gates out
    everything that shouldn't be routed.

    Uses group=1 so it runs after the default group (0) handlers, e.g. the
    support_handler persistence handler. This avoids any ordering confusion
    but does NOT affect correctness since both handlers are independent.
    """
    # ── Gate 1: must be inside a topic ───────────────────────────────────────
    thread_id = _get_thread_id(message)
    if not thread_id:
        return

    # ── Gate 2: must be from a human (not the bot itself) ────────────────────
    if not message.from_user:
        return  # Channel post / anonymous admin — skip
    if message.from_user.is_bot:
        return

    # ── Gate 3: never re-route moderation cards ──────────────────────────────
    if _is_moderation_card(message):
        return

    # ── Gate 4: look up user for this topic ──────────────────────────────────
    topic_service = get_topic_service()
    topic_doc = await topic_service.get_user_by_topic(thread_id)
    if not topic_doc:
        return  # This topic isn't tracked — ignore

    user_id: int = topic_doc["user_id"]
    topic_type: str = topic_doc.get("topic_type", "support")

    # ── Deliver ───────────────────────────────────────────────────────────────
    delivered = await _deliver_to_user(client, user_id, message)

    if delivered:
        logger.info(
            "Admin reply routed to user",
            extra={
                "ctx_admin_id": message.from_user.id,
                "ctx_user_id": user_id,
                "ctx_topic_id": thread_id,
                "ctx_topic_type": topic_type,
                "ctx_msg_id": message.id,
            },
        )
    else:
        # Notify admin inline that the user is unreachable
        try:
            await client.send_message(
                chat_id=message.chat.id,
                text=(
                    f"⚠️ Could not deliver reply to user <code>{user_id}</code>.\n"
                    "They may have blocked the bot."
                ),
                parse_mode=ParseMode.HTML,
                reply_to_message_id=message.id,
            )
        except Exception:
            pass
