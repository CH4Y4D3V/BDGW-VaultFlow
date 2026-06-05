from __future__ import annotations

# ── verification_hub.py ────────────────────────────────────────────────────────
# Responsible for:
#   • Posting a user-info card to the hub topic before any content card.
#   • Forwarding submitted content (single item or album) to the correct
#     user topic in the Verification Hub.
#   • Attaching a moderation action card (Approve NSFW / Approve Premium /
#     Reject) to the forwarded content.
#   • Parsing callback_data from moderation button presses.
#
# Spec references: Section 9 (Verification Hub), Section 10.3 (Submission Flow)
#
# MANDATORY CONTRACT for callers
# ──────────────────────────────
# • topic_id passed to forward_to_verification() and post_user_info_card()
#   MUST originate from user_topics_repo.get_or_create() (Section 9.2).
#   This module does NOT call that repo itself so it stays free of DB
#   dependencies — the responsibility belongs to the submission handler.
# ──────────────────────────────────────────────────────────────────────────────

import asyncio
from typing import Optional

from pyrogram.client import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, User

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3


# ── User-info card ─────────────────────────────────────────────────────────────

async def post_user_info_card(
    client: Client,
    user: User,
    chat_id: int,
    topic_id: Optional[int] = None,
) -> bool:
    """Send a formatted HTML user-info card to *chat_id* / *topic_id*.

    This is always the FIRST message posted for a submission so that admins
    can immediately identify the submitter.  Per spec 10.3, anonymous
    moderation no longer exists — the full name, user ID, and username are
    always displayed.

    Args:
        client:   Active Pyrogram client.
        user:     Pyrogram User object of the submitter.
        chat_id:  Target chat (Verification Hub group ID).
        topic_id: Forum topic ID inside the hub.  Must come from
                  ``user_topics_repo.get_or_create()``.

    Returns:
        True on success, False after all retries are exhausted.
    """
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "—"
    text = (
        f"👤 <b>User Info</b>\n\n"
        f"ID:   <code>{user.id}</code>\n"
        f"Name: {full_name}\n"
    )
    if user.username:
        text += f"Username: @{user.username}"

    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=chat_id,
                text=text,
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML,
            )
            return True
        except FloodWait as e:
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            logger.warning(
                "FloodWait posting user info card",
                extra={
                    "ctx_user_id": user.id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "RPCError posting user info card",
                extra={
                    "ctx_user_id": user.id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                logger.error(
                    "Failed to post user info card after all retries",
                    extra={"ctx_user_id": user.id, "ctx_error": str(e)},
                )
                return False
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(
                "Unexpected error posting user info card",
                extra={"ctx_user_id": user.id, "ctx_error": str(e)},
            )
            return False

    return False


# ── Submission forwarding ──────────────────────────────────────────────────────

async def forward_to_verification(
    client: Client,
    messages: list[Message],
    submitter_user_id: int,
    topic_id: Optional[int] = None,
) -> bool:
    """Forward a content submission to the Verification Hub and attach a
    moderation action card.

    Flow (per spec 10.3):
      1. Validate input.
      2. Copy/forward all submitted messages to the hub topic.
         Albums are copied atomically via ``copy_media_group``; single items
         fall back to ``copy_message``.
      3. Post a moderation card (reply to the last forwarded message) with
         three inline buttons:
           • 🔞 Approve NSFW
           • ⭐ Approve Premium
           • ❌ Reject
      4. The submitter's full identity (name, user ID, username) is ALWAYS
         shown.  Anonymous moderation was removed in spec v1.0 FINAL.

    Args:
        client:             Active Pyrogram client.
        messages:           Ordered list of submitted messages (1 or many for
                            an album).  Must not be empty.
        submitter_user_id:  Telegram user ID of the person who submitted.
        topic_id:           Forum topic ID in the Verification Hub.  MUST
                            originate from ``user_topics_repo.get_or_create()``.

    Returns:
        True if the moderation card was delivered successfully, False otherwise.
    """
    if not messages:
        logger.warning(
            "forward_to_verification called with empty message list",
            extra={"ctx_user_id": submitter_user_id},
        )
        return False

    group_id = settings.VERIFICATION_GROUP_ID
    first_msg_id = messages[0].id

    logger.info(
        "submission_forward_started",
        extra={
            "ctx_user_id": submitter_user_id,
            "ctx_count": len(messages),
            "ctx_group_id": group_id,
            "ctx_topic_id": topic_id,
        },
    )

    forwarded_ids: list[int] = []

    # ── RC-12: Atomic Album Forwarding ──────────────────────────────────────
    # Preserve album integrity by using copy_media_group.
    # Fall back to sequential copy_message only if copy_media_group fails.

    is_album = len(messages) > 1 and all(m.media_group_id for m in messages)

    if is_album:
        try:
            for attempt in range(_MAX_RETRIES):
                try:
                    fwd_messages = await client.copy_media_group(
                        chat_id=group_id,
                        from_chat_id=messages[0].chat.id,
                        message_id=messages[0].id,
                        message_thread_id=topic_id,
                    )
                    forwarded_ids = [m.id for m in fwd_messages]
                    break
                except FloodWait as e:
                    wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
                    logger.warning(
                        "FloodWait during copy_media_group in verification",
                        extra={
                            "ctx_user_id": submitter_user_id,
                            "ctx_wait": wait,
                            "ctx_attempt": attempt + 1,
                        },
                    )
                    await asyncio.sleep(wait)
                except RPCError as e:
                    logger.warning(
                        "RPCError during copy_media_group in verification",
                        extra={
                            "ctx_user_id": submitter_user_id,
                            "ctx_error": str(e),
                            "ctx_attempt": attempt + 1,
                        },
                    )
                    if attempt == _MAX_RETRIES - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.warning(
                "copy_media_group_failed_in_verification_fallback_to_sequential",
                extra={"ctx_error": str(e), "ctx_user_id": submitter_user_id},
            )
            forwarded_ids = []  # Force sequential fallback

    if not forwarded_ids:
        for msg in messages:
            fwd = await _forward_single(client, msg, group_id, submitter_user_id, topic_id)
            if fwd is None:
                logger.error(
                    "submission_forward_failed",
                    extra={
                        "ctx_user_id": submitter_user_id,
                        "ctx_msg_id": msg.id,
                        "ctx_group_id": group_id,
                    },
                )
                return False
            forwarded_ids.append(fwd.id)

    logger.info(
        "submission_forward_completed",
        extra={
            "ctx_user_id": submitter_user_id,
            "ctx_count": len(messages),
            "ctx_forwarded_ids": forwarded_ids,
        },
    )

    last_fwd_id = forwarded_ids[-1]
    count = len(messages)
    media_label = "album" if count > 1 else "item"

    # ── Identity display ──────────────────────────────────────────────────
    # Spec 10.3 (v1.0 FINAL): "Anonymous moderation no longer exists.
    # Admin always sees: Full Name, Username, User ID."
    # The Redis user:anon:{user_id} key and anonymous toggle were removed
    # from the spec — do NOT check any anonymous flag here.
    user_label = f"User {submitter_user_id}"

    info_text = (
        f"📬 <b>New Submission</b>\n\n"
        f"👤 Submitter: <code>{user_label}</code> (<code>{submitter_user_id}</code>)\n"
        f"📦 Content: {count} {media_label}"
    )

    # ── Moderation buttons — labels match spec 10.3 exactly ─────────────
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"🔞 Approve {settings.NSFW_DISPLAY_NAME}",
                    callback_data=f"mod_app_nsfw:{submitter_user_id}:{first_msg_id}",
                ),
                InlineKeyboardButton(
                    f"⭐ Approve {settings.PREMIUM_DISPLAY_NAME}",
                    callback_data=f"mod_app_prem:{submitter_user_id}:{first_msg_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    "❌ Reject",
                    callback_data=f"mod_reject:{submitter_user_id}:{first_msg_id}",
                ),
            ],
        ]
    )

    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=group_id,
                text=info_text,
                reply_to_message_id=last_fwd_id,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                message_thread_id=topic_id,
            )
            logger.info(
                "verification_card_created",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_first_msg_id": first_msg_id,
                    "ctx_count": count,
                    "ctx_topic_id": topic_id,
                },
            )
            return True

        except FloodWait as e:
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            logger.warning(
                "FloodWait sending moderation card",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)

        except RPCError as e:
            logger.warning(
                "RPCError sending moderation card",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                logger.error(
                    "Failed to send moderation card after all retries",
                    extra={"ctx_user_id": submitter_user_id, "ctx_error": str(e)},
                )
                return False
            await asyncio.sleep(2 ** attempt)

        except Exception as e:
            logger.error(
                "Unexpected error sending moderation card",
                extra={"ctx_user_id": submitter_user_id, "ctx_error": str(e)},
            )
            return False

    return False


# ── Internal helpers ───────────────────────────────────────────────────────────

async def _forward_single(
    client: Client,
    msg: Message,
    group_id: int,
    submitter_user_id: int,
    topic_id: Optional[int],
) -> Optional[Message]:
    """Copy a single message to *group_id* / *topic_id* with FloodWait handling.

    Uses ``copy_message`` (not ``forward_messages``) to produce a clean copy
    with no metadata leak.

    Args:
        client:             Active Pyrogram client.
        msg:                The message to copy.
        group_id:           Destination chat ID (Verification Hub).
        submitter_user_id:  Used only for structured logging.
        topic_id:           Target forum topic inside the hub.

    Returns:
        The copied ``Message`` on success, or ``None`` after all retries fail.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            result = await client.copy_message(
                chat_id=group_id,
                from_chat_id=msg.chat.id,
                message_id=msg.id,
                message_thread_id=topic_id,
            )
            return result
        except FloodWait as e:
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            logger.warning(
                "FloodWait during _forward_single",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_msg_id": msg.id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "RPCError during _forward_single",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_msg_id": msg.id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                logger.error(
                    "_forward_single exhausted all retries",
                    extra={
                        "ctx_user_id": submitter_user_id,
                        "ctx_msg_id": msg.id,
                        "ctx_error": str(e),
                    },
                )
                return None
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(
                "Unexpected error in _forward_single",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_msg_id": msg.id,
                    "ctx_error": str(e),
                },
            )
            return None

    return None


# ── Callback data parser ───────────────────────────────────────────────────────

def parse_callback_data(data: str) -> Optional[dict]:
    """Parse a moderation button callback_data string into its components.

    Expected format::

        mod_{action}:{submitter_id}:{msg_id}

    Examples::

        "mod_app_nsfw:123456789:99001"  →  {"action": "app_nsfw", ...}
        "mod_app_prem:123456789:99001"  →  {"action": "app_prem", ...}
        "mod_reject:123456789:99001"    →  {"action": "reject",   ...}

    Args:
        data: Raw ``callback_data`` string from the Pyrogram callback query.

    Returns:
        A dict with keys ``action`` (str), ``submitter_id`` (int), and
        ``msg_id`` (int), or ``None`` if the data is malformed.
    """
    if not data:
        return None

    try:
        # Split on the first two ":" delimiters; action_raw may itself contain
        # underscores (e.g. "mod_app_nsfw") so we limit the split to 2 parts.
        parts = data.split(":", 2)
        if len(parts) != 3:
            return None

        action_raw, submitter_id_str, msg_id_str = parts
        action = action_raw.removeprefix("mod_")

        return {
            "action": action,
            "submitter_id": int(submitter_id_str),
            "msg_id": int(msg_id_str),
        }

    except (ValueError, AttributeError, IndexError) as e:
        logger.warning(
            "parse_callback_data: malformed callback_data",
            extra={"ctx_data": data, "ctx_error": str(e)},
        )
        return None
