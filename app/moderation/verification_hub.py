from __future__ import annotations

import asyncio
from typing import Optional

from pyrogram.client import Client
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3


async def forward_to_verification(
    client: Client,
    messages: list[Message],
    submitter_user_id: int,
) -> bool:
    """
    Forwards all messages in the submission (single or album) to the
    verification group, then sends a moderation card with Approve/Reject
    inline buttons threaded under the last forwarded message.

    Returns True if all operations succeeded, False on any terminal failure.
    FloodWait errors are handled internally with asyncio.sleep retries.
    """
    if not messages:
        logger.warning(
            "forward_to_verification called with empty message list",
            extra={"ctx_user_id": submitter_user_id},
        )
        return False

    group_id = settings.VERIFICATION_GROUP_ID
    source_chat_id = messages[0].chat.id
    first_msg_id = messages[0].id

    forwarded_ids_in_group: list[int] = []

    for msg in messages:
        fwd_msg = await _forward_single(client, msg, group_id, submitter_user_id)
        if fwd_msg is None:
            # Terminal failure — a message could not be forwarded
            return False
        forwarded_ids_in_group.append(fwd_msg.id)

    last_fwd_id = forwarded_ids_in_group[-1]
    count = len(messages)
    media_label = "album" if count > 1 else "item"
    caption_raw = (messages[0].caption or messages[0].text or "").strip()
    preview = f'\n<i>"{caption_raw[:120]}"</i>' if caption_raw else ""

    info_text = (
        f"📬 <b>New submission</b>\n"
        f"👤 Submitter: <code>{submitter_user_id}</code>\n"
        f"📦 Content: {count} {media_label}{preview}"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Approve",
                    callback_data=f"mod_approve:{submitter_user_id}:{first_msg_id}",
                ),
                InlineKeyboardButton(
                    "❌ Reject",
                    callback_data=f"mod_reject:{submitter_user_id}:{first_msg_id}",
                ),
            ]
        ]
    )

    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=group_id,
                text=info_text,
                reply_to_message_id=last_fwd_id,
                reply_markup=keyboard,
                parse_mode="html",
            )
            logger.info(
                "Verification card sent",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_first_msg_id": first_msg_id,
                    "ctx_count": count,
                },
            )
            return True

        except FloodWait as e:
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            logger.warning(
                "FloodWait sending verification card, sleeping",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)

        except RPCError as e:
            logger.error(
                "RPC error sending verification card",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

    return False


async def _forward_single(
    client: Client,
    msg: Message,
    group_id: int,
    submitter_user_id: int,
) -> Optional[Message]:
    """
    Forwards a single message to the verification group.
    Returns the forwarded Message or None on terminal failure.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            result = await client.forward_messages(
                chat_id=group_id,
                from_chat_id=msg.chat.id,
                message_ids=msg.id,
            )
            # forward_messages returns Message for single id, List[Message] for iterable
            return result[0] if isinstance(result, list) else result

        except FloodWait as e:
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            logger.warning(
                "FloodWait during message forward, sleeping",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_msg_id": msg.id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)

        except RPCError as e:
            logger.error(
                "RPC error forwarding message to verification group",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_msg_id": msg.id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)

    return None


def parse_callback_data(data: str) -> Optional[tuple[str, int, int]]:
    """
    Parses mod callback data of the form:
        mod_approve:{user_id}:{msg_id}
        mod_reject:{user_id}:{msg_id}

    Returns (action, user_id, msg_id) where action is "approve" or "reject".
    Returns None if the data is missing, malformed, or contains an unknown action.
    """
    if not data:
        return None
    try:
        parts = data.split(":", 2)
        if len(parts) != 3:
            return None
        action_raw, user_id_str, msg_id_str = parts
        # Strip the "mod_" prefix to get the bare action name
        action = action_raw.removeprefix("mod_")
        if action not in ("approve", "reject"):
            return None
        return action, int(user_id_str), int(msg_id_str)
    except (ValueError, AttributeError):
        return None
