from __future__ import annotations

import asyncio
from typing import Optional

from pyrogram.client import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3

# ── Callback data format ──────────────────────────────────────────────────────
#
# Step 1 — moderator picks action:
#   mod_app_nsfw:{submitter_id}:{msg_id}
#   mod_app_prem:{submitter_id}:{msg_id}
#   mod_reject:{submitter_id}:{msg_id}
# ─────────────────────────────────────────────────────────────────────────────


async def forward_to_verification(
    client: Client,
    messages: list[Message],
    submitter_user_id: int,
    topic_id: Optional[int] = None
) -> bool:
    """
    Forward submission to the verification group and post the moderation card.
    If topic_id is provided, it forwards to that specific forum topic.
    """
    if not messages:
        logger.warning(
            "forward_to_verification called with empty message list",
            extra={"ctx_user_id": submitter_user_id},
        )
        return False

    group_id = settings.VERIFICATION_GROUP_ID
    first_msg_id = messages[0].id

    forwarded_ids: list[int] = []
    for msg in messages:
        fwd = await _forward_single(client, msg, group_id, submitter_user_id, topic_id)
        if fwd is None:
            return False
        forwarded_ids.append(fwd.id)

    last_fwd_id = forwarded_ids[-1]
    count = len(messages)
    media_label = "album" if count > 1 else "item"
    
    # Check if user is anonymous (F-02)
    from app.core.redis_client import get_redis
    redis = get_redis()
    is_anon = await redis.exists(f"user:anon:{submitter_user_id}")
    user_label = "Anonymous" if is_anon else f"User {submitter_user_id}"

    info_text = (
        f"📬 <b>New Submission</b>\n\n"
        f"👤 Submitter: <code>{user_label}</code> (<code>{submitter_user_id}</code>)\n"
        f"📦 Content: {count} {media_label}"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve NSFW", callback_data=f"mod_app_nsfw:{submitter_user_id}:{first_msg_id}"),
            InlineKeyboardButton("💎 Approve Premium", callback_data=f"mod_app_prem:{submitter_user_id}:{first_msg_id}")
        ],
        [
            InlineKeyboardButton("❌ Reject", callback_data=f"mod_reject:{submitter_user_id}:{first_msg_id}")
        ]
    ])

    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=group_id,
                text=info_text,
                reply_to_message_id=last_fwd_id,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                message_thread_id=topic_id
            )
            logger.info(
                "Verification card sent",
                extra={
                    "ctx_user_id": submitter_user_id,
                    "ctx_first_msg_id": first_msg_id,
                    "ctx_count": count,
                    "ctx_topic_id": topic_id
                },
            )
            return True

        except FloodWait as e:
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            await asyncio.sleep(wait)

        except RPCError as e:
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

    return False


async def _forward_single(client, msg, group_id, submitter_user_id, topic_id):
    for attempt in range(_MAX_RETRIES):
        try:
            # Using copy_message to preserve content and avoid file_id issues
            result = await client.copy_message(
                chat_id=group_id,
                from_chat_id=msg.chat.id,
                message_id=msg.id,
                message_thread_id=topic_id
            )
            return result
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
        except RPCError:
            if attempt == _MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)
    return None


def parse_callback_data(data: str) -> Optional[dict]:
    if not data:
        return None

    try:
        # mod_{action}:{submitter_id}:{msg_id}
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

    except (ValueError, AttributeError, IndexError):
        return None
