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
#   mod_approve:{submitter_id}:{msg_id}
#   mod_queue:{submitter_id}:{msg_id}
#   mod_reject:{submitter_id}:{msg_id}
#
# Step 2 — moderator picks destination (approve or queue):
#   mod_dest:{action}:{dest}:{submitter_id}:{msg_id}
#   action: "approve" | "queue"
#   dest:   "nsfw"    | "premium"
# ─────────────────────────────────────────────────────────────────────────────


async def forward_to_verification(
    client: Client,
    messages: list[Message],
    submitter_user_id: int,
) -> bool:
    """
    Forward submission to the verification group and post the 3-button
    moderation card (Approve / Queue / Reject) threaded under it.

    Returns True on full success, False on any terminal failure.
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
        fwd = await _forward_single(client, msg, group_id, submitter_user_id)
        if fwd is None:
            return False
        forwarded_ids.append(fwd.id)

    last_fwd_id = forwarded_ids[-1]
    count = len(messages)
    media_label = "album" if count > 1 else "item"
    caption_raw = (messages[0].caption or messages[0].text or "").strip()
    preview = f'\n<i>"{caption_raw[:120]}"</i>' if caption_raw else ""

    info_text = (
        f"📬 <b>New submission</b>\n"
        f"👤 Submitter: <code>{submitter_user_id}</code>\n"
        f"📦 Content: {count} {media_label}{preview}"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ Approve",
                callback_data=f"mod_approve:{submitter_user_id}:{first_msg_id}",
            ),
            InlineKeyboardButton(
                "⏳ Queue",
                callback_data=f"mod_queue:{submitter_user_id}:{first_msg_id}",
            ),
            InlineKeyboardButton(
                "❌ Reject",
                callback_data=f"mod_reject:{submitter_user_id}:{first_msg_id}",
            ),
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
                "FloodWait sending verification card",
                extra={"ctx_user_id": submitter_user_id, "ctx_wait": wait, "ctx_attempt": attempt + 1},
            )
            await asyncio.sleep(wait)

        except RPCError as e:
            logger.error(
                "RPC error sending verification card",
                extra={"ctx_user_id": submitter_user_id, "ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)

    return False


async def _forward_single(client, msg, group_id, submitter_user_id):
    for attempt in range(_MAX_RETRIES):
        try:
            result = await client.copy_message(
                chat_id=group_id,
                from_chat_id=msg.chat.id,
                message_id=msg.id,
            )
            return result
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
        except RPCError as e:
            if attempt == _MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)
    return None


def parse_callback_data(data: str) -> Optional[dict]:
    """
    Parse all moderation callback data into a structured dict.

    Step-1 format: mod_{action}:{submitter_id}:{msg_id}
    Step-2 format: mod_dest:{action}:{dest}:{submitter_id}:{msg_id}

    Returns dict with keys: step, action, submitter_id, msg_id, dest (step-2 only)
    Returns None on parse failure.
    """
    if not data:
        return None

    try:
        if data.startswith("mod_dest:"):
            # mod_dest:{action}:{dest}:{submitter_id}:{msg_id}
            parts = data.split(":", 4)
            if len(parts) != 5:
                return None
            _, action, dest, submitter_id_str, msg_id_str = parts
            if action not in ("approve", "queue"):
                return None
            if dest not in ("nsfw", "premium"):
                return None
            return {
                "step": 2,
                "action": action,
                "dest": dest,
                "submitter_id": int(submitter_id_str),
                "msg_id": int(msg_id_str),
            }
        else:
            # mod_{action}:{submitter_id}:{msg_id}
            parts = data.split(":", 2)
            if len(parts) != 3:
                return None
            action_raw, submitter_id_str, msg_id_str = parts
            action = action_raw.removeprefix("mod_")
            if action not in ("approve", "queue", "reject"):
                return None
            return {
                "step": 1,
                "action": action,
                "submitter_id": int(submitter_id_str),
                "msg_id": int(msg_id_str),
            }

    except (ValueError, AttributeError, IndexError):
        return None
