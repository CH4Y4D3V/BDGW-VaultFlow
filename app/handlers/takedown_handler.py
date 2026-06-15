from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from pyrogram import Client, ContinuePropagation, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.core.redis_client import get_redis
from app.services.takedown_service import TakedownService
from app.services.topic_manager import get_topic_manager, TOPIC_SUPPORT
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── FSM state keys (Redis) ────────────────────────────────────────────────────
STATE_IDLE = "idle"
STATE_AWAITING_ID = "awaiting_id"
STATE_AWAITING_REASON = "awaiting_reason"
STATE_AWAITING_LINK = "awaiting_link"

_takedown_service = TakedownService()
_admin_reject_states: dict[int, dict] = {}

_MAX_RETRIES = 3
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _get_fsm(user_id: int) -> tuple[str, dict]:
    redis = get_redis()
    state = await redis.get(f"state:takedown:{user_id}") or STATE_IDLE
    data_raw = await redis.get(f"data:takedown:{user_id}")
    data: dict = json.loads(data_raw) if data_raw else {}
    return state, data


async def _set_fsm(user_id: int, state: str, data: dict) -> None:
    redis = get_redis()
    if state == STATE_IDLE:
        await redis.delete(f"state:takedown:{user_id}", f"data:takedown:{user_id}")
    else:
        await redis.set(f"state:takedown:{user_id}", state, ex=3600)
        await redis.set(f"data:takedown:{user_id}", json.dumps(data), ex=3600)


async def _send_with_retry(
    client: Client,
    chat_id: int,
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
    **kwargs: Any,
) -> Optional[Message]:
    for attempt in range(_MAX_RETRIES):
        try:
            return await client.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                **kwargs,
            )
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            await asyncio.sleep(wait)
        except RPCError as e:
            if attempt == _MAX_RETRIES - 1: return None
            await asyncio.sleep(2 ** attempt)
        except Exception: return None
    return None


async def _post_takedown_card_to_hub(
    client: Client,
    user: object,
    record_id: str,
    reason: str,
    content_link: str,
) -> None:
    """
    BUG-3 FIX: Route the takedown card to the user's PERMANENT TOPIC in the
    Verification Hub (spec §14.3), not to the legacy HUB_TOPIC_TAKEDOWN.

    Previous code used `getattr(settings, "HUB_TOPIC_TAKEDOWN", 0)` which
    is a legacy multi-topic ID from the old architecture.  Per spec §9.3,
    every takedown request is routed into the user's permanent topic.
    """
    user_id: int = getattr(user, "id", 0)
    full_name = f"{getattr(user, 'first_name', '')} {getattr(user, 'last_name', '')}".strip() or "Unknown"
    username_str = f"@{user.username}" if getattr(user, "username", None) else "N/A"

    # Resolve user's permanent topic (creates it if missing)
    topic_manager = get_topic_manager()
    topic_id: Optional[int] = await topic_manager.get_or_create_user_topic(
        client=client,
        user_id=user_id,
        full_name=full_name,
    )

    card_text = (
        "🗑 <b>TAKEDOWN REQUEST</b>\n\n"
        f"👤 <b>User:</b> {full_name} ({username_str})\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
        f"📝 <b>Reason:</b> {reason}\n"
        f"🔗 <b>Link:</b> {content_link}\n"
        f"🕐 <b>Record ID:</b> <code>{record_id}</code>"
    )

    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve & Delete", callback_data=f"takedown:accept:{record_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"takedown:reject:{record_id}"),
        ]
    ])

    kwargs: dict = {"reply_markup": buttons}
    if topic_id:
        kwargs["message_thread_id"] = topic_id

    await _send_with_retry(
        client,
        chat_id=settings.VERIFICATION_GROUP_ID,
        text=card_text,
        **kwargs,
    )

async def _resolve_content_id_or_link(text: str) -> Optional[str]:
    import re
    text = text.strip()
    if not text: return None
    db = DatabaseManager.get_db()
    vault = db[settings.VAULT_COLLECTION]
    exists = await vault.find_one({"content_id": text})
    if exists: return text
    match = re.search(r"t\.me/c/(\d+)/(\d+)", text)
    if match:
        chat_id = f"-100{match.group(1)}"
        msg_id = int(match.group(2))
        doc = await vault.find_one({"vault_channel_id": chat_id, "vault_message_id": msg_id})
        if doc: return doc["content_id"]
    return None

async def _delete_after(msg: Message, delay: int = 10):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass

# ── User commands ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("takedown") & filters.private)
async def handle_takedown_start(client: Client, message: Message) -> None:
    """
    BUG-3 FIX: Reordered to match spec §14.2 exactly.

    Spec order:
      Step 1: "What is your reason for removal?" (free text, mandatory)
      Step 2: "Send the link or reference to the content." (any URL, mandatory)

    Previous code asked for Content ID/DB lookup first, which (a) violated the
    spec order and (b) rejected any valid t.me link not already in the vault DB,
    meaning users could never successfully submit a takedown.
    """
    user_id = message.from_user.id
    redis = get_redis()
    if await redis.exists(f"pay_session:{user_id}"):
        await message.reply_text("Active payment session detected. Please complete it first.")
        return

    # Clear any stale state before starting fresh
    await _set_fsm(user_id, STATE_IDLE, {})

    # Step 1: Ask for REASON first (spec §14.2 Step 1)
    await _set_fsm(user_id, STATE_AWAITING_REASON, {})
    await message.reply_text(
        "⚖️ <b>Takedown Request</b>\n\n"
        "📝 <b>Step 1 of 2</b>\n"
        "What is your reason for removal? Please describe why this content should be taken down.\n\n"
        "<i>Send /cancel to abort at any time.</i>",
        parse_mode=ParseMode.HTML,
    )


@Client.on_message(filters.command("cancel") & filters.private)
async def handle_takedown_cancel(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    state, _ = await _get_fsm(user_id)
    if state != STATE_IDLE:
        await _set_fsm(user_id, STATE_IDLE, {})
        await message.reply_text("❌ Takedown request cancelled.")


@Client.on_message(filters.private & ~filters.command(["takedown", "cancel", "start", "help"]))
async def handle_takedown_fsm(client: Client, message: Message) -> None:
    """
    BUG-3 FIX: FSM now follows spec §14.2 exactly:
      STATE_AWAITING_REASON → collect free-text reason
      STATE_AWAITING_LINK   → collect any URL or reference (no DB lookup required)

    Previous code had STATE_AWAITING_ID (DB vault lookup) which rejected any
    valid t.me link not already in the vault, making takedowns impossible.
    Link validation now accepts any non-empty text — the admin reviews it.
    """
    if not message.from_user:
        return
    user_id = message.from_user.id
    redis = get_redis()
    if await redis.exists(f"pay_session:{user_id}"):
        return
    state, data = await _get_fsm(user_id)
    if state == STATE_IDLE:
        raise ContinuePropagation

    # ── Step 1: Collect REASON (spec §14.2 Step 1) ───────────────────────────
    if state == STATE_AWAITING_REASON:
        reason = (message.text or "").strip()
        if not reason:
            await message.reply_text("❌ Please provide a reason as text.")
            return

        data["reason"] = reason
        await _set_fsm(user_id, STATE_AWAITING_LINK, data)
        await message.reply_text(
            "🔗 <b>Step 2 of 2</b>\n\n"
            "Send the link or reference to the content you want removed.\n"
            "(e.g. <code>https://t.me/c/...</code> or any message link)\n\n"
            "<i>Send /cancel to abort.</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Step 2: Collect LINK (spec §14.2 Step 2) ─────────────────────────────
    if state == STATE_AWAITING_LINK:
        # BUG-3 FIX: Accept any non-empty text as the link/reference.
        # The previous code did a DB vault lookup and rejected valid URLs that
        # hadn't been archived yet (e.g. t.me/c/2908207184/2356 → "Invalid").
        # Admins review the link manually, so no DB validation is needed here.
        content_link = (message.text or "").strip()
        if not content_link:
            await message.reply_text("❌ Please send the content link or reference.")
            return

        await _set_fsm(user_id, STATE_IDLE, {})

        # Save to DB
        db = DatabaseManager.get_db()
        from datetime import datetime, timezone
        record = {
            "user_id": user_id,
            "reason": data.get("reason", ""),
            "content_link": content_link,
            "status": "PENDING",
            "submitted_at": datetime.now(timezone.utc),
            "reviewed_at": None,
            "reviewed_by": None,
            "rejection_reason": None,
        }
        result = await db["takedown_requests"].insert_one(record)
        record_id = str(result.inserted_id)

        await message.reply_text(
            "✅ <b>Your takedown request has been submitted.</b>\n\n"
            "Our team will review it shortly and notify you of the outcome.",
            parse_mode=ParseMode.HTML,
        )

        # Route card to user's permanent topic in the hub (spec §14.3)
        asyncio.create_task(
            _post_takedown_card_to_hub(
                client,
                message.from_user,
                record_id,
                data.get("reason", ""),
                content_link,
            )
        )
        return

    # Stale STATE_AWAITING_ID from old code — clear it
    if state == STATE_AWAITING_ID:
        await _set_fsm(user_id, STATE_IDLE, {})
        raise ContinuePropagation


# ── Admin: Accept ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^takedown:accept:(.+)$"))
async def handle_takedown_accept_callback(client: Client, callback_query: CallbackQuery) -> None:
    """
    FIX (B-06): Post manual delete confirmation card.
    """
    from app.core.permissions import is_moderator
    if not await is_moderator(callback_query.from_user.id):
        await callback_query.answer("⛔ Unauthorized.", show_alert=True)
        return

    record_id = callback_query.matches[0].group(1)
    db = DatabaseManager.get_db()
    record = await db["takedown_requests"].find_one({"_id": ObjectId(record_id)})
    if not record: return

    content_id = record["content_id"]
    admin_name = callback_query.from_user.first_name or "Admin"
    
    # Update main card
    await callback_query.message.edit_text(
        callback_query.message.text.html + f"\n\n⏳ <b>Accepted by {admin_name}</b>\n<i>Please confirm deletion below.</i>",
        reply_markup=None,
        parse_mode=ParseMode.HTML
    )

    # Post confirmation card
    buttons = InlineKeyboardMarkup([[
        InlineKeyboardButton("🗑 Confirm DELETE", callback_data=f"takedown:confirm:{record_id}")
    ]])
    await callback_query.message.reply(
        f"⚠️ <b>Manual Action Required</b>\n\nConfirm deletion for Content ID: <code>{content_id}</code>",
        reply_markup=buttons,
        parse_mode=ParseMode.HTML
    )
    await callback_query.answer()

@Client.on_callback_query(filters.regex(r"^takedown:confirm:(.+)$"))
async def handle_takedown_confirm_callback(client: Client, callback_query: CallbackQuery) -> None:
    """
    Admin callback: confirm deletion.
    """
    from app.core.permissions import is_moderator
    if not await is_moderator(callback_query.from_user.id): return

    record_id = callback_query.matches[0].group(1)
    db = DatabaseManager.get_db()
    record = await db["takedown_requests"].find_one({"_id": ObjectId(record_id)})
    if not record: return

    content_id = record["content_id"]
    user_id = record.get("reported_by")
    admin_id = callback_query.from_user.id

    await _takedown_service.execute_takedown(content_id=content_id, reviewed_by=admin_id)
    
    await callback_query.message.edit_text(f"✅ <b>Content ID {content_id} DELETED</b>", reply_markup=None, parse_mode=ParseMode.HTML)
    await callback_query.answer("✅ Content Deleted")
    
    # Audit & User Notification
    if user_id:
        await client.send_message(user_id, "✅ <b>Takedown Approved & Deleted</b>", parse_mode=ParseMode.HTML)

# ── Admin: Reject ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^takedown:reject:(.+)$"))
async def handle_takedown_reject_callback(client: Client, callback_query: CallbackQuery) -> None:
    from app.core.permissions import is_moderator
    if not await is_moderator(callback_query.from_user.id): return
    record_id = callback_query.matches[0].group(1)
    _admin_reject_states[callback_query.from_user.id] = {"record_id": record_id, "card_message_id": callback_query.message.id}
    await callback_query.answer()
    await callback_query.message.reply("✏️ <b>Send rejection reason:</b>", parse_mode=ParseMode.HTML)


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID) & ~filters.bot)
async def handle_takedown_reject_reason(client: Client, message: Message) -> None:
    if not message.from_user: return
    admin_id = message.from_user.id
    state = _admin_reject_states.get(admin_id)
    if not state: return

    record_id = state["record_id"]
    reason = (message.text or "").strip()
    if not reason: return
    _admin_reject_states.pop(admin_id, None)

    db = DatabaseManager.get_db()
    record = await db["takedown_requests"].find_one_and_update({"_id": ObjectId(record_id), "status": "pending"}, {"$set": {"status": "rejected", "reviewed_by": admin_id, "rejection_reason": reason}})
    if not record: return

    user_id = record.get("reported_by")
    
    # Update card
    await client.edit_message_text(settings.VERIFICATION_GROUP_ID, state["card_message_id"], f"🗑 <b>TAKEDOWN REJECTED</b>\n\n📝 Reason: {reason}", parse_mode=ParseMode.HTML)

    # Notify User
    if user_id:
        await client.send_message(user_id, f"❌ <b>Takedown Rejected</b>\n\nReason: {reason}", parse_mode=ParseMode.HTML)
    
    ack = await message.reply_text("✅ Rejection recorded.")
    # FIX (B-07): Auto-delete admin response after 10 seconds
    asyncio.create_task(_delete_after(ack, 10))
    asyncio.create_task(_delete_after(message, 10))
