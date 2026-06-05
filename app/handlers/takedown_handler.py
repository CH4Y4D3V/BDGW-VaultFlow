from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from bson import ObjectId
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.config import settings
from app.core.redis_client import get_redis
from app.core.database import DatabaseManager
from app.services.takedown_service import TakedownService
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── FSM Keys ──
# state:takedown:{user_id} -> current state
# data:takedown:{user_id}  -> JSON string with collected data

STATE_IDLE = "idle"
STATE_AWAITING_ID = "awaiting_id"
STATE_AWAITING_REASON = "awaiting_reason"
STATE_AWAITING_LINK = "awaiting_link"

_takedown_service = TakedownService()

# In-memory admin FSM for reject-reason prompt
# Key: admin_id -> {"record_id": str, "step": str, "card_message_id": int}
_admin_reject_states: dict[int, dict] = {}


async def _get_fsm(user_id: int):
    redis = get_redis()
    state = await redis.get(f"state:takedown:{user_id}") or STATE_IDLE
    import json
    data_raw = await redis.get(f"data:takedown:{user_id}")
    data = json.loads(data_raw) if data_raw else {}
    return state, data


async def _set_fsm(user_id: int, state: str, data: dict):
    redis = get_redis()
    import json
    if state == STATE_IDLE:
        await redis.delete(f"state:takedown:{user_id}", f"data:takedown:{user_id}")
    else:
        await redis.set(f"state:takedown:{user_id}", state, ex=3600)
        await redis.set(f"data:takedown:{user_id}", json.dumps(data), ex=3600)


async def _post_takedown_card_to_hub(
    client: Client,
    user: object,
    record_id: str,
    reason: str,
    content_link: str,
) -> None:
    """
    FLOW 5: Forward formatted takedown card to HUB_TOPIC_TAKEDOWN.
    Non-fatal — failure is logged but does not block the user confirmation.
    """
    topic_id = getattr(settings, "HUB_TOPIC_TAKEDOWN", 0) or None
    if not topic_id:
        logger.warning(
            "takedown_hub_topic_not_configured",
            extra={"ctx_record_id": record_id},
        )
        return

    user_id = getattr(user, "id", 0)
    first_name = getattr(user, "first_name", "") or ""
    last_name = getattr(user, "last_name", "") or ""
    full_name = f"{first_name} {last_name}".strip() or "Unknown"
    username = getattr(user, "username", None)
    username_str = f"@{username}" if username else "N/A"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    card_text = (
        "🗑 <b>TAKEDOWN REQUEST</b>\n\n"
        f"👤 <b>User:</b> {full_name} ({username_str})\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
        f"📝 <b>Reason:</b> {reason}\n"
        f"🔗 <b>Link:</b> {content_link}\n"
        f"🕒 <b>Time:</b> {now_str}\n"
        f"🆔 <b>Record:</b> <code>{record_id}</code>"
    )

    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ Approve & Delete",
                callback_data=f"takedown:approve:{record_id}",
            ),
            InlineKeyboardButton(
                "❌ Reject",
                callback_data=f"takedown:reject:{record_id}",
            ),
        ]
    ])

    for attempt in range(3):
        try:
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=card_text,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML,
                message_thread_id=topic_id,
            )
            logger.info(
                "takedown_card_posted_to_hub",
                extra={"ctx_record_id": record_id, "ctx_user_id": user_id},
            )
            return
        except FloodWait as e:
            await asyncio.sleep(int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER)
        except RPCError as e:
            logger.error(
                "takedown_hub_post_failed",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == 2:
                return
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(
                "takedown_hub_post_unexpected",
                extra={"ctx_error": str(e)},
            )
            return


async def _resolve_content_id_or_link(text: str) -> Optional[str]:
    """
    Resolves either a direct content_id or a Telegram message link
    to a valid content_id from the vault.
    """
    text = text.strip()
    if not text:
        return None

    db = DatabaseManager.get_db()
    vault = db[settings.VAULT_COLLECTION]

    # 1. Try direct content_id match
    exists = await vault.find_one({"content_id": text})
    if exists:
        return text

    # 2. Try parsing as Telegram link
    # Format: https://t.me/c/2505469098/1934
    import re
    match = re.search(r"t\.me/c/(\d+)/(\d+)", text)
    if match:
        chat_id_raw = match.group(1)
        msg_id = int(match.group(2))
        # Pyrogram uses -100 prefix for supergroups/channels
        chat_id = f"-100{chat_id_raw}"
        
        # Search by vault coordinates
        doc = await vault.find_one({
            "vault_channel_id": chat_id,
            "vault_message_id": msg_id
        })
        if doc:
            return doc["content_id"]

    return None


@Client.on_message(filters.command("takedown") & filters.private)
async def handle_takedown_start(client: Client, message: Message) -> None:
    user_id = message.from_user.id

    # FIX 2: Payment session guard — never intercept payment flow messages
    redis = get_redis()
    if await redis.exists(f"pay_session:{user_id}"):
        await message.reply_text(
            "You have an active payment session. Please complete or cancel "
            "it before submitting a takedown request."
        )
        return

    # ── Check for direct argument
    parts = message.text.split(None, 1)
    if len(parts) > 1:
        content_id = await _resolve_content_id_or_link(parts[1])
        if not content_id:
            await message.reply_text("❌ Invalid Content ID or Link. Please check and try again.")
            return

        db = DatabaseManager.get_db()
        reported = await db["takedown_requests"].find_one({
            "content_id": content_id,
            "reported_by": user_id,
            "status": "pending",
        })
        if reported:
            await message.reply_text(
                "⏳ <b>Already Under Review</b>\n\n"
                "You have already reported this content. Our admins are reviewing it.",
                parse_mode=ParseMode.HTML,
            )
            return

        await _set_fsm(user_id, STATE_AWAITING_REASON, {"content_id": content_id})
        await message.reply_text(
            f"📝 <b>Reporting Content:</b> <code>{content_id}</code>\n\n"
            "Please describe why this content should be removed.",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Default: Start guided flow
    await _set_fsm(user_id, STATE_AWAITING_ID, {})
    await message.reply_text(
        "⚖️ <b>Takedown Request / DMCA</b>\n\n"
        "Please provide the <b>Content ID</b> or <b>Link</b> you wish to report.\n"
        "<i>(Found in the caption of the shared content)</i>\n\n"
        "Type /cancel to abort.",
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
    if not message.from_user:
        return
    user_id = message.from_user.id

    # FIX 2: Payment session guard — never intercept payment flow messages
    redis = get_redis()
    if await redis.exists(f"pay_session:{user_id}"):
        return

    state, data = await _get_fsm(user_id)

    if state == STATE_IDLE:
        from pyrogram import ContinuePropagation
        raise ContinuePropagation

    if state == STATE_AWAITING_ID:
        # ── SUPPORT CONFLICT FIX ──
        # If the user has an active support topic, they are likely trying to
        # talk to support. We clear the takedown FSM and return.
        try:
            topic_manager = get_topic_manager()
            topic_id = await topic_manager.get_user_topic_id(user_id, TOPIC_SUPPORT)
            if topic_id:
                await _set_fsm(user_id, STATE_IDLE, {})
                return
        except Exception:
            pass

        content_id = await _resolve_content_id_or_link(message.text or "")
        if not content_id:
            await message.reply_text("❌ Invalid Content ID or Link. Please check and send again.")
            return

        data["content_id"] = content_id
        await _set_fsm(user_id, STATE_AWAITING_REASON, data)
        await message.reply_text(
            "📝 <b>Reason for Takedown</b>\n\n"
            "Please describe why this content should be removed "
            "(e.g., Copyright, Private, Other).",
            parse_mode=ParseMode.HTML,
        )
        return

    if state == STATE_AWAITING_REASON:
        data["reason"] = (message.text or "").strip()
        await _set_fsm(user_id, STATE_AWAITING_LINK, data)
        await message.reply_text(
            "🔗 <b>Proof / Identity Link</b>\n\n"
            "Please provide a link or description of your identity/proof of ownership "
            "for this request.",
            parse_mode=ParseMode.HTML,
        )
        return

    if state == STATE_AWAITING_LINK:
        data["link"] = (message.text or "").strip()
        await _set_fsm(user_id, STATE_IDLE, {})

        full_reason = f"Reason: {data['reason']}\nProof: {data['link']}"
        try:
            record_id = await _takedown_service.submit_report(
                content_id=data["content_id"],
                reported_by=user_id,
                reason=full_reason,
                report_type="takedown",
            )
        except Exception as e:
            logger.error(
                "takedown_submit_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                exc_info=True,
            )
            await message.reply_text(
                "❌ Failed to submit your request. Please try again later.",
            )
            return

        await message.reply_text(
            "✅ <b>Request Submitted</b>\n\n"
            f"Your request <code>{record_id}</code> has been received and is under review.\n"
            "The content has been automatically locked pending final decision.",
            parse_mode=ParseMode.HTML,
        )

        # FLOW 5: Post card to hub — non-fatal
        asyncio.create_task(
            _post_takedown_card_to_hub(
                client,
                message.from_user,
                record_id,
                data["reason"],
                data["link"],
            )
        )
        return


# ── Admin: Approve ────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^takedown:approve:(.+)$"))
async def handle_takedown_approve_callback(client: Client, callback: CallbackQuery) -> None:
    from app.core.permissions import is_moderator
    if not is_moderator(callback.from_user.id):
        await callback.answer("⛔ Unauthorized.", show_alert=True)
        return

    record_id = callback.data.split(":", 2)[2]
    await callback.answer("Processing...")

    try:
        db = DatabaseManager.get_db()
        record = await db["takedown_requests"].find_one({"_id": ObjectId(record_id)})
        if not record:
            await callback.answer("Record not found.", show_alert=True)
            return

        content_id = record.get("content_id", "")
        user_id = record.get("reported_by")

        # Execute takedown
        success = await _takedown_service.execute_takedown(
            content_id=content_id,
            reviewed_by=callback.from_user.id,
        )

        admin_id = callback.from_user.id
        admin_name = callback.from_user.first_name or "Admin"

        # ── LOG TO ADMIN LOGS ──
        try:
            from app.services.admin_logger import get_admin_logger
            await get_admin_logger().log(
                client=client,
                action="TAKEDOWN APPROVED",
                admin_id=admin_id,
                admin_name=admin_name,
                target_user_id=user_id,
                details=f"Content ID: {content_id}"
            )
        except Exception:
            pass

        suffix = f"\n\n✅ <b>Approved & deleted by {admin_name}</b>"
        try:
            msg = callback.message
            await msg.edit_text(
                (msg.text or "") + suffix,
                reply_markup=None,
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

        # Notify user
        if user_id:
            try:
                await client.send_message(
                    chat_id=user_id,
                    text=(
                        "✅ <b>Your content removal request has been approved.</b>\n\n"
                        "The content has been removed from our platform."
                    ),
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.warning(
                    "takedown_approve_notify_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

    except Exception as e:
        logger.error(
            "takedown_approve_callback_failed",
            extra={"ctx_record_id": record_id, "ctx_error": str(e)},
            exc_info=True,
        )
        await callback.answer("⚠️ Error processing approval.", show_alert=True)


# ── Admin: Reject (step 1 — prompt for reason) ───────────────────────────────

@Client.on_callback_query(filters.regex(r"^takedown:reject:(.+)$"))
async def handle_takedown_reject_callback(client: Client, callback: CallbackQuery) -> None:
    from app.core.permissions import is_moderator
    if not is_moderator(callback.from_user.id):
        await callback.answer("⛔ Unauthorized.", show_alert=True)
        return

    record_id = callback.data.split(":", 2)[2]
    admin_id = callback.from_user.id

    _admin_reject_states[admin_id] = {
        "record_id": record_id,
        "card_message_id": callback.message.id,
    }

    await callback.answer()
    try:
        await callback.message.reply(
            "✏️ <b>Rejection Reason Required</b>\n\n"
            f"Type your reason for rejecting takedown request <code>{record_id}</code>.\n"
            "Your next message will be used as the reason.",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(
            "takedown_reject_prompt_failed",
            extra={"ctx_error": str(e)},
        )


# ── Admin: Reject reason reply ────────────────────────────────────────────────

@Client.on_message(
    filters.chat(settings.VERIFICATION_GROUP_ID) & ~filters.bot
)
async def handle_takedown_reject_reason(client: Client, message: Message) -> None:
    """
    Catch the admin's typed rejection reason for a pending takedown reject.
    Only fires when the admin has an active _admin_reject_states entry.
    """
    if not message.from_user:
        return

    admin_id = message.from_user.id
    state = _admin_reject_states.get(admin_id)
    if not state:
        return  # No pending reject — let other handlers process

    record_id = state["record_id"]
    reason = (message.text or "").strip()

    if not reason:
        await message.reply_text("❌ Rejection reason cannot be empty.")
        return

    _admin_reject_states.pop(admin_id, None)

    try:
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)
        record = await db["takedown_requests"].find_one_and_update(
            {"_id": ObjectId(record_id), "status": "pending"},
            {
                "$set": {
                    "status": "rejected",
                    "reviewed_by": admin_id,
                    "reviewed_at": now,
                    "rejection_reason": reason,
                }
            },
        )

        if not record:
            await message.reply_text("❌ Record not found or already reviewed.")
            return

        user_id = record.get("reported_by")

        # Update admin card
        card_msg_id = state.get("card_message_id")
        if card_msg_id:
            try:
                admin_name = message.from_user.first_name or "Admin"
                await client.edit_message_text(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    message_id=card_msg_id,
                    text=(
                        f"🗑 <b>TAKEDOWN REQUEST — REJECTED</b>\n\n"
                        f"❌ Rejected by {admin_name}\n"
                        f"📝 Reason: {reason}"
                    ),
                    reply_markup=None,
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.warning(
                    "takedown_reject_card_edit_failed",
                    extra={"ctx_error": str(e)},
                )

        # Notify user
        if user_id:
            try:
                await client.send_message(
                    chat_id=user_id,
                    text=(
                        "❌ <b>Your takedown request was reviewed and not approved.</b>\n\n"
                        f"<b>Reason:</b> {reason}\n\n"
                        "A support ticket has been opened. You may reply here to discuss further."
                    ),
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.warning(
                    "takedown_reject_notify_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

            # AUTO-OPEN support interaction
            try:
                from app.services.support_service import get_support_service
                from app.services.topic_manager import get_topic_manager

                topic_manager = get_topic_manager()
                topic_id = await topic_manager.get_or_create_user_topic(
                    client, user_id
                )

                support_service = get_support_service()
                # Inject rejection context as message in topic
                await client.send_message(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    text=(
                        f"🗑 <b>Auto-opened: Takedown Rejection</b>\n\n"
                        f"User <code>{user_id}</code> rejection reason:\n{reason}"
                    ),
                    message_thread_id=topic_id,
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.warning(
                    "takedown_reject_support_open_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )

        await message.reply_text(f"✅ Takedown rejected. User notified.\nReason: {reason}")

        # Cleanup reply message and prompt
        try:
            await message.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(
            "takedown_reject_reason_handler_failed",
            extra={"ctx_record_id": record_id, "ctx_error": str(e)},
            exc_info=True,
        )
        await message.reply_text("⚠️ Error processing rejection.")