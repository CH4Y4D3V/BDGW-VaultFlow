from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

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
from app.core.permissions import Role, permission_required
from app.core.database import DatabaseManager
from app.services.audit_service import get_audit, AuditAction
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Broadcast State ──
# We use a simple in-memory state for the broadcast flow.
# In a multi-worker setup, this should be in Redis, but for single admin use, this is fine.
_pending_broadcasts: dict[int, dict] = {}


async def _safe_send(client: Client, user_id: int, message: Message) -> bool:
    """Send a copy of a message with FloodWait handling."""
    try:
        await message.copy(chat_id=user_id)
        return True
    except FloodWait as e:
        await asyncio.sleep(e.value + settings.FLOODWAIT_EXTRA_BUFFER)
        # One retry after floodwait
        try:
            await message.copy(chat_id=user_id)
            return True
        except Exception:
            return False
    except Exception:
        return False


@Client.on_message(
    filters.command(["broadcast", "broadcast_media", "broadcast_album", "broadcast_file"])
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.SUDO)
async def handle_broadcast_init(client: Client, message: Message) -> None:
    admin_id = message.from_user.id
    cmd = message.command[0]
    
    _pending_broadcasts[admin_id] = {
        "type": cmd,
        "content": None,
        "started_at": datetime.now(timezone.utc)
    }
    
    await message.reply_text(
        f"📢 <b>Broadcast Initialized [{cmd}]</b>\n\n"
        "Please send the message (text, photo, video, or album) you want to broadcast now.\n\n"
        "<i>To cancel, type /cancel</i>",
        parse_mode=ParseMode.HTML
    )


@Client.on_message(filters.chat(settings.VERIFICATION_GROUP_ID) & ~filters.command(["cancel"]))
async def handle_broadcast_content(client: Client, message: Message) -> None:
    admin_id = message.from_user.id
    if admin_id not in _pending_broadcasts or _pending_broadcasts[admin_id]["content"] is not None:
        return

    _pending_broadcasts[admin_id]["content"] = message
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm & Send", callback_data=f"bc_confirm:{admin_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"bc_cancel:{admin_id}")
        ]
    ])
    
    await message.reply_text(
        "📝 <b>Content Received</b>\n\n"
        "Please review the content above. If it looks correct, click confirm to begin the broadcast to ALL users.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        reply_to_message_id=message.id
    )


@Client.on_callback_query(filters.regex(r"^bc_confirm:(?P<aid>\d+)$"))
async def handle_broadcast_confirm(client: Client, callback: CallbackQuery) -> None:
    admin_id = int(callback.matches[0].group("aid"))
    if admin_id != callback.from_user.id:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    broadcast_data = _pending_broadcasts.pop(admin_id, None)
    if not broadcast_data or not broadcast_data["content"]:
        await callback.answer("Broadcast session expired.", show_alert=True)
        return

    await callback.message.edit_text("🚀 <b>Broadcast Started</b>\n\nProgress will be logged to the Audit thread.")
    
    # Run broadcast in background task
    asyncio.create_task(_execute_broadcast(client, broadcast_data["content"], admin_id))


async def _execute_broadcast(client: Client, content_message: Message, admin_id: int) -> None:
    db = DatabaseManager.get_db()
    users_cursor = db["users"].find({"is_banned": False})
    
    total_users = await db["users"].count_documents({"is_banned": False})
    sent_count = 0
    fail_count = 0
    
    start_time = datetime.now(timezone.utc)
    
    # Audit Log Init
    await get_audit().log(
        action=AuditAction.SUB_GRANT, # Reusing action or add BROADCAST_INIT
        performed_by=admin_id,
        details={"type": "broadcast_started", "total_targets": total_users}
    )

    async for user_doc in users_cursor:
        user_id = user_doc["_id"]
        success = await _safe_send(client, user_id, content_message)
        if success:
            sent_count += 1
        else:
            fail_count += 1
            
        # Progress update every 50 users
        if (sent_count + fail_count) % 50 == 0:
            logger.info("Broadcast in progress", extra={"sent": sent_count, "failed": fail_count, "total": total_users})

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    
    summary = (
        f"✅ <b>Broadcast Complete</b>\n\n"
        f"┣ 👤 <b>Targets:</b> {total_users}\n"
        f"┣ ✨ <b>Delivered:</b> {sent_count}\n"
        f"┣ ❌ <b>Failed:</b> {fail_count}\n"
        f"┗ ⏱ <b>Duration:</b> {duration:.1f}s"
    )
    
    # Log summary to Audit thread
    if settings.HUB_TOPIC_AUDIT:
        try:
            await client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=f"[BROADCAST_COMPLETE] | Admin: {admin_id} | Details: {summary}",
                message_thread_id=settings.HUB_TOPIC_AUDIT,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

    await get_audit().log(
        action="broadcast_complete",
        performed_by=admin_id,
        details={"sent": sent_count, "failed": fail_count, "duration": duration}
    )


@Client.on_callback_query(filters.regex(r"^bc_cancel:(?P<aid>\d+)$"))
async def handle_broadcast_cancel_cb(client: Client, callback: CallbackQuery) -> None:
    admin_id = int(callback.matches[0].group("aid"))
    if admin_id != callback.from_user.id:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    _pending_broadcasts.pop(admin_id, None)
    await callback.message.edit_text("❌ <b>Broadcast Cancelled</b>")
