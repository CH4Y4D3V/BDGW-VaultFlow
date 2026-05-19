from __future__ import annotations

"""
Takedown handler — DMCA / report / content claim commands.

Public commands (any user, any chat):
  /report {content_id} {reason...}
  /dmca {content_id} {reason...}
  /content_claim {content_id} {reason...}

Admin-only commands:
  /execute_takedown {content_id}
  /dismiss_report {content_id}
  /pending_reports
"""

import asyncio

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

from app.core.permissions import Role, permission_required
from app.services.takedown_service import TakedownService
from app.utils.logger import get_logger

logger = get_logger(__name__)

_takedown_service = TakedownService()
_MAX_RETRIES = 3


# ── FloodWait-safe reply helper ───────────────────────────────────────────────

async def _safe_reply(message: Message, text: str) -> None:
    from app.config import settings
    for attempt in range(_MAX_RETRIES):
        try:
            await message.reply_text(text, parse_mode=ParseMode.HTML)
            return
        except FloodWait as e:
            wait = int(e.value) + settings.FLOODWAIT_EXTRA_BUFFER
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "Takedown handler: failed to send reply",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_RETRIES - 1:
                return
            await asyncio.sleep(2 ** attempt)


# ── /report ───────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("report"))
async def handle_report(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    parts = message.text.split(None, 2)
    if len(parts) < 3:
        await _safe_reply(
            message,
            "Usage: <code>/report {content_id} {reason}</code>\n"
            "Example: <code>/report chat123_456 Stolen content</code>",
        )
        return

    content_id = parts[1]
    reason = parts[2]
    reported_by = message.from_user.id

    try:
        record_id = await _takedown_service.submit_report(
            content_id=content_id,
            reported_by=reported_by,
            reason=reason,
            report_type="report",
        )
    except Exception as e:
        logger.error(
            "Failed to submit report",
            extra={"ctx_user_id": reported_by, "ctx_content_id": content_id, "ctx_error": str(e)},
        )
        await _safe_reply(message, "⚠️ Failed to submit report. Please try again later.")
        return

    await _safe_reply(
        message,
        f"✅ Report submitted. Content has been auto-locked pending review.\n"
        f"<i>Reference: <code>{record_id}</code></i>",
    )
    logger.info(
        "/report submitted",
        extra={"ctx_user_id": reported_by, "ctx_content_id": content_id, "ctx_record_id": record_id},
    )


# ── /dmca ─────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("dmca"))
async def handle_dmca(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    parts = message.text.split(None, 2)
    if len(parts) < 3:
        await _safe_reply(
            message,
            "Usage: <code>/dmca {content_id} {reason}</code>\n"
            "Example: <code>/dmca chat123_456 I own the copyright to this content</code>",
        )
        return

    content_id = parts[1]
    reason = parts[2]
    reported_by = message.from_user.id

    try:
        record_id = await _takedown_service.submit_report(
            content_id=content_id,
            reported_by=reported_by,
            reason=reason,
            report_type="dmca",
        )
    except Exception as e:
        logger.error(
            "Failed to submit DMCA",
            extra={"ctx_user_id": reported_by, "ctx_content_id": content_id, "ctx_error": str(e)},
        )
        await _safe_reply(message, "⚠️ Failed to submit DMCA claim. Please try again later.")
        return

    await _safe_reply(
        message,
        f"✅ DMCA claim submitted. Content locked pending legal review.\n"
        f"<i>Reference: <code>{record_id}</code></i>",
    )
    logger.info(
        "/dmca submitted",
        extra={"ctx_user_id": reported_by, "ctx_content_id": content_id, "ctx_record_id": record_id},
    )


# ── /content_claim ────────────────────────────────────────────────────────────

@Client.on_message(filters.command("content_claim"))
async def handle_content_claim(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    parts = message.text.split(None, 2)
    if len(parts) < 3:
        await _safe_reply(
            message,
            "Usage: <code>/content_claim {content_id} {reason}</code>\n"
            "Example: <code>/content_claim chat123_456 I am the original creator</code>",
        )
        return

    content_id = parts[1]
    reason = parts[2]
    reported_by = message.from_user.id

    try:
        record_id = await _takedown_service.submit_report(
            content_id=content_id,
            reported_by=reported_by,
            reason=reason,
            report_type="claim",
        )
    except Exception as e:
        logger.error(
            "Failed to submit content claim",
            extra={"ctx_user_id": reported_by, "ctx_content_id": content_id, "ctx_error": str(e)},
        )
        await _safe_reply(message, "⚠️ Failed to submit content claim. Please try again later.")
        return

    await _safe_reply(
        message,
        f"✅ Content claim submitted. Content locked pending review.\n"
        f"<i>Reference: <code>{record_id}</code></i>",
    )
    logger.info(
        "/content_claim submitted",
        extra={"ctx_user_id": reported_by, "ctx_content_id": content_id, "ctx_record_id": record_id},
    )


# ── /execute_takedown (admin only) ────────────────────────────────────────────
# ADVISORY fix: replaced inline is_moderator() check with @permission_required
# decorator matching the established pattern in admin_handler.py and payment_handler.py.
# Added user-facing denial message (handled by permission_required, silent=False default).

@Client.on_message(filters.command("execute_takedown"))
@permission_required(Role.MODERATOR)
async def handle_execute_takedown(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await _safe_reply(
            message,
            "Usage: <code>/execute_takedown {content_id}</code>",
        )
        return

    content_id = parts[1].strip()
    moderator_id = message.from_user.id

    try:
        success = await _takedown_service.execute_takedown(
            content_id=content_id,
            reviewed_by=moderator_id,
        )
    except Exception as e:
        logger.error(
            "Failed to execute takedown",
            extra={"ctx_moderator": moderator_id, "ctx_content_id": content_id, "ctx_error": str(e)},
        )
        await _safe_reply(message, f"⚠️ Failed to execute takedown: <code>{e}</code>")
        return

    if success:
        await _safe_reply(
            message,
            f"✅ Content <code>{content_id}</code> permanently removed.",
        )
    else:
        await _safe_reply(
            message,
            f"⚠️ Content <code>{content_id}</code> not found in vault or already removed.",
        )

    logger.warning(
        "/execute_takedown completed",
        extra={"ctx_moderator": moderator_id, "ctx_content_id": content_id, "ctx_success": success},
    )


# ── /dismiss_report (admin only) ─────────────────────────────────────────────
# ADVISORY fix: replaced inline is_moderator() check with @permission_required.

@Client.on_message(filters.command("dismiss_report"))
@permission_required(Role.MODERATOR)
async def handle_dismiss_report(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    parts = message.text.split(None, 1)
    if len(parts) < 2:
        await _safe_reply(
            message,
            "Usage: <code>/dismiss_report {content_id}</code>",
        )
        return

    content_id = parts[1].strip()
    moderator_id = message.from_user.id

    try:
        await _takedown_service.dismiss_report(
            content_id=content_id,
            reviewed_by=moderator_id,
        )
    except Exception as e:
        logger.error(
            "Failed to dismiss report",
            extra={"ctx_moderator": moderator_id, "ctx_content_id": content_id, "ctx_error": str(e)},
        )
        await _safe_reply(message, f"⚠️ Failed to dismiss report: <code>{e}</code>")
        return

    await _safe_reply(
        message,
        f"✅ Report dismissed. Content restored to distribution queue.",
    )
    logger.info(
        "/dismiss_report completed",
        extra={"ctx_moderator": moderator_id, "ctx_content_id": content_id},
    )


# ── /pending_reports (admin only) ─────────────────────────────────────────────
# ADVISORY fix: replaced inline is_moderator() check with @permission_required.

@Client.on_message(filters.command("pending_reports"))
@permission_required(Role.MODERATOR)
async def handle_pending_reports(client: Client, message: Message) -> None:
    try:
        reports = await _takedown_service.get_pending_reports()
    except Exception as e:
        logger.error("Failed to fetch pending reports", extra={"ctx_error": str(e)})
        await _safe_reply(message, "⚠️ Failed to fetch pending reports.")
        return

    if not reports:
        await _safe_reply(message, "✅ No pending reports.")
        return

    lines = ["📋 <b>Pending Takedown Reports</b>\n"]
    for r in reports:
        report_type = r.get("report_type", r.get("type", "unknown")).upper()
        content_id = r.get("content_id", "?")
        reported_by = r.get("reported_by", "?")
        created_at = r.get("created_at")
        date_str = created_at.strftime("%Y-%m-%d") if created_at else "?"
        lines.append(f"📋 {report_type} | <code>{content_id}</code> | by <code>{reported_by}</code> | {date_str}")

    await _safe_reply(message, "\n".join(lines))
    logger.info(
        "/pending_reports listed",
        extra={"ctx_user_id": message.from_user.id, "ctx_count": len(reports)},
    )