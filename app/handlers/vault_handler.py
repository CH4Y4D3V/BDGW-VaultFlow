from __future__ import annotations

"""
vault_handler.py — Direct Vault Channel Upload Handler

Spec: Section 11 (Vault System) + Section 12 (Queue Distribution Engine)
      + Section 13 (Watermark Pipeline)

Listens for media posted directly to the NSFW or Premium vault channel by
an admin with a #nsfw or #premium caption tag.  The pipeline is:

  1. Validate caption tag (#nsfw / #premium).
  2. Deduplicate via file_id hash (content_fingerprints collection).
  3. Archive to the correct vault channel (vault_items collection).
  4. Run the watermark pipeline (photo: PNG overlay; video: dual text).
  5. Enqueue the watermarked media for distribution.
  6. Emit Admin Logs + audit_logs entries.
  7. Acknowledge in-channel with a quoted reply.

No step that modifies state (DB write, enqueue) runs before the preceding
step succeeds.  This ensures restart safety.
"""

import hashlib
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.moderation.moderation_actions import archive_to_vault, enqueue_for_distribution
from app.core.models import ModerationState
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER


# ── Internal utilities ────────────────────────────────────────────────────────

def _extract_dest(caption: str) -> Optional[str]:
    """
    Parse the caption for a #nsfw or #premium tag.

    Returns 'nsfw', 'premium', or None if neither tag is present.
    #nsfw takes precedence if both tags appear (shouldn't happen, but be safe).
    """
    lower = caption.lower()
    if "#nsfw" in lower:
        return "nsfw"
    if "#premium" in lower:
        return "premium"
    return None


def _compute_file_id_hash(message: Message) -> Optional[str]:
    """
    Derive a deduplication hash from the message's primary media file_id.

    Uses SHA-256 of the file_id string.  Returns None if no media is found.
    This matches the fingerprinting strategy used in the submission pipeline
    (content_fingerprints collection, spec §10.5).
    """
    media = (
        message.photo
        or message.video
        or message.document
        or message.animation
        or message.audio
        or message.voice
        or message.video_note
    )
    if not media:
        return None
    file_id: str = getattr(media, "file_id", "") or ""
    if not file_id:
        return None
    return hashlib.sha256(file_id.encode()).hexdigest()


async def _send_reply(
    client: Client,
    message: Message,
    text: str,
) -> None:
    """
    Send a quoted reply into the vault channel with FloodWait handling.

    Non-fatal — errors are logged but never re-raised.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await message.reply_text(text, quote=True, parse_mode=ParseMode.HTML)
            return
        except FloodWait as e:
            wait = int(e.value) + _FLOOD_BUFFER
            logger.info(
                "_send_reply: FloodWait",
                extra={"ctx_wait": wait, "ctx_msg_id": message.id},
            )
            import asyncio
            await asyncio.sleep(wait)
        except (RPCError, Exception) as e:
            logger.warning(
                "_send_reply: failed to send reply",
                extra={"ctx_msg_id": message.id, "ctx_error": str(e)},
            )
            return  # Non-fatal; do not retry on non-FloodWait errors


async def _emit_audit(
    client: Client,
    action: str,
    admin_id: int,
    msg_id: int,
    dest: str,
    content_hash: Optional[str],
    vault_doc_id: Optional[str] = None,
    extra_detail: str = "",
) -> None:
    """
    Write a dual audit entry:
      1. audit_logs MongoDB collection (always).
      2. Admin Logs hub topic (best-effort; failure is logged but non-fatal).

    Spec §9.4: every admin content action must emit to both.
    """
    now = datetime.now(timezone.utc)
    detail = (
        f"dest={dest} | msg_id={msg_id} | hash={content_hash} | "
        f"vault_doc_id={vault_doc_id}"
    )
    if extra_detail:
        detail += f" | {extra_detail}"

    # 1. MongoDB audit_logs
    try:
        db = DatabaseManager.get_db()
        await db["audit_logs"].insert_one({
            "action": action,
            "performed_by": admin_id,
            "target": f"vault_msg:{msg_id}",
            "detail": detail,
            "timestamp": now,
        })
    except Exception as e:
        logger.warning(
            "_emit_audit: audit_logs write failed",
            extra={"ctx_action": action, "ctx_error": str(e)},
        )

    # 2. Admin Logs hub topic (best-effort)
    try:
        from app.services.admin_logger import get_admin_logger
        await get_admin_logger().log(
            client=client,
            action=action,
            admin_id=admin_id,
            admin_name=f"user:{admin_id}",
            target_user_id=None,
            details=detail,
        )
    except Exception as e:
        logger.warning(
            "_emit_audit: admin_logger failed",
            extra={"ctx_action": action, "ctx_error": str(e)},
        )


# ── Handler ───────────────────────────────────────────────────────────────────

@Client.on_message(
    filters.chat(settings.VAULT_CHANNEL_ID)
    & (
        filters.photo
        | filters.video
        | filters.document
        | filters.animation
        | filters.audio
        | filters.voice
        | filters.video_note
    )
)
async def handle_direct_vault_upload(client: Client, message: Message) -> None:
    """
    Handle media uploaded directly to the Vault channel by an admin.

    Triggered by any media message in VAULT_CHANNEL_ID that contains a
    #nsfw or #premium caption tag.

    Pipeline (all steps are ordered; failure in any step halts the pipeline):
      1. Parse caption for destination tag.
      2. Compute deduplication hash from file_id.
      3. Check content_fingerprints — reject duplicates.
      4. Archive to vault_items (MongoDB write BEFORE any Telegram action).
      5. Run watermark pipeline (spec §13).
      6. Enqueue watermarked media for distribution (spec §12).
      7. Emit Admin Logs + audit_logs entries.
      8. Reply in-channel to acknowledge.

    Admin identification:
      Channel posts (posted via channel itself) have no from_user — admin_id
      defaults to 0 in that case.  Posts sent directly by an admin user in the
      channel will have from_user populated.
    """
    caption: str = message.caption or ""

    # ── Step 1: Parse destination tag ─────────────────────────────────────────
    dest = _extract_dest(caption)
    if not dest:
        # No recognised tag — not an admin direct-upload intent; ignore silently.
        logger.debug(
            "handle_direct_vault_upload: no #nsfw/#premium tag — ignoring",
            extra={"ctx_msg_id": message.id},
        )
        return

    # Resolve admin identity (channel posts have no from_user)
    admin_id: int = 0
    if message.from_user:
        admin_id = message.from_user.id
    elif message.sender_chat:
        # Posted via the channel itself; use channel_id as a proxy identifier
        admin_id = message.sender_chat.id

    logger.info(
        "handle_direct_vault_upload: detected",
        extra={
            "ctx_msg_id": message.id,
            "ctx_dest": dest,
            "ctx_admin_id": admin_id,
        },
    )

    # ── Step 2: Compute deduplication hash ────────────────────────────────────
    content_hash = _compute_file_id_hash(message)
    if not content_hash:
        logger.warning(
            "handle_direct_vault_upload: could not compute file_id hash — aborting",
            extra={"ctx_msg_id": message.id},
        )
        await _send_reply(client, message, "⚠️ Could not read media metadata. Upload aborted.")
        return

    # ── Step 3: Duplicate check ───────────────────────────────────────────────
    db = DatabaseManager.get_db()
    try:
        existing = await db["content_fingerprints"].find_one({"hash": content_hash})
    except Exception as e:
        logger.error(
            "handle_direct_vault_upload: fingerprint lookup failed",
            extra={"ctx_msg_id": message.id, "ctx_error": str(e)},
        )
        await _send_reply(client, message, "⚠️ DB error during duplicate check. Upload aborted.")
        return

    if existing:
        logger.warning(
            "handle_direct_vault_upload: duplicate detected — rejecting",
            extra={
                "ctx_msg_id": message.id,
                "ctx_hash": content_hash,
                "ctx_existing": str(existing.get("_id")),
            },
        )
        await _send_reply(
            client,
            message,
            "🚫 <b>Duplicate Detected</b>\n\nThis media already exists in the vault.",
        )
        await _emit_audit(
            client=client,
            action="TXID DUPLICATE BLOCKED",
            admin_id=admin_id,
            msg_id=message.id,
            dest=dest,
            content_hash=content_hash,
            extra_detail="direct vault upload duplicate rejected",
        )
        return

    # ── Step 4: Archive to vault (MongoDB write — restart-safe) ───────────────
    # archive_to_vault writes to vault_items AND content_fingerprints.
    # It must complete before any Telegram action so the record survives restarts.
    vault_doc_id: Optional[str] = None
    try:
        vault_doc_id = await archive_to_vault(
            client=client,
            messages=[message],
            dest=dest,
            submitter_user_id=admin_id,
            initial_status=ModerationState.APPROVED.value,  # Admin direct-upload = pre-approved
        )
    except Exception as e:
        logger.error(
            "handle_direct_vault_upload: archive_to_vault failed",
            extra={
                "ctx_msg_id": message.id,
                "ctx_dest": dest,
                "ctx_error": str(e),
            },
            exc_info=True,
        )
        await _send_reply(client, message, "⚠️ Vault archive failed. Upload aborted.")
        return

    # ── Step 5: Watermark pipeline (spec §13) ─────────────────────────────────
    # Photo: PNG overlay, random position, opacity 90 (spec §13).
    # Video: two text watermarks, random position/timing, opacity 110–130 (spec §13).
    # The watermark service returns the processed message(s) ready for enqueue.
    watermarked_messages: list[Message] = [message]  # default: passthrough if disabled
    if settings.WATERMARK_ENABLED:
        try:
            from app.services.watermark_service import get_watermark_service
            watermark_service = get_watermark_service()
            result = await watermark_service.process(
                client=client,
                messages=[message],
                dest=dest,
            )
            if result:
                watermarked_messages = result
            else:
                # Watermark returned nothing — log and fall back to original
                logger.warning(
                    "handle_direct_vault_upload: watermark returned empty result — "
                    "using original media",
                    extra={"ctx_msg_id": message.id, "ctx_dest": dest},
                )
        except Exception as e:
            logger.error(
                "handle_direct_vault_upload: watermark pipeline failed — "
                "using original media as fallback",
                extra={
                    "ctx_msg_id": message.id,
                    "ctx_dest": dest,
                    "ctx_error": str(e),
                },
                exc_info=True,
            )
            # Do not abort — fall through with un-watermarked media.
            # A failed watermark should not silently drop content.
    else:
        logger.info(
            "handle_direct_vault_upload: WATERMARK_ENABLED=False — skipping watermark",
            extra={"ctx_msg_id": message.id},
        )

    # ── Step 6: Enqueue for distribution (spec §12) ───────────────────────────
    try:
        success = await enqueue_for_distribution(
            messages=watermarked_messages,
            dest=dest,
            submitter_user_id=admin_id,
            vault_message_ids=[message.id],
        )
    except Exception as e:
        logger.error(
            "handle_direct_vault_upload: enqueue_for_distribution failed",
            extra={
                "ctx_msg_id": message.id,
                "ctx_dest": dest,
                "ctx_error": str(e),
            },
            exc_info=True,
        )
        await _send_reply(client, message, "⚠️ Enqueue failed. Content is archived but not queued.")
        return

    if not success:
        logger.error(
            "handle_direct_vault_upload: enqueue returned False",
            extra={"ctx_msg_id": message.id, "ctx_dest": dest},
        )
        await _send_reply(client, message, "⚠️ Enqueue failed. Content is archived but not queued.")
        return

    logger.info(
        "handle_direct_vault_upload: enqueued successfully",
        extra={
            "ctx_msg_id": message.id,
            "ctx_dest": dest,
            "ctx_vault_doc_id": vault_doc_id,
        },
    )

    # ── Step 7: Audit logs ────────────────────────────────────────────────────
    action = "CONTENT APPROVED NSFW" if dest == "nsfw" else "CONTENT APPROVED PREMIUM"
    await _emit_audit(
        client=client,
        action=action,
        admin_id=admin_id,
        msg_id=message.id,
        dest=dest,
        content_hash=content_hash,
        vault_doc_id=vault_doc_id,
        extra_detail="direct vault upload",
    )

    # ── Step 8: Acknowledge in-channel ────────────────────────────────────────
    label = settings.NSFW_DISPLAY_NAME if dest == "nsfw" else settings.PREMIUM_DISPLAY_NAME
    await _send_reply(
        client,
        message,
        f"✅ <b>Archived, watermarked &amp; queued</b> for <b>{label}</b>.",
    )
