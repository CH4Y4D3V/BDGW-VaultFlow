"""
app/bot/delivery.py

Vault-First delivery pipeline.

All messages are sent as copies from the vault channel (copy_message /
copy_media_group), never forwarded.  This ensures every delivered message
appears in the bot's name with no "Forwarded from" attribution, in compliance
with the spec.

Vault reference resolution supports three schema versions for backwards
compatibility with jobs enqueued before schema migrations:

  Schema v2 — vault_chat_id + vault_message_id   (direct fields, new jobs)
  Schema v1 — source_channel_id label + metadata.message_id (legacy jobs)
  Schema v0 — content_id encoded as '{chat_id}_{msg_id}_{type}' (oldest jobs)

NSFW / Premium vault separation (§11):
  'submission_nsfw'    → NSFW Vault Channel  (copy-message source)
  'submission_premium' → Premium Vault Channel (copy-message source)

  'submission_main' is intentionally absent.  The spec defines no generic
  vault — all content is either NSFW or Premium.  Any job with an unknown
  label falls through to the content_id parser.

  IMPORTANT: The vault channels mapped here are the ARCHIVE sources,
  NOT the distribution target groups.  NSFW_VAULT_CHANNEL_ID != NSFW_GROUP_ID.
"""

from __future__ import annotations

from typing import List, Tuple

from pyrogram.client import Client
from pyrogram.errors import FloodWait, RPCError

from app.bot.client import get_bot
from app.config.settings import settings
from app.core.exceptions import (
    DispatcherError,
    FloodWaitError,
    PermanentDeliveryError,
    VaultReferenceMissingError,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ── VAULT SOURCE CHANNEL MAP ──────────────────────────────────────────────────
#
# Maps source_channel_id label strings to the Telegram VAULT CHANNEL IDs
# from which content should be copied.  These are the ARCHIVE SOURCE channels,
# not the distribution target groups.
#
# NSFW Vault channel ID resolution order:
#   settings.NSFW_VAULT_CHANNEL_ID  (canonical — preferred)
#   settings.NSFW_VAULT_ID          (alternate name)
#   0 → sentinel; resolution will fall through to content_id parser at runtime
#
# Premium Vault channel ID resolution order:
#   settings.PREMIUM_VAULT_CHANNEL_ID  (canonical — preferred)
#   settings.PREMIUM_VAULT_ID          (alternate name)
#   settings.PREMIUM_CHANNEL_ID        (legacy fallback)
#   settings.PREMIUM_GROUP_ID          (last resort)
#   0 → sentinel
#
# 'submission_main' is deliberately absent — no generic vault exists in
# the dual-vault architecture (§11).
#
_CHANNEL_LABEL_MAP: dict[str, str] = {
    "submission_nsfw": str(
        getattr(settings, "NSFW_VAULT_CHANNEL_ID", None)
        or getattr(settings, "NSFW_VAULT_ID", None)
        or 0
    ),
    "submission_premium": str(
        getattr(settings, "PREMIUM_VAULT_CHANNEL_ID", None)
        or getattr(settings, "PREMIUM_VAULT_ID", None)
        or getattr(settings, "PREMIUM_CHANNEL_ID", None)
        or getattr(settings, "PREMIUM_GROUP_ID", 0)
    ),
}

# Values that indicate an unconfigured / missing vault channel ID.
_UNSET_CHANNEL_SENTINELS: frozenset[str] = frozenset({"0", "None", ""})


def _resolve_vault_ref(job: dict) -> Tuple[str, int]:
    """
    Resolve the vault copy-source (chat_id, message_id) from a job document.

    Handles three schema versions in priority order:

    Schema v2 — direct vault fields (new jobs):
        Reads vault_chat_id and vault_message_id directly from the job
        document.  Both must be non-null and non-zero.

    Schema v1 — label + metadata.message_id (legacy jobs):
        Looks up source_channel_id in _CHANNEL_LABEL_MAP to get the vault
        channel ID, then reads message_id from job['metadata']['message_id'].
        Skipped if the resolved channel ID is a sentinel (0 / None / ""),
        indicating the vault channel is not configured in settings.

    Schema v0 — encoded content_id (oldest jobs):
        Parses content_id formatted as '{chat_id}_{msg_id}_{type}', e.g.
        '-1003958888080_10_premium'.  The chat_id segment is a negative
        integer (no underscores in its digits), so the first '_'-split token
        is always the complete chat ID.

    Args:
        job: Raw MongoDB job document dict.

    Returns:
        Tuple of (str(chat_id), int(message_id)) identifying the vault message
        to copy from.

    Raises:
        VaultReferenceMissingError: Raised when none of the three schema
            versions yields a resolvable reference.  Callers should move
            the job to the dead-letter queue (§12.3).
    """
    job_id = str(job.get("_id", "unknown"))

    # ── Schema v2: direct vault fields ────────────────────────────────────────
    vault_chat_id = job.get("vault_chat_id")
    vault_message_id = job.get("vault_message_id")
    if vault_chat_id and vault_message_id:
        return str(vault_chat_id), int(vault_message_id)

    # ── Schema v1: label + metadata.message_id ────────────────────────────────
    source_label = job.get("source_channel_id")
    meta_msg_id = (job.get("metadata") or {}).get("message_id")

    if source_label and meta_msg_id:
        resolved_channel = _CHANNEL_LABEL_MAP.get(source_label)
        if resolved_channel and resolved_channel not in _UNSET_CHANNEL_SENTINELS:
            return resolved_channel, int(meta_msg_id)
        # Label not in map or vault channel not configured — log and fall through.
        logger.warning(
            "vault_ref_v1_unresolvable",
            extra={
                "ctx_job_id": job_id,
                "ctx_label": source_label,
                "ctx_resolved_channel": resolved_channel,
                "ctx_reason": (
                    "label not in _CHANNEL_LABEL_MAP"
                    if resolved_channel is None
                    else "vault channel ID not configured in settings"
                ),
            },
        )

    # ── Schema v0: parse content_id = '{chat_id}_{msg_id}_{type}' ────────────
    # Chat IDs are negative integers with no underscores, so splitting on the
    # first two underscores always yields [chat_id, msg_id, type_suffix].
    # Example: '-1003958888080_10_premium' → ['-1003958888080', '10', 'premium']
    content_id = job.get("content_id", "")
    if content_id:
        parts = content_id.split("_", 2)
        if len(parts) >= 2:
            try:
                parsed_chat_id = parts[0]
                parsed_msg_id = int(parts[1])
                if parsed_chat_id.lstrip("-").isdigit():
                    logger.info(
                        "vault_ref_resolved_from_content_id",
                        extra={
                            "ctx_job_id": job_id,
                            "ctx_chat_id": parsed_chat_id,
                            "ctx_msg_id": parsed_msg_id,
                        },
                    )
                    return parsed_chat_id, parsed_msg_id
            except (ValueError, IndexError):
                pass

    raise VaultReferenceMissingError(
        f"Job {job_id} has no resolvable vault reference. "
        f"Checked: vault_chat_id={vault_chat_id!r}, "
        f"vault_message_id={vault_message_id!r}, "
        f"source_channel_id={source_label!r}, "
        f"metadata.message_id={meta_msg_id!r}, "
        f"content_id={content_id!r}"
    )


async def execute_telegram_delivery(job_docs: List[dict], target_id: str) -> None:
    """
    Entry point for the DistributionEngine delivery pipeline.

    Sends one or more job documents to a Telegram target using vault-first
    copy semantics.  All messages are sent as copies (copy_message /
    copy_media_group), never forwarded.

    Routing:
        1 job  → _send_single  (copy_message)
        N jobs → _send_album   (copy_media_group with sequential fallback)

    Jobs are sorted by album_sequence_index before delivery to ensure
    correct album item ordering.  None indices are treated as 0.

    Args:
        job_docs:  One or more job documents belonging to the same delivery
                   unit (a single message or one complete album).
        target_id: Telegram channel or group ID to deliver content to.

    Raises:
        FloodWaitError:
            Telegram rate-limit encountered.  Caller should wait
            FloodWaitError.seconds before retrying.
        DispatcherError:
            Unrecoverable RPC error or unexpected exception during delivery.
            Caller should increment retry_count and eventually DLQ the job.
        PermanentDeliveryError:
            Permanent failure — do not retry.  Move job to dead-letter queue.
        VaultReferenceMissingError:
            No vault reference could be resolved.  Move job to dead-letter
            queue (§12.3).
    """
    bot = get_bot()
    if not job_docs:
        return

    # Sort by album_sequence_index; None is treated as 0 to handle jobs
    # that pre-date the album_sequence_index field.
    sorted_jobs = sorted(
        job_docs,
        key=lambda x: (
            x.get("album_sequence_index")
            if x.get("album_sequence_index") is not None
            else 0
        ),
    )

    try:
        if len(sorted_jobs) == 1:
            await _send_single(bot, sorted_jobs[0], target_id)
        else:
            await _send_album(bot, sorted_jobs, target_id)

    except FloodWait as exc:
        raise FloodWaitError(seconds=int(exc.value))
    except RPCError as exc:
        logger.error(
            "telegram_rpc_error_during_delivery",
            extra={"ctx_target": target_id, "ctx_error": str(exc)},
        )
        raise DispatcherError(f"RPC error delivering to {target_id}: {exc}")
    except (PermanentDeliveryError, VaultReferenceMissingError) as exc:
        logger.error(
            "permanent_or_unresolvable_delivery_failure",
            extra={"ctx_target": target_id, "ctx_error": str(exc)},
        )
        raise
    except Exception as exc:
        logger.error(
            "unexpected_delivery_failure",
            extra={"ctx_target": target_id, "ctx_error": str(exc)},
            exc_info=True,
        )
        raise DispatcherError(f"Unexpected error delivering to {target_id}: {exc}")


async def _send_single(bot: Client, job: dict, target_id: str) -> None:
    """
    Send a single message to the target by copying it from the vault.

    Uses Pyrogram's copy_message so the message appears in the bot's name
    with no forwarded-from attribution (spec requirement).  The caption from
    the job document is applied; a None caption preserves any caption on the
    original vault message.

    Args:
        bot:       Authenticated Pyrogram Client.
        job:       Job document containing vault reference fields and caption.
        target_id: Telegram channel or group ID to deliver to.

    Raises:
        VaultReferenceMissingError: If no vault reference is resolvable
            (propagates to execute_telegram_delivery).
        FloodWait:  Propagated to execute_telegram_delivery for handling.
        RPCError:   Propagated to execute_telegram_delivery for handling.
    """
    from_chat_id, message_id = _resolve_vault_ref(job)

    await bot.copy_message(
        chat_id=target_id,
        from_chat_id=from_chat_id,
        message_id=message_id,
        caption=job.get("caption") or None,
    )

    logger.info(
        "single_message_delivered",
        extra={
            "ctx_target": target_id,
            "ctx_from_chat": from_chat_id,
            "ctx_msg_id": message_id,
        },
    )


async def _send_album(bot: Client, sorted_jobs: List[dict], target_id: str) -> None:
    """
    Send a media album to the target from the vault.

    Primary strategy — copy_media_group:
        A single Telegram API call that copies all items in the media group.
        Preferred because it preserves Telegram's native album grouping and
        is atomic from the caller's perspective.  Vault reference is resolved
        from the first item only (Telegram delivers the entire group when
        given the first message ID).

        FloodWait raised here is re-raised immediately — no partial delivery
        has occurred, so retrying the full album after the wait is safe.

    Fallback strategy — sequential copy_message:
        Used when copy_media_group fails (e.g. Pyrogram version limitation,
        RPC error on specific media types).  Copies each album item
        individually, in album_sequence_index order.

        PARTIAL DELIVERY WARNING: If FloodWait or an RPC error occurs after
        one or more items have already been sent, some items will have been
        delivered to the target while others have not.  The FloodWait
        exception is re-raised so the caller respects the required wait
        period.  On retry the caller should account for potential duplicate
        delivery of already-sent items (accept as a known edge case or
        implement target-side deduplication).

    Args:
        bot:         Authenticated Pyrogram Client.
        sorted_jobs: Job documents sorted by album_sequence_index (ascending).
        target_id:   Telegram channel or group ID to deliver to.

    Raises:
        VaultReferenceMissingError: If any vault reference is unresolvable.
        FloodWait:  Propagated from both primary and fallback paths.
        RPCError:   Propagated if the sequential fallback also fails.
    """
    primary_job = sorted_jobs[0]
    from_chat_id, first_msg_id = _resolve_vault_ref(primary_job)

    # ── Primary: copy_media_group ─────────────────────────────────────────────
    try:
        await bot.copy_media_group(
            chat_id=target_id,
            from_chat_id=from_chat_id,
            message_id=first_msg_id,
        )
        logger.info(
            "album_delivered_copy_media_group",
            extra={
                "ctx_target": target_id,
                "ctx_from_chat": from_chat_id,
                "ctx_first_msg_id": first_msg_id,
                "ctx_item_count": len(sorted_jobs),
            },
        )
        return
    except FloodWait:
        # No partial delivery — safe to propagate and let caller retry whole album.
        raise
    except Exception as exc:
        logger.warning(
            "copy_media_group_failed_falling_back_to_sequential",
            extra={
                "ctx_target": target_id,
                "ctx_from_chat": from_chat_id,
                "ctx_error": str(exc),
                "ctx_item_count": len(sorted_jobs),
            },
        )

    # ── Fallback: sequential copy_message ─────────────────────────────────────
    logger.info(
        "album_sequential_fallback_started",
        extra={"ctx_target": target_id, "ctx_item_count": len(sorted_jobs)},
    )
    for i, job in enumerate(sorted_jobs):
        item_chat_id, item_msg_id = _resolve_vault_ref(job)
        # FloodWait here means partial delivery has already occurred for
        # earlier items in this loop.  Re-raise so the caller can wait
        # the required interval before retrying.  Partial delivery is a
        # known edge case in sequential album fallback — see docstring.
        await bot.copy_message(
            chat_id=target_id,
            from_chat_id=item_chat_id,
            message_id=item_msg_id,
            # Caption only on the first item to avoid repeated captions
            # on a split album.
            caption=job.get("caption") if i == 0 else None,
        )

    logger.info(
        "album_delivered_sequential_fallback",
        extra={"ctx_target": target_id, "ctx_count": len(sorted_jobs)},
    )