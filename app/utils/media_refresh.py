"""
app/utils/media_refresh.py

Centralised media reference refresh utility.

Telegram file references (the file_reference bytes embedded in every file_id
string) expire within hours to days.  Any worker that stores a file_id at
approval time and re-uses it later will hit 400 FILE_REFERENCE_EXPIRED.

The fix: NEVER download from a raw stored file_id.  Always call
get_messages() first to obtain a live Message object, then pass that object
to download_media().  This forces Telegram to return a fresh file_reference.

Refresh priority order
──────────────────────
1. Job doc fields     (vault_channel_id + vault_message_id stored at enqueue time — zero DB round trip)
2. Vault DB lookup    (fallback when job doc fields are absent — legacy jobs)
3. Origin chat copy   (fallback — may be deleted by user or Telegram)
4. FileId direct      (last resort — will fail on old jobs but beats nothing)

Changes from previous version
──────────────────────────────
FIX 1 — settings.FLOODWAIT_EXTRA_BUFFER was read at module import time.
         Pyrogram plugin loader imports all handlers before the app is fully
         initialised.  Any module-level settings access can crash the loader
         silently.  _FLOOD_BUFFER is now a lazy property resolved at call time.

FIX 2 — resolve_fresh_message() ignored vault_channel_id / vault_message_id
         already present in job_doc, always doing a DB lookup even when the
         caller had the coordinates.  The DB lookup is now a fallback only.

DESIGN — DatabaseManager.get_db() is no longer called inside this utility.
         Callers that need the DB fallback path must pass the db instance.
         This removes the hidden coupling and makes the module testable.

All public helpers are async-safe, FloodWait-aware, and never raise —
they return None on unrecoverable failure so callers can dead-letter the job.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pyrogram.client import Client
from pyrogram.errors import (
    ChannelInvalid,
    FileReferenceExpired,
    FloodWait,
    MessageIdInvalid,
    PeerIdInvalid,
    RPCError,
)
from pyrogram.types import Message

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_MAX_RETRIES = 3


def _flood_buffer() -> int:
    """
    FIX 1: Lazy getter instead of module-level constant.
    Resolves settings at call time, never at import time.
    Safe for Pyrogram plugin loader.
    """
    return getattr(settings, "FLOODWAIT_EXTRA_BUFFER", 2)


# ── Low-level message fetch ───────────────────────────────────────────────────

async def fetch_message_safe(
    client: Client,
    chat_id: int,
    message_id: int,
    context: str = "",
) -> Optional[Message]:
    """
    Fetch a single Telegram message, returning None on any non-retryable error.
    Retries on FloodWait up to _MAX_RETRIES times.
    Never raises.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            messages = await client.get_messages(
                chat_id=chat_id,
                message_ids=message_id,
            )
            if not isinstance(messages, list):
                messages = [messages]

            msg = next(
                (m for m in messages if m and m.id and m.media),
                None,
            )
            if msg is None:
                logger.warning(
                    "fetch_message_safe: message has no media or was deleted",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_message_id": message_id,
                        "ctx_context": context,
                    },
                )
            return msg

        except FloodWait as e:
            wait = int(e.value) + _flood_buffer()
            logger.warning(
                "fetch_message_safe: FloodWait",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_message_id": message_id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                    "ctx_context": context,
                },
            )
            await asyncio.sleep(wait)

        except (MessageIdInvalid, ChannelInvalid, PeerIdInvalid) as e:
            logger.warning(
                "fetch_message_safe: message or chat not found",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_message_id": message_id,
                    "ctx_error": str(e),
                    "ctx_context": context,
                },
            )
            return None

        except RPCError as e:
            logger.warning(
                "fetch_message_safe: RPCError",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_message_id": message_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                    "ctx_context": context,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)

        except asyncio.CancelledError:
            raise

        except Exception as e:
            logger.error(
                "fetch_message_safe: unexpected error",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_message_id": message_id,
                    "ctx_error": str(e),
                    "ctx_context": context,
                },
                exc_info=True,
            )
            return None

    return None


# ── Media object extraction ───────────────────────────────────────────────────

def extract_media_object(message: Message):
    """
    Return the media object from a Message, trying all supported types.
    Returns None if the message has no recognised media.
    """
    if message is None:
        return None
    for attr in ("video", "document", "animation", "photo", "audio", "voice", "video_note"):
        obj = getattr(message, attr, None)
        if obj is not None:
            return obj
    return None


# ── Vault-first fresh reference resolution ────────────────────────────────────

async def resolve_fresh_message(
    client: Client,
    job_doc: dict,
    job_id: str = "",
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Message]:
    """
    Resolve a live Telegram Message object using the freshest available reference.

    FIX 2: Priority order is now:
      1. vault_channel_id + vault_message_id already in job_doc  (zero DB cost)
      2. DB vault lookup via content_id                           (legacy fallback)
      3. origin_chat_id + origin_message_id from job_doc metadata

    The db argument is required only if job_doc does not contain vault coordinates
    and a DB lookup fallback is needed.  Pass None to skip the DB path.
    """
    # ── Path 1: coordinates already on the job doc ────────────────────────────
    job_vault_channel_id = _int_or_none(job_doc.get("vault_channel_id"))
    job_vault_message_id = _int_or_none(job_doc.get("vault_message_id"))

    if job_vault_channel_id and job_vault_message_id:
        msg = await fetch_message_safe(
            client,
            chat_id=job_vault_channel_id,
            message_id=job_vault_message_id,
            context=f"job_doc_vault:job={job_id}",
        )
        if msg is not None:
            return msg
        logger.warning(
            "resolve_fresh_message: job_doc vault fetch failed, trying DB fallback",
            extra={"ctx_job_id": job_id},
        )

    # ── Path 2: DB vault lookup (legacy — when job doc lacks vault coords) ────
    if db is not None:
        metadata = job_doc.get("metadata", {})
        source_chat_id = metadata.get("source_chat_id")
        source_message_id = metadata.get("source_message_id")

        if source_chat_id and source_message_id:
            vault_content_id = f"{source_chat_id}_{source_message_id}"
            try:
                vault_doc = await db[settings.VAULT_COLLECTION].find_one(
                    {"content_id": vault_content_id}
                )
            except Exception as e:
                logger.error(
                    "resolve_fresh_message: DB vault lookup failed",
                    extra={"ctx_job_id": job_id, "ctx_error": str(e)},
                )
                vault_doc = None

            if vault_doc:
                db_vault_channel_id = _int_or_none(
                    vault_doc.get("vault_channel_id") or str(settings.VAULT_CHANNEL_ID)
                )
                db_vault_message_id = _int_or_none(vault_doc.get("vault_message_id"))

                if db_vault_channel_id and db_vault_message_id:
                    msg = await fetch_message_safe(
                        client,
                        chat_id=db_vault_channel_id,
                        message_id=db_vault_message_id,
                        context=f"db_vault_refresh:job={job_id}",
                    )
                    if msg is not None:
                        return msg
            else:
                logger.warning(
                    "resolve_fresh_message: no vault doc found in DB",
                    extra={"ctx_job_id": job_id, "ctx_content_id": vault_content_id},
                )
        else:
            logger.warning(
                "resolve_fresh_message: DB path requested but job metadata missing source coords",
                extra={"ctx_job_id": job_id},
            )

    # ── Path 3: origin chat fallback ──────────────────────────────────────────
    metadata = job_doc.get("metadata", {})
    origin_chat_id = _int_or_none(metadata.get("source_chat_id"))
    origin_message_id = _int_or_none(metadata.get("source_message_id"))

    if origin_chat_id and origin_message_id:
        msg = await fetch_message_safe(
            client,
            chat_id=origin_chat_id,
            message_id=origin_message_id,
            context=f"origin_fallback:job={job_id}",
        )
        if msg is not None:
            return msg

    logger.error(
        "resolve_fresh_message: all resolution paths exhausted",
        extra={"ctx_job_id": job_id},
    )
    return None


# ── Download with fresh reference ─────────────────────────────────────────────

async def download_with_refresh(
    client: Client,
    job_doc: dict,
    dest_dir: str,
    job_id: str = "",
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[str]:
    """
    Download media for a queue job using a freshly resolved file reference.

    Reads origin/vault coordinates from job_doc and falls through the refresh
    priority chain.  Returns the local file path on success, None on failure.

    job_doc keys used:
        vault_channel_id   (str or int)  — preferred, zero DB cost
        vault_message_id   (int)         — preferred, zero DB cost
        metadata.source_chat_id          — used for DB lookup and origin fallback
        metadata.source_message_id       — used for DB lookup and origin fallback
        media_path         (str, optional) — local path if already on disk
        media_file_id      (str, optional) — legacy raw file_id, last resort

    db: pass your AsyncIOMotorDatabase instance to enable the DB vault lookup
        fallback for legacy jobs that lack vault_channel_id/vault_message_id
        in the job doc itself.  Pass None to skip (saves a round trip when
        job doc coordinates are always populated at enqueue time).
    """
    # ── Fast path: local file already on disk ─────────────────────────────────
    media_path = job_doc.get("media_path")
    if media_path and Path(media_path).exists():
        logger.debug(
            "download_with_refresh: local file already present",
            extra={"ctx_job_id": job_id, "ctx_path": media_path},
        )
        return media_path

    if media_path:
        logger.warning(
            "download_with_refresh: media_path recorded but file missing on disk",
            extra={"ctx_job_id": job_id, "ctx_path": media_path},
        )

    # ── Resolve fresh message ─────────────────────────────────────────────────
    msg = await resolve_fresh_message(
        client=client,
        job_doc=job_doc,
        job_id=job_id,
        db=db,
    )

    if msg is not None:
        return await _download_message(client, msg, dest_dir, job_id)

    # ── Last resort: raw file_id (will fail for old jobs) ─────────────────────
    legacy_file_id = job_doc.get("media_file_id")
    if legacy_file_id:
        logger.warning(
            "download_with_refresh: falling back to raw file_id "
            "(may hit FILE_REFERENCE_EXPIRED)",
            extra={
                "ctx_job_id": job_id,
                "ctx_file_id_prefix": legacy_file_id[:20],
            },
        )
        try:
            dest = _make_dest_path(dest_dir, job_id)
            downloaded = await client.download_media(
                message=legacy_file_id,
                file_name=dest,
            )
            if downloaded and Path(str(downloaded)).exists():
                return str(downloaded)
        except FileReferenceExpired:
            logger.error(
                "download_with_refresh: FILE_REFERENCE_EXPIRED on legacy file_id — "
                "vault_message_id and vault_channel_id must be stored at enqueue time",
                extra={"ctx_job_id": job_id},
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(
                "download_with_refresh: legacy file_id download failed",
                extra={"ctx_job_id": job_id, "ctx_error": str(e)},
            )

    return None


async def _download_message(
    client: Client,
    msg: Message,
    dest_dir: str,
    job_id: str,
) -> Optional[str]:
    """Download the media from a live Message object. Returns local path or None."""
    for attempt in range(_MAX_RETRIES):
        try:
            dest = _make_dest_path(dest_dir, job_id)
            downloaded = await client.download_media(
                message=msg,
                file_name=dest,
            )
            if downloaded and Path(str(downloaded)).exists():
                logger.info(
                    "_download_message: downloaded successfully",
                    extra={"ctx_job_id": job_id, "ctx_path": str(downloaded)},
                )
                return str(downloaded)
            logger.error(
                "_download_message: download_media returned empty or missing path",
                extra={"ctx_job_id": job_id, "ctx_result": str(downloaded)},
            )
            return None

        except FloodWait as e:
            wait = int(e.value) + _flood_buffer()
            logger.warning(
                "_download_message: FloodWait",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt + 1,
                },
            )
            await asyncio.sleep(wait)

        except FileReferenceExpired:
            logger.error(
                "_download_message: FILE_REFERENCE_EXPIRED even on fresh message — "
                "Telegram caching anomaly",
                extra={"ctx_job_id": job_id, "ctx_attempt": attempt + 1},
            )
            return None

        except RPCError as e:
            logger.warning(
                "_download_message: RPCError",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)

        except asyncio.CancelledError:
            raise

        except Exception as e:
            logger.error(
                "_download_message: unexpected error",
                extra={
                    "ctx_job_id": job_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
                exc_info=True,
            )
            if attempt == _MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)

    return None


# ── Direct send helpers (Telegram-to-Telegram — no download needed) ───────────

async def resolve_send_media(
    client: Client,
    job_doc: dict,
    job_id: str = "",
    db: Optional[AsyncIOMotorDatabase] = None,
) -> Optional[Message]:
    """
    For direct Telegram-to-Telegram delivery (no local file needed),
    resolve a fresh Message whose file_id can be used in send_photo / send_video.

    Pyrogram's send_* methods accept a live file_id from a fresh Message without
    hitting FILE_REFERENCE_EXPIRED because the client re-fetches the reference
    internally when given the Message object's media attribute.

    Returns fresh Message on success, None if all sources exhausted.
    """
    return await resolve_fresh_message(
        client=client,
        job_doc=job_doc,
        job_id=job_id,
        db=db,
    )


# ── Utilities ─────────────────────────────────────────────────────────────────

def _int_or_none(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _make_dest_path(dest_dir: str, job_id: str) -> str:
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    unique = uuid.uuid4().hex[:12]
    return str(Path(dest_dir) / f"dl_{job_id}_{unique}")