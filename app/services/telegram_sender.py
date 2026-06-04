"""
TelegramSender — copies source channel content to the target channel.

Watermark pipeline fixes:
  - Default WATERMARK_OPACITY raised from 30 → 90 (35% alpha — clearly
    visible on both light and dark backgrounds without being intrusive).
    30/255 ≈ 12% opacity which is effectively invisible on most photos.
  - Every step of the watermark pipeline now emits a log line so Railway
    logs show exactly where the process succeeds or falls back.
  - Silent fallback to copy_message is replaced with an explicit WARNING
    so the user can immediately see in Railway logs when watermark is
    bypassed and why.
  - download_media return-type handling made more robust: str path (some
    Pyrogram builds return a path even with in_memory=True) is now read
    from disk and treated as bytes.
"""

import asyncio
import io
import math
import os
import tempfile
from typing import List, Optional, Tuple

from pyrogram import Client, enums
from pyrogram.errors import FloodWait, MessageIdInvalid, ChannelInvalid, PeerIdInvalid
from pyrogram.types import Message
from loguru import logger

from app.utils.config import settings
from app.database.models import QueueItem, SentLog
from app.database.repositories import sent_log_repo

try:
    from PIL import Image, ImageDraw, ImageFont, ImageOps
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
    logger.warning(
        "Pillow not installed — watermarking disabled. "
        "Add 'Pillow==10.3.0' to requirements.txt and rebuild."
    )

_CAPTION_MAX_LEN = 1024

_WATERMARK_TEXT: str = settings.WATERMARK or ""

# Read watermark settings — fall back to defaults when env vars are absent.
# WATERMARK_OPACITY: 0–255 alpha value.
#   30  (old default) = 12% opacity  — nearly invisible
#   90  (new default) = 35% opacity  — clearly visible, not intrusive
#   128                = 50% opacity  — bold
#   200                = 78% opacity  — very strong
_WATERMARK_OPACITY: int = int(getattr(settings, "WATERMARK_OPACITY", 90))
_WATERMARK_COUNT: int = int(getattr(settings, "WATERMARK_COUNT", 4))
_WATERMARK_ROTATION: int = int(getattr(settings, "WATERMARK_ROTATION", -35))
_WATERMARK_FONT_SCALE: float = float(getattr(settings, "WATERMARK_FONT_SCALE", 0.04))

_WATERMARK_ENABLED: bool = _PIL_AVAILABLE and bool(_WATERMARK_TEXT)

# ── Startup diagnostic — always visible in Railway logs ──────────────────────
if _WATERMARK_ENABLED:
    logger.info(
        f"Watermark ENABLED | "
        f"text='{_WATERMARK_TEXT}' | "
        f"opacity={_WATERMARK_OPACITY}/255 ({_WATERMARK_OPACITY/255*100:.0f}%) | "
        f"count={_WATERMARK_COUNT} | "
        f"rotation={_WATERMARK_ROTATION}° | "
        f"font_scale={_WATERMARK_FONT_SCALE}"
    )
elif not _PIL_AVAILABLE:
    logger.warning(
        "Watermark DISABLED — Pillow is not installed. "
        "Add 'Pillow==10.3.0' to requirements.txt and rebuild the Docker image."
    )
elif not _WATERMARK_TEXT:
    logger.warning(
        "Watermark DISABLED — WATERMARK environment variable is empty or not set. "
        "Set WATERMARK=@yourchannel in Railway → Variables, then redeploy."
    )


# ---------------------------------------------------------------------------
# Watermark core  (CPU-bound — run via executor, never call directly in async)
# ---------------------------------------------------------------------------

def _load_font(font_size: int):
    """Load the best available bold font at the given size."""
    font_candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    for path in font_candidates:
        if os.path.exists(path):
            try:
                font = ImageFont.truetype(path, font_size)
                logger.debug(f"Watermark font loaded: {path} @ {font_size}px")
                return font
            except Exception as exc:
                logger.debug(f"Font load failed ({path}): {exc}")
    logger.warning(
        "No truetype font found — falling back to Pillow built-in bitmap font. "
        "Install fonts-dejavu-core in the Dockerfile for better results."
    )
    return ImageFont.load_default()


def _make_text_stamp(
    text: str, font, opacity: int, rotation: int
) -> "Image.Image":
    scratch = Image.new("RGBA", (1, 1))
    draw = ImageDraw.Draw(scratch)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0] + 40
    th = bbox[3] - bbox[1] + 40

    txt_img = Image.new("RGBA", (tw, th), (0, 0, 0, 0))
    d = ImageDraw.Draw(txt_img)
    # Black stroke makes white text visible on ANY background
    d.text(
        (20, 20), text, font=font,
        fill=(255, 255, 255, opacity),
        stroke_width=3,
        stroke_fill=(0, 0, 0, opacity),
    )
    return txt_img.rotate(rotation, expand=True, resample=Image.BICUBIC)


def _apply_watermark_sync(image_bytes: bytes) -> bytes:
    """
    Apply the watermark to image_bytes and return the result as JPEG bytes.

    This is CPU-bound and MUST be called via run_in_executor — never directly
    from async code.

    Returns the original image_bytes unchanged if:
      - Watermarking is disabled (_WATERMARK_ENABLED is False)
      - Any exception occurs during processing (with a WARNING log)
    """
    if not _WATERMARK_ENABLED:
        return image_bytes

    try:
        img = Image.open(io.BytesIO(image_bytes))
        # Honour EXIF orientation so rotated phone photos come out right
        try:
            img = ImageOps.exif_transpose(img)
        except Exception:
            pass

        img = img.convert("RGBA")
        w, h = img.size

        font_size = max(16, int(min(w, h) * _WATERMARK_FONT_SCALE))
        logger.debug(
            f"Watermark: image {w}×{h}, font_size={font_size}px, "
            f"opacity={_WATERMARK_OPACITY}"
        )

        font = _load_font(font_size)
        stamp = _make_text_stamp(
            _WATERMARK_TEXT, font, _WATERMARK_OPACITY, _WATERMARK_ROTATION
        )
        sw, sh = stamp.size

        overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        count = max(1, _WATERMARK_COUNT)
        cols = math.ceil(math.sqrt(count))
        rows = math.ceil(count / cols)
        positions: List[Tuple[int, int]] = []

        for row in range(rows):
            for col in range(cols):
                if len(positions) >= count:
                    break
                x = int((col + 0.5) * w / cols) - sw // 2
                y = int((row + 0.5) * h / rows) - sh // 2
                positions.append((x, y))

        for x, y in positions:
            overlay.paste(stamp, (x, y), stamp)

        watermarked = Image.alpha_composite(img, overlay).convert("RGB")
        out = io.BytesIO()
        watermarked.save(out, format="JPEG", quality=92)
        result = out.getvalue()
        logger.debug(
            f"Watermark applied: {len(image_bytes)} bytes → {len(result)} bytes"
        )
        return result

    except Exception as exc:
        logger.warning(
            f"Watermark processing failed — sending original image: {exc}",
            exc_info=True,
        )
        return image_bytes


async def _apply_watermark(image_bytes: bytes) -> bytes:
    """Async wrapper — offloads the CPU-bound PIL work to the default executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _apply_watermark_sync, image_bytes)


# ---------------------------------------------------------------------------
# Temp-file helpers
# ---------------------------------------------------------------------------

def _bytes_to_tempfile(data: bytes, suffix: str = ".jpg") -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
    except Exception:
        os.close(fd)
        raise
    return path


def _cleanup(*paths: str) -> None:
    for p in paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception as exc:
            logger.debug(f"Failed to remove temp file {p}: {exc}")


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------

class TelegramSender:
    def __init__(self, client: Client) -> None:
        self.client = client

    # ------------------------------------------------------------------
    # Caption helpers
    # ------------------------------------------------------------------

    def _build_caption(self, original_html: Optional[str]) -> str:
        parts: List[str] = []
        if original_html:
            parts.append(original_html)
        if settings.FIXED_CAPTION:
            if parts:
                parts.append("\n" + "—" * 10 + "\n")
            parts.append(settings.FIXED_CAPTION)
        caption = "".join(parts)
        if len(caption) > _CAPTION_MAX_LEN:
            logger.warning(
                f"Caption truncated: {len(caption)} → {_CAPTION_MAX_LEN} chars"
            )
            caption = caption[:_CAPTION_MAX_LEN]
        return caption

    # ------------------------------------------------------------------
    # Watermark download helper
    # ------------------------------------------------------------------

    async def _download_and_watermark(self, message: Message) -> Optional[str]:
        """
        Download the photo from `message`, apply the watermark, and save it
        to a temporary file.  Returns the file path on success, or None if
        any step fails (with an appropriate log at WARNING level).

        The caller must delete the returned file with _cleanup() when done.
        """
        if not message.photo:
            return None
        if not _WATERMARK_ENABLED:
            return None

        logger.debug(f"Downloading photo for watermark: message {message.id}")

        try:
            raw = await self.client.download_media(message, in_memory=True)

            if raw is None:
                logger.warning(
                    f"Watermark SKIPPED (message {message.id}): "
                    "download_media returned None. "
                    "Possible cause: bot cannot access this file. "
                    "Falling back to copy_message (no watermark)."
                )
                return None

            # Normalise to bytes — handle all return types Pyrogram may give.
            if isinstance(raw, io.BytesIO):
                raw_bytes = raw.getvalue()
                logger.debug(
                    f"download_media returned BytesIO: {len(raw_bytes)} bytes"
                )
            elif isinstance(raw, bytes):
                raw_bytes = raw
                logger.debug(
                    f"download_media returned bytes: {len(raw_bytes)} bytes"
                )
            elif isinstance(raw, str):
                # Some Pyrogram builds return a temp file path even with
                # in_memory=True.  Read the file and delete it.
                logger.debug(
                    f"download_media returned file path: {raw}. Reading from disk."
                )
                try:
                    with open(raw, "rb") as fh:
                        raw_bytes = fh.read()
                except Exception as read_exc:
                    logger.warning(
                        f"Watermark SKIPPED (message {message.id}): "
                        f"could not read downloaded file {raw}: {read_exc}. "
                        "Falling back to copy_message (no watermark)."
                    )
                    return None
                finally:
                    _cleanup(raw)
            else:
                logger.warning(
                    f"Watermark SKIPPED (message {message.id}): "
                    f"download_media returned unexpected type {type(raw).__name__}. "
                    "Falling back to copy_message (no watermark)."
                )
                return None

            if not raw_bytes:
                logger.warning(
                    f"Watermark SKIPPED (message {message.id}): "
                    "downloaded file is empty. "
                    "Falling back to copy_message (no watermark)."
                )
                return None

            processed = await _apply_watermark(raw_bytes)
            return _bytes_to_tempfile(processed, suffix=".jpg")

        except Exception as exc:
            logger.warning(
                f"Watermark SKIPPED (message {message.id}): "
                f"unexpected error in download+watermark pipeline: {exc}. "
                "Falling back to copy_message (no watermark).",
                exc_info=True,
            )
            return None

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    async def send_item(self, item: QueueItem) -> bool:
        """
        Send one queue item to the target channel.

        Returns True on success, False on permanent per-message failure.
        Raises FloodWait / PeerIdInvalid so the posting_worker can handle
        backoff and re-queue correctly.
        """
        try:
            if item.media_group_id and item.message_ids:
                logger.info(
                    f"Sending media group {item.media_group_id} "
                    f"({len(item.message_ids)} items)…"
                )
                sent = await self._send_media_group(item)
            else:
                logger.info(f"Sending single message {item.message_id}…")
                sent = await self._send_single(item)

            if sent:
                await sent_log_repo.create_log(
                    SentLog(
                        source_message_id=item.message_id,
                        target_chat_id=settings.TARGET_CHAT_ID,
                        target_message_ids=[m.id for m in sent],
                        status="success",
                    )
                )
                logger.success(f"Sent source message {item.message_id}.")
                return True

            logger.warning(
                f"send_item: Telegram returned no messages for {item.message_id}."
            )
            return False

        except FloodWait:
            raise

        except PeerIdInvalid as exc:
            # Recoverable — retry after peer resolution.
            logger.warning(
                f"PeerIdInvalid for message {item.message_id}: {exc}. "
                "Re-raising for re-queue."
            )
            raise

        except (MessageIdInvalid, ChannelInvalid) as exc:
            # Permanent per-message failure.
            logger.warning(
                f"Permanent Telegram error for message {item.message_id}: {exc}. "
                "Marking as failed."
            )
            return False

        except Exception as exc:
            err_str = str(exc).lower()
            if "peer id invalid" in err_str:
                logger.warning(
                    f"PeerIdInvalid (via Exception) for message "
                    f"{item.message_id}: {exc}. Re-raising for re-queue."
                )
                raise PeerIdInvalid(exc.x if hasattr(exc, "x") else str(exc))

            logger.error(
                f"Unexpected error sending message {item.message_id}: {exc}",
                exc_info=True,
            )
            return False

    # ------------------------------------------------------------------
    # Single message
    # ------------------------------------------------------------------

    async def _send_single(self, item: QueueItem) -> List[Message]:
        original = await self.client.get_messages(
            settings.SOURCE_CHANNEL_ID, item.message_id
        )
        if original is None or original.empty:
            raise MessageIdInvalid(
                f"Message {item.message_id} is deleted or unavailable."
            )

        caption = self._build_caption(
            original.caption.html if original.caption else None
        )

        # ── Watermark path (photos only) ──────────────────────────────────────
        if original.photo and _WATERMARK_ENABLED:
            logger.info(f"Attempting watermarked send for message {item.message_id}…")
            temp_path = await self._download_and_watermark(original)

            if temp_path:
                try:
                    sent = await self.client.send_photo(
                        chat_id=settings.TARGET_CHAT_ID,
                        photo=temp_path,
                        caption=caption,
                        parse_mode=enums.ParseMode.HTML,
                    )
                    logger.debug(
                        f"Watermarked send_photo succeeded for message {item.message_id}."
                    )
                    return [sent]

                except (PeerIdInvalid, ChannelInvalid):
                    # Propagate — outer handler will re-queue.
                    raise

                except Exception as exc:
                    logger.warning(
                        f"WATERMARK SKIPPED (message {item.message_id}): "
                        f"send_photo failed: {exc}. "
                        "Falling back to copy_message (no watermark).",
                        exc_info=True,
                    )
                finally:
                    _cleanup(temp_path)

            else:
                # _download_and_watermark already logged the specific reason.
                logger.warning(
                    f"WATERMARK SKIPPED (message {item.message_id}): "
                    "download+watermark returned None. "
                    "Falling back to copy_message (no watermark)."
                )

        elif original.photo and not _WATERMARK_ENABLED:
            logger.debug(
                f"Watermark disabled — sending message {item.message_id} "
                "via copy_message."
            )

        # ── Fallback / non-photo path ─────────────────────────────────────────
        sent = await self.client.copy_message(
            chat_id=settings.TARGET_CHAT_ID,
            from_chat_id=settings.SOURCE_CHANNEL_ID,
            message_id=item.message_id,
            caption=caption,
        )
        return [sent]

    # ------------------------------------------------------------------
    # Media group
    # ------------------------------------------------------------------

    async def _send_media_group(self, item: QueueItem) -> List[Message]:
        from pyrogram.types import (
            InputMediaPhoto,
            InputMediaVideo,
            InputMediaDocument,
            InputMediaAudio,
        )

        raw_messages = await self.client.get_messages(
            settings.SOURCE_CHANNEL_ID, item.message_ids
        )
        if not isinstance(raw_messages, list):
            raw_messages = [raw_messages]

        valid: List[Message] = sorted(
            [m for m in raw_messages if m and not m.empty],
            key=lambda m: m.id,
        )

        if not valid:
            raise MessageIdInvalid(
                f"All messages in media group {item.media_group_id} are deleted."
            )

        if len(valid) < len(item.message_ids):
            logger.warning(
                f"Media group {item.media_group_id}: expected "
                f"{len(item.message_ids)} messages, got {len(valid)}. "
                "Sending partial album."
            )

        original_html: Optional[str] = next(
            (m.caption.html for m in valid if m.caption), None
        )
        caption = self._build_caption(original_html)

        # ── Watermarked send path ─────────────────────────────────────────────
        if _WATERMARK_ENABLED:
            logger.debug(
                f"Attempting watermarked media group send for "
                f"{item.media_group_id} ({len(valid)} items)…"
            )
            temp_paths: List[Optional[str]] = []
            media_inputs = []
            first_caption_assigned = False

            try:
                for msg in valid:
                    item_caption = caption if not first_caption_assigned else ""

                    if msg.photo:
                        temp_path = await self._download_and_watermark(msg)
                        temp_paths.append(temp_path)
                        if temp_path:
                            media_inputs.append(
                                InputMediaPhoto(
                                    media=temp_path,
                                    caption=item_caption,
                                    parse_mode=enums.ParseMode.HTML,
                                )
                            )
                            first_caption_assigned = True
                        else:
                            raise RuntimeError(
                                f"Watermark download failed for photo in msg {msg.id} "
                                f"(group {item.media_group_id})"
                            )

                    elif msg.video:
                        temp_paths.append(None)
                        media_inputs.append(
                            InputMediaVideo(
                                media=msg.video.file_id,
                                caption=item_caption,
                                parse_mode=enums.ParseMode.HTML,
                            )
                        )
                        first_caption_assigned = True

                    elif msg.document:
                        temp_paths.append(None)
                        media_inputs.append(
                            InputMediaDocument(
                                media=msg.document.file_id,
                                caption=item_caption,
                                parse_mode=enums.ParseMode.HTML,
                            )
                        )
                        first_caption_assigned = True

                    elif msg.audio:
                        temp_paths.append(None)
                        media_inputs.append(
                            InputMediaAudio(
                                media=msg.audio.file_id,
                                caption=item_caption,
                                parse_mode=enums.ParseMode.HTML,
                            )
                        )
                        first_caption_assigned = True

                    else:
                        temp_paths.append(None)
                        logger.warning(
                            f"Skipping message {msg.id} in group "
                            f"{item.media_group_id}: unsupported media type."
                        )

                if media_inputs:
                    sent = await self.client.send_media_group(
                        chat_id=settings.TARGET_CHAT_ID,
                        media=media_inputs,
                    )
                    logger.debug(
                        f"Watermarked send_media_group succeeded for "
                        f"{item.media_group_id}."
                    )
                    return sent

            except (PeerIdInvalid, ChannelInvalid):
                raise

            except Exception as exc:
                logger.warning(
                    f"WATERMARK SKIPPED (group {item.media_group_id}): "
                    f"watermarked media group send failed: {exc}. "
                    "Falling back to copy_media_group (no watermark).",
                    exc_info=True,
                )
            finally:
                _cleanup(*[p for p in temp_paths if p])

        # ── Fallback: copy_media_group (no watermark) ─────────────────────────
        captions: List[str] = [caption] + [""] * (len(valid) - 1)
        sent = await self.client.copy_media_group(
            chat_id=settings.TARGET_CHAT_ID,
            from_chat_id=settings.SOURCE_CHANNEL_ID,
            message_id=valid[0].id,
            captions=captions,
        )
        return sent
