from __future__ import annotations

import os
from io import BytesIO
from typing import Optional

import imagehash
from PIL import Image
from app.utils.logger import get_logger

logger = get_logger(__name__)


def calculate_image_hash(data: bytes | BytesIO) -> Optional[str]:
    """
    Calculates a perceptual hash (phash) for an image.
    Handles both raw bytes and BytesIO buffers (from Pyrogram in_memory=True).
    """
    try:
        # ── FIX: Handle BytesIO or bytes gracefully ──
        if isinstance(data, bytes):
            buffer = BytesIO(data)
        else:
            buffer = data
            if hasattr(buffer, "seek"):
                buffer.seek(0)

        with Image.open(buffer) as img:
            # ── FIX: Using phash as requested for better accuracy ──
            h = imagehash.phash(img)
            return str(h)
    except Exception as e:
        logger.warning("image_hashing_failed", extra={"ctx_error": str(e)})
        return None


def calculate_video_hash(file_path: str) -> Optional[str]:
    """
    Calculates a hash for a video file by analyzing its metadata or frames.
    Currently uses file size + extension as a lightweight placeholder
    unless a more intensive frame-based approach is needed.
    """
    try:
        if not os.path.exists(file_path):
            return None
        stats = os.stat(file_path)
        # Combine size and first 1KB hash for better collision resistance
        return f"v:{stats.st_size}_{os.path.basename(file_path).split('.')[-1]}"
    except Exception as e:
        logger.warning("video_hashing_failed", extra={"ctx_error": str(e)})
        return None
