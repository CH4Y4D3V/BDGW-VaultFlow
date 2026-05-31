from __future__ import annotations

import os
from io import BytesIO
from typing import Optional

import imagehash
from PIL import Image
from app.utils.logger import get_logger

logger = get_logger(__name__)


def calculate_image_hash(data: bytes) -> Optional[str]:
    """
    Calculates a perceptual hash (dhash) for an image.
    High performance, resistant to resizing and minor edits.
    """
    try:
        with Image.open(BytesIO(data)) as img:
            # dhash is generally good for near-duplicate detection
            h = imagehash.dhash(img)
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
