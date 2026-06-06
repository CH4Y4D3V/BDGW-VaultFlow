from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, List

from pyrogram import Client
from pyrogram.types import Message

from app.config import settings
from app.core.models import MediaType
from app.watermark.ffmpeg_processor import FFmpegProcessor
from app.utils.logger import get_logger

logger = get_logger(__name__)

class WatermarkService:
    """
    High-level service for orchestrating the watermark pipeline.
    
    Handles:
      - Downloading media from Telegram.
      - Routing to FFmpegProcessor for photo/video watermarking.
      - Re-uploading watermarked media to Telegram (best-effort for vault enqueuing).
      - Cleanup of temporary files.
    """

    def __init__(self) -> None:
        self.processor = FFmpegProcessor()
        self.temp_dir = Path(getattr(settings, "PROCESSED_MEDIA_DIR", ".")) / "downloads"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    async def process(
        self,
        client: Client,
        messages: List[Message],
        dest: str,
    ) -> Optional[List[Message]]:
        """
        Process one or more messages (e.g. an album) through the watermark pipeline.
        
        Args:
            client:   Pyrogram client.
            messages: List of media messages to watermark.
            dest:     Destination type ('nsfw' or 'premium').

        Returns:
            A list of new Message objects (containing watermarked media) or None on failure.
        """
        if not messages:
            return None

        watermarked_messages: List[Message] = []
        
        for message in messages:
            try:
                # 1. Resolve Media Type
                media_type = self._resolve_media_type(message)
                if not media_type:
                    logger.debug("watermark_service: skipping non-media message", extra={"ctx_msg_id": message.id})
                    watermarked_messages.append(message)
                    continue

                # 2. Download Media
                download_path = await message.download(file_name=str(self.temp_dir / f"dl_{message.id}_{message.media_group_id or 'single'}"))
                if not download_path:
                    logger.warning("watermark_service: download failed", extra={"ctx_msg_id": message.id})
                    watermarked_messages.append(message)
                    continue

                # 3. Configure Processor
                output_path = self.processor._generate_output_path(download_path)
                config = self._build_config(dest, media_type)

                # 4. Process
                final_path = await self.processor.process(download_path, output_path, media_type, config)
                
                # 5. Re-upload (This is a simplified approach for the handler to have the files)
                # In a real pipeline, the handler expects pyrogram Message objects to pass to enqueue_for_distribution.
                # However, re-uploading here creates a NEW message. 
                # For direct vault uploads, we might just want to store the PATH and let enqueue handle it.
                # But to follow the existing vault_handler flow, we need to return something that enqueue can use.
                
                # IMPORTANT: The current enqueue_for_distribution expects pyrogram Message objects.
                # We will re-upload to a hidden buffer chat or the vault itself as a 'temp' if needed.
                # For now, we attach the PATH to the message object as a custom attribute.
                setattr(message, "watermarked_path", final_path)
                watermarked_messages.append(message)

                # Cleanup original download
                if os.path.exists(download_path):
                    os.remove(download_path)

            except Exception as e:
                logger.error("watermark_service: processing failed", extra={"ctx_msg_id": message.id, "ctx_error": str(e)}, exc_info=True)
                watermarked_messages.append(message)

        return watermarked_messages

    def _resolve_media_type(self, message: Message) -> Optional[MediaType]:
        if message.photo: return MediaType.PHOTO
        if message.video: return MediaType.VIDEO
        return None

    def _build_config(self, dest: str, media_type: MediaType) -> dict:
        config = {
            "position": settings.WATERMARK_POSITION,
            "opacity": settings.WATERMARK_OPACITY,
        }
        
        if media_type == MediaType.PHOTO:
            # NSFW vs Premium Logos
            logo_key = "WATERMARK_LOGO_NSFW" if dest == "nsfw" else "WATERMARK_LOGO_PREMIUM"
            config["watermark_image_path"] = getattr(settings, logo_key, None)
        else:
            config["watermark_text"] = settings.WATERMARK_TEXT or "BDGW"
            
        return config

# ── Singleton ───────────────────────────────────────────────────────────────

_watermark_service: Optional[WatermarkService] = None

def get_watermark_service() -> WatermarkService:
    global _watermark_service
    if _watermark_service is None:
        _watermark_service = WatermarkService()
    return _watermark_service
