import asyncio
import hashlib
import shutil
import uuid
from pathlib import Path
from typing import Optional
from app.config import settings
from app.core.models import WatermarkPosition, MediaType
from app.core.exceptions import (
    FFmpegError,
    FFmpegTimeoutError,
    WatermarkAssetError,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


class WatermarkCache:
    """In-memory cache for processed watermark assets or common frames."""
    def __init__(self):
        self._cache = {}

    def get(self, key: str) -> Optional[str]:
        return self._cache.get(key)

    def set(self, key: str, path: str):
        self._cache[key] = path


class FFmpegProcessor:
    """
    Handles FFmpeg-based media processing for watermarking.
    F-08 Dual System:
    - Photos: random corner PNG logo (NSFW or Premium)
    - Videos: random position dynamic text
    """

    def __init__(self):
        self._cache = WatermarkCache()
        self._temp_dir = Path(settings.MEDIA_TEMP_DIR)
        self._output_dir = Path(settings.WATERMARK_OUTPUT_DIR)
        
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    async def process(
        self,
        input_path: str,
        output_path: str,
        media_type: MediaType,
        config: dict,
    ) -> str:
        """
        Main entry point for watermarking a single file.
        F-08 Dual System Implementation.
        """
        if media_type == MediaType.PHOTO:
            return await self._process_photo(input_path, output_path, config)
        elif media_type == MediaType.VIDEO:
            return await self._process_video(input_path, output_path, config)
        else:
            # Fallback for non-visual types
            shutil.copy(input_path, output_path)
            return output_path

    async def _process_photo(self, input_path: str, output_path: str, config: dict) -> str:
        import random
        from PIL import Image

        logo_path = config.get("watermark_image_path")
        if not logo_path or not Path(logo_path).exists():
            shutil.copy(input_path, output_path)
            return output_path

        try:
            # Wrap in run_in_executor if CPU usage becomes an issue
            with Image.open(input_path) as base, Image.open(logo_path) as logo:
                base = base.convert("RGBA")
                logo = logo.convert("RGBA")
                
                # Random scale 10-20% of base width
                scale = random.uniform(0.1, 0.2)
                logo_w = int(base.width * scale)
                logo_h = int(logo.height * (logo_w / logo.width))
                logo = logo.resize((logo_w, logo_h), Image.Resampling.LANCZOS)
                
                # Random corner
                margin = 20
                positions = [
                    (margin, margin), # Top Left
                    (base.width - logo_w - margin, margin), # Top Right
                    (margin, base.height - logo_h - margin), # Bottom Left
                    (base.width - logo_w - margin, base.height - logo_h - margin) # Bottom Right
                ]
                pos = random.choice(positions)
                
                # Apply logo
                base.alpha_composite(logo, dest=pos)
                
                # Save as high quality JPEG
                base.convert("RGB").save(output_path, "JPEG", quality=95)
                
            return output_path
        except Exception as e:
            logger.error("Photo watermarking failed", extra={"ctx_error": str(e)})
            shutil.copy(input_path, output_path)
            return output_path

    async def _process_video(self, input_path: str, output_path: str, config: dict) -> str:
        import random
        text = config.get("watermark_text", "BDGW")
        
        # F-08: Random position text watermark
        # Choose random percentage for x and y
        x_pct = random.randint(5, 85)
        y_pct = random.randint(5, 85)
        
        # Build drawtext filter
        # fontsize is 5% of height
        drawtext = (
            f"drawtext=text='{text}':"
            f"x=(w-text_w)*{x_pct}/100:y=(h-text_h)*{y_pct}/100:"
            f"fontsize=h/20:fontcolor=white@0.5:shadowcolor=black:shadowx=2:shadowy=2"
        )

        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-vf", drawtext,
            "-c:a", "copy",
            "-preset", "ultrafast",
            output_path
        ]

        logger.debug("Executing FFmpeg for video watermark", extra={"ctx_cmd": " ".join(cmd)})

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            
            # 5 minute timeout for video processing
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)

            if process.returncode != 0:
                error_msg = stderr.decode()
                logger.error("Video watermarking failed", extra={"ctx_stderr": error_msg})
                shutil.copy(input_path, output_path)
                return output_path

            return output_path
        except asyncio.TimeoutError:
            logger.error("FFmpeg process timed out")
            shutil.copy(input_path, output_path)
            return output_path
        except Exception as e:
            logger.error("FFmpeg execution error", extra={"ctx_error": str(e)})
            shutil.copy(input_path, output_path)
            return output_path

    def _generate_output_path(self, input_path: str) -> str:
        ext = Path(input_path).suffix.lower().lstrip(".") or "jpg"
        stem = Path(input_path).stem
        unique = uuid.uuid4().hex[:8]
        filename = f"{stem}_wm_{unique}.{ext}"
        return str(self._output_dir / filename)

    @property
    def cache(self) -> WatermarkCache:
        return self._cache
