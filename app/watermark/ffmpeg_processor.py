import asyncio
import hashlib
import random
import shutil
import uuid
from pathlib import Path
from typing import Optional
from app.config import settings
from app.core.models import WatermarkPosition, MediaType
from app.utils.logger import get_logger

logger = get_logger(__name__)

MEDIA_TEMP_DIR = Path(getattr(settings, "PROCESSED_MEDIA_DIR", ".")) / "tmp"
WATERMARK_OUTPUT_DIR = Path(getattr(settings, "PROCESSED_MEDIA_DIR", ".")) / "watermarked"

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
        self._temp_dir = MEDIA_TEMP_DIR
        self._output_dir = WATERMARK_OUTPUT_DIR
        
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
                
                # ── SYSTEM 17: FIXED SCALE 15% ──
                scale = settings.WATERMARK_SCALE # 0.15
                logo_w = int(base.width * scale)
                logo_h = int(logo.height * (logo_w / logo.width))
                logo = logo.resize((logo_w, logo_h), Image.Resampling.LANCZOS)
                
                # ── ROTATION ──
                rotation = config.get("rotation", settings.WATERMARK_ROTATION)
                if rotation != 0:
                    logo = logo.rotate(rotation, expand=True)
                    # Recalculate size after rotation if it expanded
                    logo_w, logo_h = logo.size

                # ── RANDOM POSITION ──
                margin = 20
                positions = {
                    "TOP_LEFT": (margin, margin),
                    "TOP_RIGHT": (base.width - logo_w - margin, margin),
                    "BOTTOM_LEFT": (margin, base.height - logo_h - margin),
                    "BOTTOM_RIGHT": (base.width - logo_w - margin, base.height - logo_h - margin),
                    "CENTER": ((base.width - logo_w) // 2, (base.height - logo_h) // 2)
                }
                pos = positions[config.get("position", random.choice(list(positions.keys())))]
                
                # ── OPACITY from config or settings ──
                opacity = config.get("opacity", settings.WATERMARK_OPACITY)
                # Apply opacity to logo
                logo_with_opacity = logo.copy()
                alpha = logo_with_opacity.getchannel('A')
                alpha = alpha.point(lambda p: int(p * opacity))
                logo_with_opacity.putalpha(alpha)

                # Apply logo
                base.alpha_composite(logo_with_opacity, dest=pos)
                
                # Save as high quality JPEG
                base.convert("RGB").save(output_path, "JPEG", quality=95)
                
            return output_path
        except Exception as e:
            logger.error("Photo watermarking failed", extra={"ctx_error": str(e)})
            shutil.copy(input_path, output_path)
            return output_path

    async def _process_video(self, input_path: str, output_path: str, config: dict) -> str:
        text1 = config.get("watermark_text", settings.WATERMARK_TEXT_NSFW or "BDGW")
        text2 = config.get("watermark_text_secondary", settings.WATERMARK_TEXT_PREMIUM or "VaultFlow")

        # FFmpeg drawtext requires alpha as a float in [0.0, 1.0].
        # settings.WATERMARK_OPACITY is already normalized to 0.0-1.0 by
        # the settings validator. The config dict may also carry a pre-normalized
        # float value. Use it directly — do NOT divide by 255 again.
        raw_opacity = config.get("opacity", settings.WATERMARK_OPACITY) or 0.42
        raw_float = float(raw_opacity)
        # Safety: if someone passed an old-style 0-255 integer, normalize it.
        if raw_float > 1.0:
            raw_float = raw_float / 255.0
        opacity_float = round(min(max(raw_float, 0.0), 1.0), 3)

        # Randomize start times (0 to 5 seconds)
        start1 = round(random.uniform(0, 5), 1)
        start2 = round(random.uniform(0, 5), 1)

        offset_x1 = random.randint(20, 100)
        offset_y1 = random.randint(20, 100)
        offset_x2 = random.randint(20, 100)
        offset_y2 = random.randint(20, 100)

        # Corner selection for pos1
        corner1 = random.choice(["TL", "TR", "BL", "BR", "C"])
        if corner1 == "TL":   pos1 = f"x={offset_x1}:y={offset_y1}"
        elif corner1 == "TR": pos1 = f"x=w-text_w-{offset_x1}:y={offset_y1}"
        elif corner1 == "BL": pos1 = f"x={offset_x1}:y=h-text_h-{offset_y1}"
        elif corner1 == "BR": pos1 = f"x=w-text_w-{offset_x1}:y=h-text_h-{offset_y1}"
        else:                  pos1 = "x=(w-text_w)/2:y=(h-text_h)/2"

        # Corner selection for pos2 (different from pos1)
        corner2 = random.choice([c for c in ["TL", "TR", "BL", "BR", "C"] if c != corner1])
        if corner2 == "TL":   pos2 = f"x={offset_x2}:y={offset_y2}"
        elif corner2 == "TR": pos2 = f"x=w-text_w-{offset_x2}:y={offset_y2}"
        elif corner2 == "BL": pos2 = f"x={offset_x2}:y=h-text_h-{offset_y2}"
        elif corner2 == "BR": pos2 = f"x=w-text_w-{offset_x2}:y=h-text_h-{offset_y2}"
        else:                  pos2 = "x=(w-text_w)/2:y=(h-text_h)/2"

        # Escape single quotes in text strings to avoid breaking the FFmpeg
        # filter expression (e.g. "BD GONE WILD ✦ PREMIUM" is safe, but any
        # literal apostrophe would break the filter string).
        def _esc(t: str) -> str:
            return t.replace("'", "\\'").replace(":", "\\:")

        font_path = getattr(settings, "WATERMARK_FONT_PATH", None)
        font_clause = f":fontfile='{font_path}'" if font_path and Path(font_path).exists() else ""

        drawtext1 = (
            f"drawtext=text='{_esc(text1)}'"
            f"{font_clause}"
            f":{pos1}"
            f":enable='between(t,{start1},99999)'"
            f":fontsize=h/20:fontcolor=white@{opacity_float}"
            f":shadowcolor=black:shadowx=2:shadowy=2"
        )

        drawtext2 = (
            f"drawtext=text='{_esc(text2)}'"
            f"{font_clause}"
            f":{pos2}"
            f":enable='between(t,{start2},99999)'"
            f":fontsize=h/25:fontcolor=white@{opacity_float}"
            f":shadowcolor=black:shadowx=2:shadowy=2"
        )

        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-vf", f"{drawtext1},{drawtext2}",
            "-c:a", "copy",
            "-preset", "ultrafast",
            output_path,
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

    async def apply_image_watermark(self, input_path: str, watermark_path: str, position: WatermarkPosition, opacity: float, scale: float) -> str:
        output_path = self._generate_output_path(input_path)
        return await self._process_photo(input_path, output_path, {
            "watermark_image_path": watermark_path,
            "position": position.value,
            "opacity": opacity,
            "scale": scale,
        })

    async def apply_video_watermark(self, input_path: str, watermark_path: str, position: WatermarkPosition, opacity: float, scale: float, watermark_text: str = "BDGW") -> str:
        output_path = self._generate_output_path(input_path)
        return await self._process_video(input_path, output_path, {
            "watermark_text": watermark_text,
            "position": position.value,
            "opacity": opacity,
            "scale": scale,
        })
