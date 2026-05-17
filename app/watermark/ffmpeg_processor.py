import asyncio
import hashlib
import os
import shutil
import uuid
from pathlib import Path
from typing import Optional
from app.config import settings
from app.core.models import MediaType, WatermarkPosition
from app.core.exceptions import (
    FFmpegNotFoundError,
    FFmpegTimeoutError,
    WatermarkProcessingError,
    MediaFileNotFoundError,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


class WatermarkCache:
    """
    Maps (watermark_path, target_dimensions) → cached overlay path.
    Prevents redundant FFmpeg rescale operations on the same watermark asset.
    """

    def __init__(self, cache_dir: str):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._entries: dict[str, Path] = {}

    def _key(self, watermark_path: str, width: int, height: int) -> str:
        raw = f"{watermark_path}:{width}x{height}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def get(self, watermark_path: str, width: int, height: int) -> Optional[Path]:
        key = self._key(watermark_path, width, height)
        cached = self._entries.get(key)
        if cached and cached.exists():
            return cached
        self._entries.pop(key, None)
        return None

    def store(self, watermark_path: str, width: int, height: int, result_path: Path) -> None:
        key = self._key(watermark_path, width, height)
        self._entries[key] = result_path

    def invalidate(self, watermark_path: str) -> None:
        keys_to_remove = [
            k for k, _ in self._entries.items() if watermark_path in k
        ]
        for k in keys_to_remove:
            self._entries.pop(k, None)

    def clear(self) -> None:
        self._entries.clear()
        shutil.rmtree(self._cache_dir, ignore_errors=True)
        self._cache_dir.mkdir(parents=True, exist_ok=True)


class FFmpegProcessor:
    """
    Async FFmpeg watermark engine.
    Never blocks the event loop — all FFmpeg calls use asyncio.create_subprocess_exec.
    """

    def __init__(self):
        self._ffmpeg_path: Optional[str] = None
        self._cache = WatermarkCache(settings.WATERMARK_CACHE_DIR)
        self._output_dir = Path(settings.PROCESSED_MEDIA_DIR)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    async def ensure_ffmpeg(self) -> str:
        if self._ffmpeg_path:
            return self._ffmpeg_path

        path = shutil.which("ffmpeg")
        if not path:
            raise FFmpegNotFoundError(
                "ffmpeg not found in PATH. Install ffmpeg to enable watermarking."
            )
        self._ffmpeg_path = path
        return path

    async def apply_image_watermark(
        self,
        input_path: str,
        watermark_path: str,
        output_path: Optional[str] = None,
        position: WatermarkPosition = WatermarkPosition.BOTTOM_RIGHT,
        opacity: Optional[float] = None,
        scale: Optional[float] = None,
    ) -> str:
        """Apply PNG watermark to an image using FFmpeg."""
        await self.ensure_ffmpeg()

        if not Path(input_path).exists():
            raise MediaFileNotFoundError(f"Input file not found: {input_path}")
        if not Path(watermark_path).exists():
            raise MediaFileNotFoundError(f"Watermark file not found: {watermark_path}")

        op = opacity if opacity is not None else settings.WATERMARK_OPACITY
        sc = scale if scale is not None else settings.WATERMARK_SCALE
        overlay = self._build_overlay_expression(position)

        out_path = output_path or self._generate_output_path(input_path, "jpg")

        # scale watermark relative to input, then overlay with alpha
        filter_complex = (
            f"[1:v]scale=iw*{sc}:-1,format=rgba,colorchannelmixer=aa={op}[wm];"
            f"[0:v][wm]{overlay}[out]"
        )

        cmd = [
            self._ffmpeg_path,
            "-y",
            "-i", input_path,
            "-i", watermark_path,
            "-filter_complex", filter_complex,
            "-map", "[out]",
            "-q:v", "2",
            out_path,
        ]

        await self._run_ffmpeg(cmd, operation="image_watermark", input_path=input_path)
        logger.info(
            "Image watermark applied",
            extra={"ctx_input": input_path, "ctx_output": out_path},
        )
        return out_path

    async def apply_video_watermark(
        self,
        input_path: str,
        watermark_path: str,
        output_path: Optional[str] = None,
        position: WatermarkPosition = WatermarkPosition.BOTTOM_RIGHT,
        opacity: Optional[float] = None,
        scale: Optional[float] = None,
    ) -> str:
        """Apply PNG watermark to a video using FFmpeg, preserving audio."""
        await self.ensure_ffmpeg()

        if not Path(input_path).exists():
            raise MediaFileNotFoundError(f"Input file not found: {input_path}")
        if not Path(watermark_path).exists():
            raise MediaFileNotFoundError(f"Watermark file not found: {watermark_path}")

        op = opacity if opacity is not None else settings.WATERMARK_OPACITY
        sc = scale if scale is not None else settings.WATERMARK_SCALE
        overlay = self._build_overlay_expression(position)

        out_path = output_path or self._generate_output_path(input_path, "mp4")

        filter_complex = (
            f"[1:v]scale=iw*{sc}:-1,format=rgba,colorchannelmixer=aa={op}[wm];"
            f"[0:v][wm]{overlay}[out]"
        )

        cmd = [
            self._ffmpeg_path,
            "-y",
            "-i", input_path,
            "-i", watermark_path,
            "-filter_complex", filter_complex,
            "-map", "[out]",
            "-map", "0:a?",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-c:a", "copy",
            "-movflags", "+faststart",
            out_path,
        ]

        await self._run_ffmpeg(cmd, operation="video_watermark", input_path=input_path)
        logger.info(
            "Video watermark applied",
            extra={"ctx_input": input_path, "ctx_output": out_path},
        )
        return out_path

    async def get_media_dimensions(self, file_path: str) -> tuple[int, int]:
        """Returns (width, height) using ffprobe."""
        ffprobe = shutil.which("ffprobe")
        if not ffprobe:
            raise FFmpegNotFoundError("ffprobe not found")

        cmd = [
            ffprobe,
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            file_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        line = stdout.decode().strip()
        parts = line.split(",")
        if len(parts) != 2:
            raise WatermarkProcessingError(f"Could not parse dimensions from: {line}")
        return int(parts[0]), int(parts[1])

    async def _run_ffmpeg(self, cmd: list[str], operation: str, input_path: str) -> None:
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=settings.FFMPEG_TIMEOUT,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise FFmpegTimeoutError(
                    f"FFmpeg {operation} timed out after {settings.FFMPEG_TIMEOUT}s: {input_path}"
                )

            if proc.returncode != 0:
                error_msg = stderr.decode(errors="replace").strip()
                raise WatermarkProcessingError(
                    f"FFmpeg {operation} failed (exit {proc.returncode}): {error_msg[-500:]}"
                )

        except (FFmpegTimeoutError, WatermarkProcessingError):
            raise
        except Exception as e:
            raise WatermarkProcessingError(f"FFmpeg subprocess error: {e}") from e

    @staticmethod
    def _build_overlay_expression(position: WatermarkPosition) -> str:
        margin = 10
        positions = {
            WatermarkPosition.TOP_LEFT: f"overlay={margin}:{margin}",
            WatermarkPosition.TOP_RIGHT: f"overlay=W-w-{margin}:{margin}",
            WatermarkPosition.BOTTOM_LEFT: f"overlay={margin}:H-h-{margin}",
            WatermarkPosition.BOTTOM_RIGHT: f"overlay=W-w-{margin}:H-h-{margin}",
            WatermarkPosition.CENTER: "overlay=(W-w)/2:(H-h)/2",
        }
        return positions.get(position, f"overlay=W-w-{margin}:H-h-{margin}")

    def _generate_output_path(self, input_path: str, ext: str) -> str:
        stem = Path(input_path).stem
        unique = uuid.uuid4().hex[:8]
        filename = f"{stem}_wm_{unique}.{ext}"
        return str(self._output_dir / filename)

    @property
    def cache(self) -> WatermarkCache:
        return self._cache
