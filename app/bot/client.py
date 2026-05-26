from __future__ import annotations

from typing import Optional

from pyrogram.client import Client

from app.config import settings

_bot_instance: Optional[Client] = None
_bot_id: Optional[int] = None


def get_bot() -> Client:
    """Return the singleton Pyrogram Client, creating it once."""
    global _bot_instance

    if _bot_instance is None:
        if (
            not settings.BOT_TOKEN
            or not getattr(settings, "API_ID", None)
            or not getattr(settings, "API_HASH", None)
        ):
            raise RuntimeError(
                "CRITICAL: Pyrogram client missing mandatory Telegram credentials "
                "(BOT_TOKEN, API_ID, API_HASH). Cannot start client."
            )

        _bot_instance = Client(
            name=settings.SESSION_NAME,
            bot_token=settings.BOT_TOKEN,
            api_id=settings.API_ID,
            api_hash=settings.API_HASH,
            plugins=dict(root="app.handlers"),
            workers=min(
                32,
                getattr(settings, "DISPATCHER_WORKER_COUNT", 4) * 4,
            ),
            max_concurrent_transmissions=getattr(
                settings,
                "MAX_CONCURRENT_TRANSMISSIONS",
                10,
            ),
        )

    return _bot_instance


def get_bot_id() -> Optional[int]:
    return _bot_id


def set_bot_id(user_id: int) -> None:
    global _bot_id
    _bot_id = user_id