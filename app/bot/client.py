from __future__ import annotations

from typing import Optional

from pyrogram import Client

from app.config.settings import settings

_bot_instance: Optional[Client] = None


def get_bot() -> Client:
    """Return the singleton Pyrogram Client, creating it once."""
    global _bot_instance
    if _bot_instance is None:
        _bot_instance = Client(
            name=settings.SESSION_NAME,
            bot_token=settings.BOT_TOKEN,
            api_id=settings.API_ID,
            api_hash=settings.API_HASH,
        )
    return _bot_instance