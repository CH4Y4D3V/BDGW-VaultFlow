"""
app/services/topic_service.py

Thin compatibility shim for User-Centric Topic Manager.
The canonical implementation lives in app/services/topic_manager.py.
"""
from __future__ import annotations

# Re-export everything from the canonical module
from app.services.topic_manager import (
    TopicManager,
    get_topic_manager,
    TOPIC_CONTENT,
    TOPIC_SUPPORT,
    TOPIC_PAYMENT,
    TOPIC_REJECTED,
)

# Aliases for backward compatibility
TopicService = TopicManager
get_topic_service = get_topic_manager


async def _warm_cache_from_db_alias(self) -> None:
    """Alias: warm_cache_from_db → restore_cache (lifecycle.py uses the former)."""
    await self.restore_cache()


# Patch the alias onto the class if it doesn't already have it.
if not hasattr(TopicManager, "warm_cache_from_db"):
    TopicManager.warm_cache_from_db = _warm_cache_from_db_alias


__all__ = [
    "TopicService",
    "TopicManager",
    "get_topic_service",
    "get_topic_manager",
    "TOPIC_CONTENT",
    "TOPIC_SUPPORT",
    "TOPIC_PAYMENT",
    "TOPIC_REJECTED",
]
