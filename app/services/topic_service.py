"""
app/services/topic_service.py

Thin compatibility shim so that imports from EITHER:
  - app.services.topic_service
  - app.services.topic_manager

resolve to the SAME TopicManager singleton.

The canonical implementation lives in app/services/topic_manager.py.
This file is a pure re-export with backward-compat aliases.

DO NOT put logic here. All logic stays in topic_manager.py.
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

# Aliases so lifecycle.py and any code using TopicService still works
TopicService = TopicManager
get_topic_service = get_topic_manager


async def _warm_cache_from_db_alias(self) -> None:
    """Alias: warm_cache_from_db → restore_cache (lifecycle.py uses the former)."""
    await self.restore_cache()


# Patch the alias onto the class if it doesn't already have it.
# This handles lifecycle.py calling `topic_mgr.warm_cache_from_db()`.
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
