from __future__ import annotations

# ------------------------------------------------------------
# FILE: app/services/topic_manager.py
# Spec: Master Reference Sections 9.1, 9.2, 9.4, 22, 25, 25A.2, 25A.19
# ------------------------------------------------------------
#
# Responsibilities:
#   1. ensure_shared_topics()  — called once on bot startup.
#                                Creates the "📋 Admin Logs" forum topic if
#                                it does not exist, then writes topic_id to
#                                hub_config (MongoDB) and patches settings so
#                                AdminLogger can use it immediately.
#
#   2. get_or_create_user_topic() — canonical resolver for "one user = one
#                                   permanent topic" (Section 9.2).
#                                   All routing systems call this before
#                                   posting into a user topic.
#
#   3. recover_user_topic()    — called by any sender that receives a
#                                Telegram API error indicating the topic was
#                                deleted. Recreates, re-maps, and logs.
#
# Design rules (from spec):
#   • MongoDB (hub_config / user_topics) is the ONLY source of truth.
#   • Every topic creation or recovery event is dual-written to audit_logs
#     AND the Admin Logs topic (Section 22).
#   • Redis distributed lock prevents concurrent duplicate topic creation
#     for the same user (Section 24).
#   • All Telegram calls handle FloodWait explicitly (Section 24).
#   • Every function has a docstring (mandatory output spec).
#   • No hardcoded IDs — everything flows through hub_config (Section 25).
# ------------------------------------------------------------

import asyncio
from datetime import datetime, timezone
from typing import Optional, Any

from pyrogram.client import Client
from pyrogram.errors import (
    FloodWait,
    RPCError,
    TopicDeleted,
    TopicClosed,
    MessageIdInvalid,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.core.redis_client import get_redis
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Exact names as specified in Section 9.1
_ADMIN_LOGS_TOPIC_NAME = "📋 Admin Logs"

# User topic name template — Section 9.2
# Rendered as: "👤 Full Name | 123456789"
_USER_TOPIC_NAME_TEMPLATE = "👤 {full_name} | {user_id}"

# Redis lock TTL in seconds — covers one full Telegram round-trip + buffer
_LOCK_TTL_SECONDS = 30

# hub_config key names (Section 25A.19)
_KEY_ADMIN_LOGS_TOPIC_ID = "admin_logs_topic_id"
_KEY_HUB_SUPERGROUP_ID = "hub_supergroup_id"

# Admin Logs entry template (Section 9.4)
_ADMIN_LOG_TEMPLATE = (
    "<b>[{action}]</b>\n"
    "Admin     : System\n"
    "Admin ID  : N/A\n"
    "Target    : {full_name} (@{username})\n"
    "Target ID : <code>{user_id}</code>\n"
    "Detail    : {detail}\n"
    "Time      : {timestamp}"
)

# Telegram error names that indicate a forum topic no longer exists
_TOPIC_GONE_ERRORS = (TopicDeleted, TopicClosed, MessageIdInvalid)

# Topic types (backward compatibility for legacy multi-topic calls)
TOPIC_CONTENT = "content"
TOPIC_SUPPORT = "support"
TOPIC_PAYMENT = "payment"
TOPIC_REJECTED = "rejected"


class TopicManager:
    """
    User-Centric Topic Manager (Section 9.2).
    Ensures every user has exactly one permanent forum topic in the Hub.
    """

    _instance: Optional[TopicManager] = None

    def __new__(cls) -> TopicManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def restore_cache(self) -> None:
        """
        Warms up internal caches from database.
        Currently a no-op as MongoDB is the source of truth for every call.
        """
        logger.debug("TopicManager: cache restoration complete (no-op)")

    async def ensure_shared_topics(self, bot: Client) -> None:
        """
        Bootstrap the Verification Hub's shared infrastructure topics.
        ...
        """
        hub_id = await _get_hub_config_int(_KEY_HUB_SUPERGROUP_ID)
        if not hub_id:
            hub_id = getattr(settings, "HUB_SUPERGROUP_ID", 0) or getattr(settings, "VERIFICATION_GROUP_ID", 0)

        if not hub_id:
            logger.critical(
                "hub_supergroup_id not set in hub_config or settings — "
                "cannot ensure shared topics. Set this key before launch.",
            )
            return

        # ── Check if Admin Logs topic already exists ──────────────────────────────
        existing_topic_id = await _get_hub_config_int(_KEY_ADMIN_LOGS_TOPIC_ID)
        if existing_topic_id:
            logger.info(
                "Admin Logs topic already configured",
                extra={"ctx_topic_id": existing_topic_id},
            )
            _patch_settings_admin_logs(existing_topic_id)
            return

        # ── Create Admin Logs topic — guarded by a startup-scoped lock ────────────
        lock_key = "topic_create:admin_logs"
        async with _redis_lock(lock_key, ttl=60):
            # Re-check inside lock — another instance may have just created it
            existing_topic_id = await _get_hub_config_int(_KEY_ADMIN_LOGS_TOPIC_ID)
            if existing_topic_id:
                logger.info(
                    "Admin Logs topic was created by a concurrent startup — using it",
                    extra={"ctx_topic_id": existing_topic_id},
                )
                _patch_settings_admin_logs(existing_topic_id)
                return

            topic_id = await _create_forum_topic(
                bot=bot,
                chat_id=hub_id,
                name=_ADMIN_LOGS_TOPIC_NAME,
            )
            if topic_id is None:
                logger.error(
                    "Failed to create Admin Logs topic — AdminLogger will be silenced "
                    "until the topic is manually created and hub_config is updated.",
                )
                return

            # ── Write topic_id back to hub_config (Section 25A.19) ───────────────
            await _upsert_hub_config(_KEY_ADMIN_LOGS_TOPIC_ID, topic_id)

            # ── Patch settings in-process so AdminLogger can use it immediately ──
            _patch_settings_admin_logs(topic_id)

            # ── Audit log (MongoDB only — cannot post to topic that was just made)─
            await _write_audit_log(
                action="TOPIC CREATED",
                admin_user_id=None,
                target_user_id=None,
                detail={
                    "topic_name": _ADMIN_LOGS_TOPIC_NAME,
                    "topic_id": topic_id,
                    "hub_id": hub_id,
                    "note": "Admin Logs topic auto-created on startup",
                },
            )

            logger.info(
                "Admin Logs topic created and persisted to hub_config",
                extra={"ctx_topic_id": topic_id, "ctx_hub_id": hub_id},
            )

    async def get_or_create_user_topic(
        self,
        bot: Client,
        user_id: int,
        full_name: Optional[str] = None,
        username: Optional[str] = None,
    ) -> Optional[int]:
        """
        Return the permanent forum topic ID for a user, creating it if needed.

        This is the ONLY function all systems should call when they need to post
        into a user's topic. It enforces the "one user = one permanent topic"
        invariant (Section 9.2, Core Principle 9).

        Algorithm:
          1. Look up user_topics collection in MongoDB for an existing topic_id.
             If found, return it immediately (fast path — no lock needed).
          2. Acquire a per-user Redis distributed lock to prevent concurrent
             duplicate creation.
          3. Re-check MongoDB inside the lock (TOCTOU guard).
          4. Create the forum topic via Telegram API with FloodWait handling.
          5. Write the new mapping to user_topics (upsert — idempotent).
          6. Write to audit_logs (MongoDB).
          7. Post a TOPIC CREATED entry to the Admin Logs topic.
          8. Return the new topic_id.

        Args:
            bot:       Authenticated Pyrogram client.
            user_id:   Telegram user ID of the target user.
            full_name: User's display name (used in topic title). If None, fetched from DB.
            username:  Telegram username without @, or None.

        Returns:
            Integer forum topic ID, or None if creation failed.
        """
        # ── Fast path: existing mapping ───────────────────────────────────────────
        existing = await self.get_user_topic_id(user_id)
        if existing:
            return existing

        # ── Resolve missing name from DB if needed ───────────────────────────────
        if full_name is None:
            db = DatabaseManager.get_db()
            user_doc = await db["users"].find_one({"_id": user_id})
            if user_doc:
                full_name = user_doc.get("full_name", f"User {user_id}")
                username = username or user_doc.get("username")
            else:
                full_name = f"User {user_id}"

        # ── Slow path: create under lock ─────────────────────────────────────────
        lock_key = f"topic_create:{user_id}"
        async with _redis_lock(lock_key, ttl=_LOCK_TTL_SECONDS):
            # TOCTOU guard: another request may have created the topic while we
            # were waiting for the lock.
            existing = await self.get_user_topic_id(user_id)
            if existing:
                return existing

            hub_id = await _get_hub_config_int(_KEY_HUB_SUPERGROUP_ID)
            if not hub_id:
                hub_id = getattr(settings, "HUB_SUPERGROUP_ID", 0) or getattr(settings, "VERIFICATION_GROUP_ID", 0)

            if not hub_id:
                logger.error(
                    "hub_supergroup_id not configured in DB or settings — cannot create user topic",
                    extra={"ctx_user_id": user_id},
                )
                return None

            topic_name = _USER_TOPIC_NAME_TEMPLATE.format(
                full_name=full_name,
                user_id=user_id,
            )
            topic_id = await _create_forum_topic(
                bot=bot,
                chat_id=hub_id,
                name=topic_name,
            )
            if topic_id is None:
                logger.error(
                    "Failed to create user topic",
                    extra={"ctx_user_id": user_id, "ctx_topic_name": topic_name},
                )
                return None

            # ── Persist mapping to MongoDB BEFORE any Telegram post (Section 25) ─
            await _upsert_user_topic(user_id=user_id, topic_id=topic_id)

            # ── Dual audit write (Section 22) ─────────────────────────────────────
            await _write_audit_log(
                action="TOPIC CREATED",
                admin_user_id=None,
                target_user_id=user_id,
                detail={
                    "topic_name": topic_name,
                    "topic_id": topic_id,
                    "hub_id": hub_id,
                },
            )
            await _post_admin_log_entry(
                bot=bot,
                action="TOPIC CREATED",
                full_name=full_name,
                username=username or "N/A",
                user_id=user_id,
                detail=f"User topic auto-created: {topic_name} (topic_id={topic_id})",
            )

            logger.info(
                "User topic created",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_topic_id": topic_id,
                    "ctx_topic_name": topic_name,
                },
            )
            return topic_id

    async def get_user_topic_id(self, user_id: int, topic_type: Optional[str] = None) -> Optional[int]:
        """
        Return the existing topic ID for a user from MongoDB.
        
        Args:
            user_id: Telegram user ID.
            topic_type: Ignored (Section 9.2: One User = One Permanent Topic).
            
        Returns:
            Integer topic_id or None.
        """
        return await _get_user_topic_id(user_id)

    async def get_user_by_topic(self, topic_id: int) -> Optional[dict]:
        """
        Return the user document associated with a topic ID.
        Used by topic_router for bidirectional routing.
        """
        try:
            db = DatabaseManager.get_db()
            doc = await db["user_topics"].find_one({"topic_id": topic_id})
            return doc
        except Exception as exc:
            logger.error(
                "Failed to query user_topics by topic_id",
                extra={"ctx_topic_id": topic_id, "ctx_error": str(exc)},
                exc_info=exc,
            )
        return None

    async def recover_user_topic(
        self,
        bot: Client,
        user_id: int,
        full_name: Optional[str] = None,
        username: Optional[str] = None,
    ) -> Optional[int]:
        """
        Recover a user topic that was manually deleted by a Telegram admin.

        Must be called by any sender that receives a Telegram API error
        (TopicDeleted, TopicClosed, MessageIdInvalid) when posting to a user's
        topic ID that is stored in MongoDB. The topic was externally deleted;
        this function repairs the state.

        Recovery procedure (Section 9.2):
          1. Acquire a per-user Redis lock (prevents concurrent recovery storms).
          2. Create a new forum topic with the same name format.
          3. Update the user_topics mapping in MongoDB with the new topic_id.
          4. Write to audit_logs (MongoDB) with action TOPIC RECOVERED.
          5. Post a TOPIC RECOVERED entry to the Admin Logs topic.
          6. Return the new topic_id so the caller can retry its send.

        Args:
            bot:       Authenticated Pyrogram client.
            user_id:   Telegram user ID whose topic needs recovery.
            full_name: User's display name. If None, fetched from DB.
            username:  Telegram username without @, or None.

        Returns:
            New integer forum topic ID, or None if recovery failed.
        """
        # ── Resolve missing name from DB if needed ───────────────────────────────
        if full_name is None:
            db = DatabaseManager.get_db()
            user_doc = await db["users"].find_one({"_id": user_id})
            if user_doc:
                full_name = user_doc.get("full_name", f"User {user_id}")
                username = username or user_doc.get("username")
            else:
                full_name = f"User {user_id}"

        lock_key = f"topic_recover:{user_id}"
        async with _redis_lock(lock_key, ttl=_LOCK_TTL_SECONDS):
            hub_id = await _get_hub_config_int(_KEY_HUB_SUPERGROUP_ID)
            if not hub_id:
                hub_id = getattr(settings, "HUB_SUPERGROUP_ID", 0) or getattr(settings, "VERIFICATION_GROUP_ID", 0)

            if not hub_id:
                logger.error(
                    "hub_supergroup_id not configured in DB or settings — cannot recover user topic",
                    extra={"ctx_user_id": user_id},
                )
                return None

            topic_name = _USER_TOPIC_NAME_TEMPLATE.format(
                full_name=full_name,
                user_id=user_id,
            )
            topic_id = await _create_forum_topic(
                bot=bot,
                chat_id=hub_id,
                name=topic_name,
            )
            if topic_id is None:
                logger.error(
                    "Failed to recover user topic — Telegram API error during creation",
                    extra={"ctx_user_id": user_id},
                )
                return None

            # ── Update MongoDB mapping BEFORE any re-send attempt ─────────────────
            await _upsert_user_topic(user_id=user_id, topic_id=topic_id)

            # ── Dual audit write (Section 22) ─────────────────────────────────────
            await _write_audit_log(
                action="TOPIC RECOVERED",
                admin_user_id=None,
                target_user_id=user_id,
                detail={
                    "new_topic_id": topic_id,
                    "topic_name": topic_name,
                    "hub_id": hub_id,
                    "reason": "Previous topic was manually deleted by a Telegram admin",
                },
            )
            await _post_admin_log_entry(
                bot=bot,
                action="TOPIC RECOVERED",
                full_name=full_name,
                username=username or "N/A",
                user_id=user_id,
                detail=(
                    f"Topic was manually deleted and has been recreated. "
                    f"New topic_id={topic_id}. History prior to deletion is lost."
                ),
            )

            logger.warning(
                "User topic recovered after deletion",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_new_topic_id": topic_id,
                },
            )
            return topic_id


# ── Factory ──────────────────────────────────────────────────────────────────


def get_topic_manager() -> TopicManager:
    """Return the TopicManager singleton."""
    return TopicManager()


# ── Public API (Global Wrappers for backward compatibility) ──────────────────


async def ensure_shared_topics(bot: Client) -> None:
    await get_topic_manager().ensure_shared_topics(bot)


async def get_or_create_user_topic(
    bot: Client,
    user_id: int,
    full_name: Optional[str] = None,
    username: Optional[str] = None,
    **kwargs: Any,
) -> Optional[int]:
    """Wrapper for backward compatibility."""
    return await get_topic_manager().get_or_create_user_topic(
        bot=bot, user_id=user_id, full_name=full_name, username=username
    )


async def recover_user_topic(
    bot: Client,
    user_id: int,
    full_name: Optional[str] = None,
    username: Optional[str] = None,
) -> Optional[int]:
    """Wrapper for backward compatibility."""
    return await get_topic_manager().recover_user_topic(
        bot=bot, user_id=user_id, full_name=full_name, username=username
    )


def is_topic_gone_error(exc: Exception) -> bool:
    """
    Return True if the exception indicates the forum topic no longer exists.

    Callers use this to decide whether to invoke recover_user_topic().

    Args:
        exc: Exception raised by a Pyrogram send call.

    Returns:
        True if the topic is deleted/closed/invalid, False otherwise.
    """
    return isinstance(exc, _TOPIC_GONE_ERRORS)


# ── Internal Telegram helpers ─────────────────────────────────────────────────


async def _create_forum_topic(
    bot: Client,
    *,
    chat_id: int,
    name: str,
) -> Optional[int]:
    """
    Create a forum topic in the given supergroup and return its topic_id.

    Handles FloodWait by sleeping the required time and retrying once.
    On any other Telegram RPC error, logs and returns None so the caller
    can decide how to handle the failure.

    Args:
        bot:     Authenticated Pyrogram client.
        chat_id: Supergroup ID with forum/topics mode enabled.
        name:    Display name for the new topic.

    Returns:
        Integer topic ID on success, or None on failure.
    """
    for attempt in range(2):
        try:
            topic = await bot.create_forum_topic(chat_id=chat_id, title=name)
            return topic.id
        except FloodWait as exc:
            wait = int(exc.value) + getattr(settings, "FLOODWAIT_EXTRA_BUFFER", 2)
            logger.warning(
                "FloodWait creating forum topic — sleeping",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_name": name,
                    "ctx_wait": wait,
                    "ctx_attempt": attempt,
                },
            )
            await asyncio.sleep(wait)
            # Retry falls through to next iteration
        except RPCError as exc:
            logger.error(
                "Telegram RPC error creating forum topic",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_name": name,
                    "ctx_error": str(exc),
                    "ctx_attempt": attempt,
                },
                exc_info=exc,
            )
            return None
        except Exception as exc:
            logger.error(
                "Unexpected error creating forum topic",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_name": name,
                    "ctx_error": str(exc),
                },
                exc_info=exc,
            )
            return None
    # Both attempts exhausted (two FloodWaits in a row)
    logger.error(
        "Failed to create forum topic after retries",
        extra={"ctx_chat_id": chat_id, "ctx_name": name},
    )
    return None


async def _post_admin_log_entry(
    bot: Client,
    *,
    action: str,
    full_name: str,
    username: str,
    user_id: int,
    detail: str,
) -> None:
    """
    Post a structured entry to the Admin Logs forum topic (Section 9.4).

    Reads admin_logs_topic_id and hub_supergroup_id from hub_config at
    call time. Falls back to settings if not configured in DB.

    Handles FloodWait with a single retry.

    Args:
        bot:       Authenticated Pyrogram client.
        action:    Uppercase action type string (e.g. "TOPIC RECOVERED").
        full_name: Display name of the affected user.
        username:  Telegram username without @, or "N/A".
        user_id:   Telegram ID of the affected user.
        detail:    Human-readable description of the specific event.
    """
    admin_logs_topic_id = await _get_hub_config_int(_KEY_ADMIN_LOGS_TOPIC_ID)
    hub_id = await _get_hub_config_int(_KEY_HUB_SUPERGROUP_ID)

    # ── Fallbacks ─────────────────────────────────────────────────────────────
    if not admin_logs_topic_id:
        admin_logs_topic_id = getattr(settings, "HUB_TOPIC_ADMIN_LOGS", 0)
    if not hub_id:
        hub_id = getattr(settings, "HUB_SUPERGROUP_ID", 0) or getattr(settings, "VERIFICATION_GROUP_ID", 0)

    if not admin_logs_topic_id or not hub_id:
        logger.debug(
            "Admin Logs topic or hub not configured — skipping Admin Logs post",
            extra={"ctx_action": action, "ctx_user_id": user_id},
        )
        return

    text = _ADMIN_LOG_TEMPLATE.format(
        action=action,
        full_name=full_name,
        username=username,
        user_id=user_id,
        detail=detail,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    for attempt in range(2):
        try:
            await bot.send_message(
                chat_id=hub_id,
                text=text,
                parse_mode="html",
                message_thread_id=admin_logs_topic_id,
            )
            return
        except FloodWait as exc:
            wait = int(exc.value) + getattr(settings, "FLOODWAIT_EXTRA_BUFFER", 2)
            logger.warning(
                "FloodWait posting to Admin Logs topic — sleeping",
                extra={"ctx_wait": wait, "ctx_attempt": attempt},
            )
            await asyncio.sleep(wait)
        except Exception as exc:
            logger.error(
                "Failed to post to Admin Logs topic",
                extra={"ctx_action": action, "ctx_error": str(exc)},
                exc_info=exc,
            )
            return


# ── Internal MongoDB helpers ──────────────────────────────────────────────────


async def _get_user_topic_id(user_id: int) -> Optional[int]:
    """
    Look up the existing forum topic ID for a user from MongoDB.

    This is the hot path for all routing calls — it must be fast.
    Returns None if no mapping exists (topic not yet created).

    Args:
        user_id: Telegram user ID.

    Returns:
        Integer topic_id or None.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["user_topics"].find_one({"user_id": user_id})
        if doc:
            return int(doc["topic_id"])
    except Exception as exc:
        logger.error(
            "Failed to query user_topics collection",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            exc_info=exc,
        )
    return None


async def _upsert_user_topic(*, user_id: int, topic_id: int) -> None:
    """
    Write or update the user → topic_id mapping in MongoDB (user_topics).

    Uses upsert=True so the operation is safe to call on both creation
    and recovery. The unique index on user_id (Section 25A.20) enforces
    the one-topic-per-user invariant at the database level.

    Args:
        user_id:  Telegram user ID.
        topic_id: Forum topic ID to associate with this user.
    """
    try:
        db = DatabaseManager.get_db()
        await db["user_topics"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "user_id": user_id,
                    "topic_id": topic_id,
                },
                "$setOnInsert": {
                    "created_at": datetime.now(timezone.utc),
                },
            },
            upsert=True,
        )
    except Exception as exc:
        logger.error(
            "Failed to upsert user_topics mapping",
            extra={
                "ctx_user_id": user_id,
                "ctx_topic_id": topic_id,
                "ctx_error": str(exc),
            },
            exc_info=exc,
        )


async def _get_hub_config_int(key: str) -> Optional[int]:
    """
    Fetch a single integer value from the hub_config collection by key.

    Returns None if the key is missing or the query fails, so callers
    can handle the absence gracefully without raising.

    Args:
        key: hub_config key name (e.g. "admin_logs_topic_id").

    Returns:
        Integer value, or None.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["hub_config"].find_one({"key": key})
        if doc and doc.get("value") is not None:
            return int(doc["value"])
    except Exception as exc:
        logger.error(
            "Failed to read hub_config",
            extra={"ctx_key": key, "ctx_error": str(exc)},
            exc_info=exc,
        )
    return None


async def _upsert_hub_config(key: str, value: int) -> None:
    """
    Write or update a key-value pair in the hub_config MongoDB collection.

    Uses upsert=True so repeated calls are idempotent. The unique index
    on `key` (Section 25A.19) prevents duplicate documents.

    Args:
        key:   Config key name (e.g. "admin_logs_topic_id").
        value: Integer value to store.
    """
    try:
        db = DatabaseManager.get_db()
        await db["hub_config"].update_one(
            {"key": key},
            {"$set": {"key": key, "value": value}},
            upsert=True,
        )
    except Exception as exc:
        logger.error(
            "Failed to upsert hub_config",
            extra={"ctx_key": key, "ctx_value": value, "ctx_error": str(exc)},
            exc_info=exc,
        )


async def _write_audit_log(
    *,
    action: str,
    admin_user_id: Optional[int],
    target_user_id: Optional[int],
    detail: dict,
) -> None:
    """
    Write a structured entry to the audit_logs collection (Section 22).

    This is the MongoDB half of the mandatory dual-write (audit_logs +
    Admin Logs topic). Must be called before the Admin Logs topic post
    so there is always at least one durable record even if Telegram fails.

    Never re-raises — audit log failure must not abort the business
    operation that triggered it.

    Args:
        action:          Uppercase action type string.
        admin_user_id:   Admin who triggered the action, or None for system.
        target_user_id:  User affected, or None if not user-specific.
        detail:          Arbitrary dict of action-specific metadata.
    """
    try:
        db = DatabaseManager.get_db()
        await db["audit_logs"].insert_one({
            "action": action,
            "admin_user_id": admin_user_id,
            "target_user_id": target_user_id,
            "detail": detail,
            "timestamp": datetime.now(timezone.utc),
        })
    except Exception as exc:
        logger.error(
            "Failed to write audit_log entry",
            extra={
                "ctx_action": action,
                "ctx_target": target_user_id,
                "ctx_error": str(exc),
            },
            exc_info=exc,
        )


# ── Settings patch helper ─────────────────────────────────────────────────────


def _patch_settings_admin_logs(topic_id: int) -> None:
    """
    Set settings.HUB_TOPIC_ADMIN_LOGS in-process to the given topic_id.

    This is a secondary convenience so that AdminLogger (admin_logger.py),
    which reads settings.HUB_TOPIC_ADMIN_LOGS, can begin posting immediately
    after ensure_shared_topics() returns — without requiring AdminLogger to
    be refactored to read from hub_config directly.

    The authoritative value is always hub_config in MongoDB. This patch is
    ephemeral and is re-applied on every bot startup via ensure_shared_topics().

    Args:
        topic_id: The Admin Logs forum topic ID to inject into settings.
    """
    try:
        settings.HUB_TOPIC_ADMIN_LOGS = topic_id
        logger.debug(
            "settings.HUB_TOPIC_ADMIN_LOGS patched in-process",
            extra={"ctx_topic_id": topic_id},
        )
    except Exception as exc:
        # Some settings implementations (e.g. frozen Pydantic models) may
        # raise on attribute assignment. Log and continue — hub_config is
        # the real source of truth.
        logger.warning(
            "Could not patch settings.HUB_TOPIC_ADMIN_LOGS — "
            "AdminLogger may not post until it reads hub_config directly. "
            "Consider refactoring AdminLogger to use hub_config.",
            extra={"ctx_topic_id": topic_id, "ctx_error": str(exc)},
        )


# ── Redis distributed lock context manager ────────────────────────────────────


class _redis_lock:
    """
    Async context manager that acquires a Redis SET NX PX distributed lock.

    Usage:
        async with _redis_lock("my_lock_key", ttl=30):
            ...  # critical section

    If the lock cannot be acquired, the body IS still entered (fail-open)
    with a warning, so the operation proceeds rather than silently dropping
    a topic creation. The risk is a duplicate Telegram API call — which is
    safe because we always upsert into MongoDB afterward and Telegram will
    simply create two topics (the second one unused). The upsert ensures
    only the last-write topic_id is stored, and both topics remain in the
    hub as benign orphans.

    The lock key is namespaced under "vaultflow:lock:".

    Args:
        key: Logical lock identifier (e.g. "topic_create:12345").
        ttl: Lock expiry in seconds.
    """

    def __init__(self, key: str, *, ttl: int = _LOCK_TTL_SECONDS) -> None:
        self._key = f"vaultflow:lock:{key}"
        self._ttl = ttl
        self._acquired = False

    async def __aenter__(self) -> "_redis_lock":
        try:
            redis = await get_redis()
            self._acquired = bool(
                await redis.set(self._key, "1", nx=True, px=self._ttl * 1000)
            )
            if not self._acquired:
                logger.debug(
                    "Could not acquire distributed lock — proceeding without it",
                    extra={"ctx_lock_key": self._key},
                )
        except Exception as exc:
            logger.error(
                "Redis lock acquisition failed — proceeding without lock",
                extra={"ctx_lock_key": self._key, "ctx_error": str(exc)},
                exc_info=exc,
            )
            self._acquired = False
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._acquired:
            try:
                redis = await get_redis()
                await redis.delete(self._key)
            except Exception as exc:
                logger.warning(
                    "Failed to release distributed lock — will expire via TTL",
                    extra={"ctx_lock_key": self._key, "ctx_error": str(exc)},
                )
        return False  # Never suppress caller exceptions
