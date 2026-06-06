"""
app/services/support_service.py

Support System — Section 15 (BDGW VaultFlow Master Reference v1.0)

Responsibilities:
  - Route user DM messages to the user's permanent Verification Hub topic.
  - Build the spec-compliant Section 15.2 support request card.
  - Send structured entries to the Admin Logs topic (Section 9.4).
  - Notify a user's topic with arbitrary text (used by open/close flows).

Design invariants enforced here:
  1. Every Telegram call is wrapped in FloodWait-aware retry logic.
  2. Every exception is caught and logged — no silent failures.
  3. Every admin action emits to both audit_logs AND the Admin Logs topic.
  4. All user routing goes through topic_manager.get_or_create_user_topic().
  5. The hub_config lookup is cached after first load so sweeps do not
     hammer MongoDB; a fresh load is forced when cache is None.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from html import escape
from typing import Optional

from pyrogram.client import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.config import settings
from app.core.database import DatabaseManager
from app.services.topic_manager import get_topic_manager
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Hub-config cache (populated lazily; restart-safe because hub_config lives
# in MongoDB, not in memory).
# ---------------------------------------------------------------------------

_hub_config_cache: Optional[dict] = None


async def _load_hub_config(db) -> dict:
    """
    Load all hub_config key-value pairs into a single dict.

    Results are cached module-wide. Call invalidate_hub_config_cache() if
    hub_config changes at runtime (very rare).
    """
    global _hub_config_cache
    if _hub_config_cache is None:
        docs = await db["hub_config"].find({}).to_list(length=None)
        _hub_config_cache = {doc["key"]: doc["value"] for doc in docs}
    return _hub_config_cache


def invalidate_hub_config_cache() -> None:
    """Force a fresh hub_config load on the next call to _load_hub_config."""
    global _hub_config_cache
    _hub_config_cache = None


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------

async def _tg_retry(coro_factory, max_attempts: int = 3):
    """
    Execute a coroutine factory with FloodWait-aware retry.

    Args:
        coro_factory: Zero-argument callable that returns an awaitable.
        max_attempts: Maximum attempts before the last exception is re-raised.

    Returns:
        The result of the first successful execution.

    Raises:
        The exception from the final attempt if all attempts fail.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            return await coro_factory()
        except FloodWait as fw:
            last_exc = fw
            if attempt == max_attempts - 1:
                break
            wait_secs = fw.value + 1
            logger.warning(
                "tg_flood_wait",
                extra={"ctx_wait": wait_secs, "ctx_attempt": attempt + 1},
            )
            await asyncio.sleep(wait_secs)
        except RPCError as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                break
            await asyncio.sleep(2 ** attempt)
        except Exception as exc:
            # Non-Telegram exceptions: raise immediately — don't retry.
            raise exc
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Admin Logs topic writer  (Section 9.4)
# ---------------------------------------------------------------------------

async def send_admin_log_entry(
    client: Client,
    action_type: str,
    admin_user_id: Optional[int],
    admin_name: Optional[str],
    target_user_id: Optional[int],
    target_name: Optional[str],
    target_username: Optional[str],
    detail: str,
) -> None:
    """
    Write a structured entry to the Admin Logs topic in the Verification Hub.

    Per Section 9.4, EVERY admin action must produce one of these entries.
    Failures are logged but never raised so the caller's primary operation
    is not aborted.

    Args:
        client:           Active Pyrogram client.
        action_type:      e.g. "SUPPORT ACCEPTED", "SUPPORT CLOSED".
        admin_user_id:    ID of the acting admin (None if system-triggered).
        admin_name:       Display name of the acting admin.
        target_user_id:   ID of the user being acted upon.
        target_name:      Display name of the target user.
        target_username:  Telegram username of the target (without @).
        detail:           Action-specific free-text description.
    """
    db = DatabaseManager.get_db()
    try:
        hub_config = await _load_hub_config(db)
        topic_id = hub_config.get("admin_logs_topic_id")
        if not topic_id:
            logger.warning(
                "admin_logs_topic_id_missing",
                extra={"ctx_action": action_type},
            )
            return

        safe_admin_name = escape(admin_name or "System")
        safe_target_name = escape(target_name or "Unknown")
        target_uname_str = (
            f"(@{escape(target_username)})" if target_username else "(no username)"
        )
        timestamp = datetime.now(timezone.utc).isoformat()

        text = (
            f"<b>[{escape(action_type)}]</b>\n"
            f"Admin     : {safe_admin_name}\n"
            f"Admin ID  : {admin_user_id or 'N/A'}\n"
            f"Target    : {safe_target_name} {target_uname_str}\n"
            f"Target ID : {target_user_id or 'N/A'}\n"
            f"Detail    : {escape(detail)}\n"
            f"Time      : {timestamp}"
        )

        await _tg_retry(
            lambda: client.send_message(
                chat_id=settings.VERIFICATION_GROUP_ID,
                text=text,
                message_thread_id=topic_id,
                parse_mode=ParseMode.HTML,
            )
        )
    except Exception as exc:
        logger.error(
            "admin_log_entry_failed",
            extra={"ctx_action": action_type, "ctx_error": str(exc)},
        )


# ---------------------------------------------------------------------------
# Accept markup
# ---------------------------------------------------------------------------

def build_accept_markup(user_id: int) -> InlineKeyboardMarkup:
    """
    Build the inline keyboard shown on a PENDING support request card.

    Args:
        user_id: Telegram user ID of the user who opened the support session.

    Returns:
        InlineKeyboardMarkup with a single "✅ Accept Support" button.
    """
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            text="✅ Accept Support",
            callback_data=f"support_accept:{user_id}",
        )
    ]])


# ---------------------------------------------------------------------------
# SupportService
# ---------------------------------------------------------------------------

class SupportService:
    """
    Core support routing service.

    Handles:
      - Forwarding user DM messages to their permanent Verification Hub topic.
      - Building the spec-compliant (Section 15.2) support request card.
      - Sending arbitrary notifications to a user's topic.

    All methods are idempotent and restart-safe; they never depend on
    in-memory state that would not survive a bot restart.
    """

    def __init__(self) -> None:
        """Initialise with a reference to the shared topic manager."""
        self.topic_manager = get_topic_manager()

    # ------------------------------------------------------------------
    # Card builder — Section 15.2
    # ------------------------------------------------------------------

    async def build_user_support_card(
        self,
        db,
        user_id: int,
        from_user,
        message: Optional[Message] = None,
    ) -> str:
        """
        Build the full HTML support request card as specified in Section 15.2.

        Queries: users, subscriptions, referrals, content_submissions.

        Args:
            db:        Motor database handle.
            user_id:   Telegram user ID of the requester.
            from_user: Pyrogram User object (provides name and username).
            message:   Optional originating Message for its text preview.

        Returns:
            HTML-formatted string ready to send to the hub topic.
        """
        user_link = f"tg://user?id={user_id}"
        safe_name = escape(
            (from_user.full_name if hasattr(from_user, "full_name") else None)
            or "Unknown"
        )
        raw_username = (
            from_user.username if hasattr(from_user, "username") else None
        )
        safe_username = f"@{escape(raw_username)}" if raw_username else "no username"

        # -- Pull user document -----------------------------------------
        user_doc = await db["users"].find_one({"user_id": user_id}) or {}

        join_date = user_doc.get("join_date")
        join_date_str = (
            join_date.strftime("%Y-%m-%d") if isinstance(join_date, datetime)
            else str(join_date) if join_date
            else "Unknown"
        )
        warnings_count = user_doc.get("warnings", 0)
        is_muted = user_doc.get("is_muted", False)
        is_banned = user_doc.get("is_banned", False)

        # -- Premium status --------------------------------------------
        now = datetime.now(timezone.utc)
        active_sub = await db["subscriptions"].find_one({
            "user_id": user_id,
            "status": "ACTIVE",
            "expires_at": {"$gt": now},
        })
        if active_sub:
            premium_str = "Yes"
        else:
            expired_sub = await db["subscriptions"].find_one({
                "user_id": user_id,
                "status": "EXPIRED",
            })
            premium_str = "Expired" if expired_sub else "No"

        # -- Referral count --------------------------------------------
        referral_count = await db["referrals"].count_documents(
            {"referrer_user_id": user_id}
        )

        # -- Submission counts -----------------------------------------
        total_subs = await db["content_submissions"].count_documents(
            {"user_id": user_id}
        )
        approved_subs = await db["content_submissions"].count_documents(
            {"user_id": user_id, "status": {"$in": ["APPROVED_NSFW", "APPROVED_PREMIUM"]}}
        )
        rejected_subs = await db["content_submissions"].count_documents(
            {"user_id": user_id, "status": "REJECTED"}
        )

        # -- Message preview -------------------------------------------
        if message is not None:
            msg_text = message.text or message.caption or "[media]"
            if len(msg_text) > 200:
                msg_text = msg_text[:197] + "..."
        else:
            msg_text = "[no message]"

        return (
            f"🆘 <b>SUPPORT REQUEST</b>\n\n"
            f"<b>From</b>       : <a href='{user_link}'>{safe_name}</a>"
            f" ({safe_username})\n"
            f"<b>User ID</b>    : <code>{user_id}</code>\n"
            f"<b>Join Date</b>  : {join_date_str}\n"
            f"<b>Premium</b>    : {premium_str}\n"
            f"<b>Warnings</b>   : {warnings_count}\n"
            f"<b>Muted</b>      : {'Yes' if is_muted else 'No'}\n"
            f"<b>Banned</b>     : {'Yes' if is_banned else 'No'}\n"
            f"<b>Referrals</b>  : {referral_count}\n"
            f"<b>Submissions</b>: {total_subs} total / "
            f"{approved_subs} approved / {rejected_subs} rejected\n"
            f"<b>Message</b>    : {escape(msg_text)}"
        )

    # ------------------------------------------------------------------
    # Message bridge — Section 15.4
    # ------------------------------------------------------------------

    async def handle_user_message(self, client: Client, message: Message) -> bool:
        """
        Route a user's DM to their permanent Verification Hub topic.

        Steps:
          1. Resolve (or lazily create) the user's permanent topic.
          2. Copy the raw message into the topic.
          3. Log the message to the support_messages collection.
          4. Log the message to the cleanup_service for user-side deletion
             on session closure (Section 20, Rule 5).

        Returns:
            True on success, False if an unrecoverable error occurred.
        """
        user_id = message.from_user.id
        try:
            # A-16 FIX: Enforce support session state machine
            db = DatabaseManager.get_db()
            session_doc = await db["support_sessions"].find_one({"user_id": user_id}, sort=[("created_at", -1)])

            if session_doc and session_doc.get("status") == "CLOSED":
                await message.reply_text(
                    "Your previous support session is closed. "
                    "To open a new one, please use the /help command again."
                )
                return True

            topic_id = await self.topic_manager.get_or_create_user_topic(
                client, user_id
            )

            # Forward raw message to topic --------------------------------
            await _tg_retry(
                lambda: client.copy_message(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    from_chat_id=message.chat.id,
                    message_id=message.id,
                    message_thread_id=topic_id,
                )
            )

            # Persist to support_messages ---------------------------------
            db = DatabaseManager.get_db()
            await db["support_messages"].insert_one({
                "user_id": user_id,
                "topic_id": topic_id,
                "user_message_id": message.id,
                "direction": "user_to_admin",
                "created_at": datetime.now(timezone.utc),
            })

            # Register for user-side cleanup on session closure -----------
            # Import here to avoid circular dependency at module load time.
            try:
                from app.services.cleanup_service import get_cleanup_service
                cleanup = get_cleanup_service()
                await cleanup.log_support_message(
                    user_id=user_id,
                    message_id=message.id,
                    chat_id=user_id,  # DM chat_id == user_id
                )
            except Exception as ce:
                logger.warning(
                    "support_cleanup_log_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(ce)},
                )

            # Audit -------------------------------------------------------
            try:
                from app.services.audit_service import get_audit
                await get_audit().log(
                    action="SUPPORT_MESSAGE",
                    performed_by=user_id,
                    target_user_id=user_id,
                )
            except Exception as ae:
                logger.warning(
                    "support_audit_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(ae)},
                )

            return True

        except Exception as exc:
            logger.error(
                "support_handle_user_message_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                exc_info=True,
            )
            return False

    # ------------------------------------------------------------------
    # Generic topic notification
    # ------------------------------------------------------------------

    async def notify_to_topic(
        self,
        client: Client,
        user_id: int,
        text: str,
        reply_markup: Optional[InlineKeyboardMarkup] = None,
        **kwargs,
    ) -> Optional[Message]:
        """
        Send an arbitrary notification to a user's permanent Verification Hub
        topic. Used for support request cards, system alerts, etc.

        Args:
            client:        Active Pyrogram client.
            user_id:       Target user's Telegram ID.
            text:          HTML-formatted text to send.
            reply_markup:  Optional inline keyboard to attach.

        Returns:
            The sent Message object, or None on failure.
        """
        try:
            topic_id = await self.topic_manager.get_or_create_user_topic(
                client, user_id
            )
            sent: Message = await _tg_retry(
                lambda: client.send_message(
                    chat_id=settings.VERIFICATION_GROUP_ID,
                    text=text,
                    reply_markup=reply_markup,
                    message_thread_id=topic_id,
                    parse_mode=ParseMode.HTML,
                )
            )
            return sent
        except Exception as exc:
            logger.error(
                "support_notify_to_topic_failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )
            return None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_support_service: Optional[SupportService] = None


def get_support_service() -> SupportService:
    """
    Return the module-level SupportService singleton.

    Initialised on first call; subsequent calls return the same instance.
    """
    global _support_service
    if _support_service is None:
        _support_service = SupportService()
    return _support_service
