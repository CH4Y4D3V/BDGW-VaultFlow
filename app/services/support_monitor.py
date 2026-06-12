from __future__ import annotations

"""
support_monitor.py — Polling worker for 5-minute unattended support sessions.

FIX B-05: Query now uses `created_at` (the field actually written when a
support session is created) instead of `opened_at` (which does not exist),
so the 5-minute unattended notification fires correctly.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, PeerIdInvalid, RPCError

from app.core.database import DatabaseManager
from app.core.hub_config import get_hub_config
from app.utils.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_POLL_INTERVAL_SECONDS = 60
_FLOOD_BUFFER = 1
_MAX_SEND_RETRIES = 3


class SupportMonitor:
    """
    Background polling worker: detects PENDING support sessions unattended
    for more than 5 minutes and fires the Section 15.3 user notification.
    """

    def __init__(
        self,
        bot: Client,
        poll_interval: int = _DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        self._bot = bot
        self._poll_interval = poll_interval
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            logger.warning("SupportMonitor.start: already running — ignored")
            return
        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(), name="support_monitor_loop"
        )
        logger.info("SupportMonitor started", extra={"ctx_interval": self._poll_interval})

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("SupportMonitor stopped")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.check_inactivity()
            except Exception as exc:
                logger.error(
                    "SupportMonitor._run_loop: unhandled error",
                    extra={"ctx_error": str(exc)},
                    exc_info=True,
                )
            await asyncio.sleep(self._poll_interval)

    # ── Core scan ──────────────────────────────────────────────────────────

    async def check_inactivity(self) -> None:
        """
        Scan support_sessions for PENDING sessions older than 5 minutes
        that have not yet received the unattended notification.

        FIX B-05: Uses `created_at` field (what is actually written to DB)
        instead of the non-existent `opened_at` field.
        """
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(minutes=5)

        try:
            # FIX: was `"opened_at": {"$lte": threshold}` — field does not exist.
            # Support sessions are inserted with `created_at`, not `opened_at`.
            cursor = db["support_sessions"].find({
                "status": "PENDING",
                "created_at": {"$lte": threshold},          # FIX B-05
                "notified_unattended": {"$ne": True},
            })
        except Exception as exc:
            logger.error(
                "check_inactivity: cursor creation failed",
                extra={"ctx_error": str(exc)},
            )
            return

        async for session in cursor:
            user_id: int = session["user_id"]
            topic_id: int = session.get("topic_id", 0)
            session_id: str = str(session["_id"])

            logger.info(
                "support_session_unattended",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_topic_id": topic_id,
                    "ctx_session_id": session_id,
                },
            )

            await self._process_unattended_session(
                db=db,
                session=session,
                user_id=user_id,
                topic_id=topic_id,
                session_id=session_id,
                now=now,
            )

    async def _process_unattended_session(
        self,
        db,
        session: dict,
        user_id: int,
        topic_id: int,
        session_id: str,
        now: datetime,
    ) -> None:
        try:
            # Mark notified BEFORE any Telegram call (restart-safe)
            update_result = await db["support_sessions"].update_one(
                {
                    "_id": session["_id"],
                    "status": "PENDING",
                    "notified_unattended": {"$ne": True},
                },
                {"$set": {"notified_unattended": True}},
            )

            if update_result.modified_count == 0:
                return  # Already flagged by concurrent process

            hub_cfg = get_hub_config()
            hub_id: Optional[int] = hub_cfg.get("hub_supergroup_id")

            # Alert admin in hub topic
            if hub_id and topic_id:
                hub_alert = (
                    "🚨 <b>SUPPORT UNATTENDED</b>\n\n"
                    f"User <code>{user_id}</code> has been waiting over 5 minutes.\n"
                    "No admin has accepted the session.\n\n"
                    "Please click <b>✅ Accept Support</b> to assist the user."
                )
                await self._send_with_retry(
                    chat_id=hub_id,
                    text=hub_alert,
                    thread_id=topic_id,
                )

            # Notify user (exact §15.3 text)
            user_text = (
                "ℹ️ No admin available currently.\n"
                "Your request has been noted. "
                "An admin will respond when available."
            )
            try:
                await self._send_with_retry(chat_id=user_id, text=user_text)
            except (PeerIdInvalid, Exception) as exc:
                logger.warning(
                    "_process_unattended_session: user DM failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                )

            # Dual audit log
            await self._write_audit_log(db=db, user_id=user_id, session_id=session_id, now=now)

            # Admin Logs topic entry
            if hub_id:
                logs_topic_id: Optional[int] = hub_cfg.get("admin_logs_topic_id")
                if logs_topic_id:
                    log_text = (
                        "<b>[SUPPORT UNATTENDED NOTIFIED]</b>\n"
                        f"Admin ID  : System\n"
                        f"Target ID : {user_id}\n"
                        f"Session   : {session_id}\n"
                        f"Time      : {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    )
                    await self._send_with_retry(
                        chat_id=hub_id,
                        text=log_text,
                        thread_id=logs_topic_id,
                    )

        except Exception as exc:
            logger.error(
                "_process_unattended_session: failed",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_session_id": session_id,
                    "ctx_error": str(exc),
                },
                exc_info=True,
            )

    async def _send_with_retry(
        self,
        chat_id: int,
        text: str,
        thread_id: Optional[int] = None,
    ) -> None:
        attempt = 0
        while attempt < _MAX_SEND_RETRIES:
            try:
                kwargs: dict = {"parse_mode": ParseMode.HTML}
                if thread_id:
                    kwargs["message_thread_id"] = thread_id
                await self._bot.send_message(chat_id=chat_id, text=text, **kwargs)
                return
            except FloodWait as fw:
                wait = int(fw.value) + _FLOOD_BUFFER
                logger.info(
                    "_send_with_retry: FloodWait",
                    extra={"ctx_chat_id": chat_id, "ctx_wait": wait},
                )
                await asyncio.sleep(wait)
            except RPCError as exc:
                attempt += 1
                logger.warning(
                    "_send_with_retry: RPCError",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_error": str(exc),
                        "ctx_attempt": attempt,
                    },
                )
                if attempt >= _MAX_SEND_RETRIES:
                    raise
                await asyncio.sleep(2 ** (attempt - 1))

    @staticmethod
    async def _write_audit_log(
        db,
        user_id: int,
        session_id: str,
        now: datetime,
    ) -> None:
        try:
            await db["audit_logs"].insert_one({
                "action": "SUPPORT_UNATTENDED_NOTIFIED",
                "admin_user_id": None,
                "target_user_id": user_id,
                "detail": {"session_id": session_id},
                "timestamp": now,
            })
        except Exception as exc:
            logger.warning(
                "_write_audit_log: failed",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_session_id": session_id,
                    "ctx_error": str(exc),
                },
            )
