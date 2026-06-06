from __future__ import annotations

"""
support_monitor.py — Polling worker for 5-minute unattended support sessions.

Implements the Section 15.3 "5-Minute Unattended Rule":

  If a PENDING support session is not accepted within 5 minutes:
    → User receives the spec-exact notification text (Section 15.3).
    → Session status remains PENDING (admin may still accept).
    → Notification fires exactly once per session (notified_unattended flag).

Schema references:
  - support_sessions collection (Section 25A.8)
  - notified_unattended field    (Section 25A.8)
  - audit_logs collection        (Section 25A.17)
  - hub_config collection        (Section 25A.19)

Design notes:
  This monitor is a polling safety net. support_handler.py also schedules
  a per-session asyncio task (_unattended_check) that fires after 5 minutes.
  That per-session task is NOT restart-safe. This monitor catches sessions
  whose per-session task was lost on restart by scanning the DB directly.
  The notified_unattended flag prevents double-notification regardless of
  which path fires first.

  All Telegram calls handle FloodWait explicitly (Section 24).
  DB write precedes every Telegram send (Section 1 — restart safety).
  All events written to audit_logs AND Admin Logs topic (Section 22).
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

# How long to sleep between each DB scan. Configurable but not
# exposed to ENV for now — 60 seconds is a safe default that keeps
# the P99 notification latency under 2 minutes (5 min wait + 1 scan cycle).
_DEFAULT_POLL_INTERVAL_SECONDS = 60

# FloodWait extra buffer matches platform-wide convention.
_FLOOD_BUFFER = 1

# Maximum Telegram retry attempts per send call.
_MAX_SEND_RETRIES = 3


class SupportMonitor:
    """
    Background polling worker: detects PENDING support sessions unattended
    for more than 5 minutes and fires the Section 15.3 user notification.

    Usage:
        monitor = SupportMonitor(bot=client)
        monitor.start()          # schedules the background task
        ...
        await monitor.stop()     # graceful shutdown
    """

    def __init__(self, bot: Client, poll_interval: int = _DEFAULT_POLL_INTERVAL_SECONDS) -> None:
        """
        Initialise the monitor.

        Args:
            bot:           Active Pyrogram Client instance.
            poll_interval: Seconds between each DB scan. Default: 60.
        """
        self._bot = bot
        self._poll_interval = poll_interval
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # DB handle is fetched lazily inside the run loop — not here —
        # to avoid capturing a stale handle before Motor is connected.

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Schedule the polling loop as a background asyncio task.

        Safe to call multiple times — a running task is not restarted.
        """
        if self._running:
            logger.warning("SupportMonitor.start: already running — ignored")
            return
        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(),
            name="support_monitor_loop",
        )
        logger.info("SupportMonitor started", extra={"ctx_interval": self._poll_interval})

    async def stop(self) -> None:
        """
        Stop the polling loop and await task completion.

        Sets the running flag to False; the loop exits cleanly on the
        next sleep boundary. The task is then cancelled and awaited.
        """
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("SupportMonitor stopped")

    async def _run_loop(self) -> None:
        """
        Main polling loop. Runs `check_inactivity` every `_poll_interval`
        seconds until `stop()` is called.
        """
        while self._running:
            try:
                await self.check_inactivity()
            except Exception as exc:
                # Top-level catch: a bug in check_inactivity must not kill the loop.
                logger.error(
                    "SupportMonitor._run_loop: unhandled error in check_inactivity",
                    extra={"ctx_error": str(exc)},
                    exc_info=True,
                )
            await asyncio.sleep(self._poll_interval)

    # ── Core scan ─────────────────────────────────────────────────────────────

    async def check_inactivity(self) -> None:
        """
        Scan support_sessions for PENDING sessions older than 5 minutes
        that have not yet received the unattended notification.

        Per Section 25A.8, the collection is support_sessions and the
        once-per-session guard field is notified_unattended (bool).
        Per Section 25A.8, all statuses are uppercase strings.

        FIX (B-05): Use 'created_at' instead of 'opened_at' to match SupportService.

        For each matching session:
          1. Mark notified_unattended = True in MongoDB FIRST (restart-safe).
          2. Post admin alert to user's permanent topic in hub.
          3. Send user the exact Section 15.3 notification text.
          4. Write dual audit log (MongoDB + Admin Logs topic).
        """
        db = DatabaseManager.get_db()
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(minutes=5)

        try:
            # Query support_sessions (Section 25A.8) — correct collection,
            # correct field names, correct status casing.
            cursor = db["support_sessions"].find({
                "status": "PENDING",                         # uppercase per spec
                "created_at": {"$lte": threshold},           # created_at (B-05 FIX)
                "notified_unattended": {"$ne": True},        # notified_unattended not inactivity_warned
            })
        except Exception as exc:
            logger.error(
                "check_inactivity: cursor creation failed",
                extra={"ctx_error": str(exc)},
            )
            return

        async for session in cursor:
            user_id: int = session["user_id"]
            topic_id: int = session["topic_id"]
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
        """
        Handle a single unattended session: mark flag, notify admin topic,
        notify user, write audit log.

        Restart-safety invariant:
          notified_unattended is set to True in MongoDB BEFORE any Telegram
          message is sent. If the bot crashes mid-way, the session is already
          flagged and will not be double-processed on restart (Section 1, §3).

        Args:
            db:         Motor database handle.
            session:    The support_sessions document dict.
            user_id:    Telegram user ID.
            topic_id:   The user's permanent hub topic ID (from session doc).
            session_id: String ObjectId of the session.
            now:        Current UTC datetime for log timestamps.
        """
        try:
            # ── Step 1: Mark notified_unattended = True BEFORE any Telegram send ──
            # This is the restart-safety write. If we crash after this line,
            # the scan will skip this session on the next cycle.
            update_result = await db["support_sessions"].update_one(
                {
                    "_id": session["_id"],
                    "status": "PENDING",
                    "notified_unattended": {"$ne": True},  # idempotency guard
                },
                {"$set": {"notified_unattended": True}},
            )

            if update_result.modified_count == 0:
                # Another process already flagged this session (race condition
                # or stale cursor read) — skip to avoid double-notification.
                logger.info(
                    "_process_unattended_session: already flagged — skipping",
                    extra={"ctx_session_id": session_id, "ctx_user_id": user_id},
                )
                return

            hub_cfg = get_hub_config()
            hub_id: Optional[int] = hub_cfg.get("hub_supergroup_id")

            # ── Step 2: Post admin alert to user's permanent hub topic ─────────
            if hub_id:
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
            else:
                logger.warning(
                    "_process_unattended_session: hub_supergroup_id not in hub_config",
                    extra={"ctx_session_id": session_id},
                )

            # ── Step 3: Notify user — exact text from Section 15.3 ────────────
            user_text = (
                "ℹ️ No admin available currently.\n"
                "Your request has been noted. "
                "An admin will respond when available."
            )
            try:
                await self._send_with_retry(chat_id=user_id, text=user_text)
            except PeerIdInvalid:
                logger.warning(
                    "_process_unattended_session: user unreachable for DM",
                    extra={"ctx_user_id": user_id},
                )
            except Exception as exc:
                logger.warning(
                    "_process_unattended_session: user DM failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
                )

            # ── Step 4: Dual audit log — MongoDB + Admin Logs topic ────────────
            await self._write_audit_log(
                db=db,
                user_id=user_id,
                session_id=session_id,
                now=now,
            )

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

    # ── Telegram helper ───────────────────────────────────────────────────────

    async def _send_with_retry(
        self,
        chat_id: int,
        text: str,
        thread_id: Optional[int] = None,
    ) -> None:
        """
        Send a Telegram message with explicit FloodWait handling (Section 24).

        FloodWait is slept and retried without consuming an attempt slot.
        RPCErrors consume an attempt slot with exponential backoff.
        Raises the last exception after _MAX_SEND_RETRIES RPCError failures.

        Args:
            chat_id:   Destination chat or user ID.
            text:      HTML-formatted message text.
            thread_id: Optional forum topic thread ID.
        """
        attempt = 0
        while attempt < _MAX_SEND_RETRIES:
            try:
                kwargs: dict = {"parse_mode": ParseMode.HTML}
                if thread_id:
                    kwargs["message_thread_id"] = thread_id
                await self._bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    **kwargs,
                )
                return
            except FloodWait as fw:
                # FloodWait: sleep the required time and retry without
                # incrementing attempt — not a permanent failure.
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

    # ── Audit helper ──────────────────────────────────────────────────────────

    @staticmethod
    async def _write_audit_log(
        db,
        user_id: int,
        session_id: str,
        now: datetime,
    ) -> None:
        """
        Write a record to audit_logs collection (Section 25A.17 / Section 22).

        Failures are caught and logged — an audit write failure must never
        abort the primary notification flow.

        Args:
            db:         Motor database handle.
            user_id:    Target user ID.
            session_id: String ObjectId of the support session.
            now:        Timestamp to use for the audit record.
        """
        try:
            await db["audit_logs"].insert_one({
                "action": "SUPPORT_UNATTENDED_NOTIFIED",
                "admin_user_id": None,           # system-triggered
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
