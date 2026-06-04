"""
Bootstrap — historical channel scan.

Scans from the LATEST message in the source channel backwards and stops
when it reaches a message with ID < START_MESSAGE_ID.  Messages are then
sorted ascending (oldest first) and inserted into the queue so they are
sent in the correct chronological order.

Requires USER_SESSION_STRING (a userbot session, not the bot token) because
bots cannot call get_chat_history on channels they are a member of via
the Bot API — only user accounts can read channel history.

Race-condition fix (RC-2):
  The old skip guard checked `last_processed_message_id > START_MESSAGE_ID`.
  This misfired whenever a live message arrived during peer warm-up and
  advanced the cursor before bootstrap ran — causing the scan to be skipped
  on every restart.

  The new guard uses a dedicated `scan_completed` boolean stored in the state
  document.  It is only set to True AFTER the scan finishes successfully.
  Live messages advancing the cursor can never trigger a false skip.
"""

from pyrogram import Client
from loguru import logger
from app.utils.config import settings
from app.database.repositories import state_repo
from app.services.queue_manager import queue_manager

# Number of messages to insert before emitting a progress log
_PROGRESS_LOG_INTERVAL = 100


async def _run_historical_scan_with_userbot() -> None:
    logger.info("Starting historical scan using userbot session…")

    userbot = Client(
        name="userbot_scanner",
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
        session_string=settings.USER_SESSION_STRING.get_secret_value(),
        in_memory=True,
    )

    try:
        await userbot.start()
        logger.info("Userbot client started for historical scan.")

        try:
            chat = await userbot.get_chat(settings.SOURCE_CHANNEL_ID)
            logger.info(
                f"Source channel resolved via userbot: '{chat.title}' "
                f"(id={chat.id})"
            )
        except Exception as exc:
            raise RuntimeError(
                f"Userbot cannot resolve source channel "
                f"{settings.SOURCE_CHANNEL_ID}: {exc}. "
                f"Ensure the user account is a member of that channel."
            )

        logger.info(
            f"Scanning channel history. Collecting messages with ID >= "
            f"{settings.START_MESSAGE_ID}…"
        )

        collected: list = []
        skipped = 0

        async for message in userbot.get_chat_history(
            settings.SOURCE_CHANNEL_ID,
            offset_id=0,
        ):
            if message.id < settings.START_MESSAGE_ID:
                logger.info(
                    f"Reached message {message.id} < START_MESSAGE_ID "
                    f"({settings.START_MESSAGE_ID}). Stopping scan."
                )
                break

            if message.service:
                skipped += 1
                continue

            collected.append(message)

            if len(collected) % 200 == 0:
                logger.info(
                    f"Collected {len(collected)} messages so far "
                    f"(current ID={message.id})…"
                )

        if not collected:
            logger.warning(
                "Historical scan: no messages found in the configured range. "
                f"START_MESSAGE_ID={settings.START_MESSAGE_ID}"
            )
            # Mark completed even on empty result so we don't retry forever.
            await state_repo.mark_scan_completed()
            return

        # Sort ascending (oldest first) so the queue preserves posting order
        collected.sort(key=lambda m: m.id)

        logger.info(
            f"Collected {len(collected)} messages "
            f"(skipped {skipped} service messages). "
            f"ID range: {collected[0].id} – {collected[-1].id}. "
            f"Inserting into queue…"
        )

        queued = 0
        for message in collected:
            await queue_manager.add_message_to_queue(message)
            queued += 1

            if queued % _PROGRESS_LOG_INTERVAL == 0:
                logger.info(
                    f"Queue progress: {queued}/{len(collected)}, "
                    f"current_id={message.id}"
                )

        last_id = collected[-1].id

        # Use $max so we never roll back the cursor if live handlers already
        # advanced it beyond last_id.
        await state_repo.update_state_safe(last_processed_id=last_id)

        # ── RC-2 fix: mark scan as completed ONLY after successful finish ──
        # This boolean is the sole skip guard on next restart.  Live messages
        # advancing last_processed_message_id can never cause a false skip.
        await state_repo.mark_scan_completed()

        logger.success(
            f"Historical scan complete. "
            f"queued={queued}, skipped={skipped}, last_id={last_id}"
        )

    finally:
        try:
            await userbot.stop()
            logger.info("Userbot client stopped.")
        except Exception:
            pass


async def initial_channel_scan(client: Client) -> None:
    """
    Run the historical scan on first startup.

    Skip logic (RC-2 fix):
      - Skip ONLY if state.scan_completed is True.
      - last_processed_message_id is intentionally NOT used as the skip guard
        because live messages can advance it before this function runs,
        causing a false skip on every restart.

    The `client` parameter is kept for API compatibility but is not used —
    the scan uses a separate userbot client internally.
    """
    state = await state_repo.get_state()

    # ── RC-2: guard on scan_completed flag, NOT on the cursor value ──────────
    if state is not None and state.scan_completed:
        logger.info(
            f"Historical scan already completed "
            f"(last_processed_message_id={state.last_processed_message_id}). "
            f"Skipping."
        )
        return

    if not settings.USER_SESSION_STRING:
        logger.warning(
            "USER_SESSION_STRING not set. Historical scan skipped. "
            "Only new messages arriving after bot startup will be queued."
        )
        if state is None:
            await state_repo.update_state_safe(
                last_processed_id=settings.START_MESSAGE_ID
            )
        # Mark completed so we don't warn on every restart.
        await state_repo.mark_scan_completed()
        return

    try:
        await _run_historical_scan_with_userbot()
    except Exception as exc:
        logger.error(
            f"Historical scan failed: {exc}. "
            f"Bot continues — live messages will still be queued normally.",
            exc_info=True,
        )
        # Do NOT mark scan_completed on failure — next restart will retry.
