"""
app/handlers/user_handler.py
────────────────────────────────────────────────────────────────────────────
User-facing command and callback handlers for BDGW VaultFlow.

Handlers covered here
  /start      — new-user registration + onboarding OR returning-user menu
  /rules      — community rules (DM delivery)
  /mystatus   — user account dashboard
  /ping       — health-check (dev/ops use only)

  Callbacks
    onboarding:accept_terms  — terms acceptance flow
    menu:<action>            — main-menu navigation

Spec coverage
  Section 4   (command behaviour)
  Section 5   (first-time onboarding — shown once, DB written first)
  Section 6   (main menu layout)
  Section 16  (referral — credited ONLY after referred user joins
               MAIN_CHANNEL_ID, not immediately on /start)
  Section 17  (user status dashboard)
  Section 25  (restart safety — DB write precedes any Telegram send)
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    FloodWait,
    RPCError,
    UserIsBlocked,
    PeerIdInvalid,
    InputUserDeactivated,
    MessageNotModified,
    UserNotParticipant,
    ChatAdminRequired,
)
from pyrogram.types import (
    CallbackQuery,
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from app.config import settings
from app.core.database import DatabaseManager
from app.core.redis_client import get_redis
from app.repositories.subscription_repository import SubscriptionRepository
from app.repositories.queue_repository import QueueRepository
from app.services.onboarding_service import OnboardingService
from app.bot.keyboards import KeyboardBuilder
from app.services.subscription_service import SubscriptionService
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────
_MAX_RETRIES: int = 3

# ── Module-level cache ────────────────────────────────────────────────────
_bot_username: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
#  Lazy service/repository getters
#  (no module-level instantiation — DB must be connected first)
# ═══════════════════════════════════════════════════════════════════════════

def _flood_buffer() -> int:
    """Return the extra seconds added on top of a FloodWait value."""
    return getattr(settings, "FLOODWAIT_EXTRA_BUFFER", 2)


def _get_sub_repo() -> SubscriptionRepository:
    """Return a SubscriptionRepository bound to the active Motor database."""
    return SubscriptionRepository()


def _get_queue_repo() -> QueueRepository:
    """Return a QueueRepository bound to the active Motor database."""
    return QueueRepository(DatabaseManager.get_db())


def _get_onboarding_service() -> OnboardingService:
    """Return an OnboardingService with fully-wired dependencies."""
    from app.repositories.user_repository import UserRepository
    return OnboardingService(_get_sub_repo(), UserRepository())


def _get_sub_service() -> SubscriptionService:
    """Return a SubscriptionService instance."""
    return SubscriptionService()


# ═══════════════════════════════════════════════════════════════════════════
#  Hub config helper
# ═══════════════════════════════════════════════════════════════════════════

async def _get_hub_config_value(key: str) -> Optional[int]:
    """
    Look up a key in the hub_config collection (Section 25A.19).

    Falls back to an identically-named attribute on settings if the DB
    lookup fails or the key is absent.  Returns None if neither source
    has the value.

    Never raises.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["hub_config"].find_one({"key": key})
        if doc and doc.get("value") is not None:
            return int(doc["value"])
    except Exception as e:
        logger.warning(
            "hub_config_lookup_failed",
            extra={"ctx_key": key, "ctx_error": str(e)},
        )
    # Fallback: settings attribute with the same name (e.g. MAIN_CHANNEL_ID)
    return getattr(settings, key.upper(), None)


# ═══════════════════════════════════════════════════════════════════════════
#  Telegram helpers
# ═══════════════════════════════════════════════════════════════════════════

async def _get_bot_username(client: Client) -> str:
    """
    Return the bot's Telegram username, caching the result after the first
    successful lookup to avoid repeated get_me() API calls.

    Returns an empty string on failure; callers must handle that case.
    """
    global _bot_username
    if _bot_username is None:
        try:
            me = await client.get_me()
            _bot_username = me.username or ""
        except Exception as e:
            logger.exception(
                "bot_username_lookup_failed",
                extra={"ctx_error": str(e)},
            )
            _bot_username = ""
    return _bot_username


async def _send_private(
    client: Client,
    user_id: int,
    text: str,
    reply_markup=None,
) -> bool:
    """
    Send a DM to `user_id` with FloodWait handling and exponential back-off.

    Returns True on success, False if the user has blocked the bot or the
    peer is invalid / deactivated, or if all retries are exhausted.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            await client.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
            return True
        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            return False
        except FloodWait as e:
            wait = int(e.value) + _flood_buffer()
            logger.warning(
                "FloodWait on DM send",
                extra={"ctx_user_id": user_id, "ctx_wait": wait},
            )
            await asyncio.sleep(wait)
        except RPCError as e:
            logger.warning(
                "DM delivery RPC error",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_error": str(e),
                    "ctx_attempt": attempt + 1,
                },
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
    return False


async def _reply_floodwait(
    message: Message,
    text: str,
    reply_markup=None,
) -> Optional[Message]:
    """
    Reply to a Message with FloodWait handling.

    Returns the sent Message object on success, None on unrecoverable failure.
    Handles FloodWait explicitly per spec requirement (Section 24).
    """
    for attempt in range(_MAX_RETRIES):
        try:
            return await message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
        except FloodWait as e:
            wait = int(e.value) + _flood_buffer()
            logger.warning(
                "FloodWait on reply",
                extra={"ctx_wait": wait, "ctx_attempt": attempt + 1},
            )
            await asyncio.sleep(wait)
        except (UserIsBlocked, PeerIdInvalid, InputUserDeactivated):
            return None
        except RPCError as e:
            logger.warning(
                "Reply RPC error",
                extra={"ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_RETRIES - 1:
                return None
            await asyncio.sleep(2 ** attempt)
    return None


async def _cleanup_messages(*messages: Optional[Message], delay: float = 10.0) -> None:
    """
    Delete one or more messages after `delay` seconds.  Ignores failures
    silently (message may already be deleted).
    """
    await asyncio.sleep(delay)
    for msg in messages:
        if msg is None:
            continue
        try:
            await msg.delete()
        except Exception:
            pass


async def _delete_after(message: Message, delay: float = 10.0) -> None:
    """Delete a single message after `delay` seconds.  Ignores failures."""
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass


async def _ack_in_group(
    client: Client,
    message: Message,
    ack_text: str,
    dm_sent: bool,
) -> None:
    """
    Post a short acknowledgement reply in a group chat.

    If the DM was delivered, post `ack_text` and auto-delete after 10 s.
    If the DM was blocked, instruct the user to start the bot first.
    """
    if dm_sent:
        try:
            ack = await message.reply_text(ack_text, parse_mode=ParseMode.HTML)
            asyncio.create_task(_cleanup_messages(ack, message, delay=10.0))
        except Exception:
            pass
    else:
        try:
            bot_username = await _get_bot_username(client)
            link = f"https://t.me/{bot_username}" if bot_username else "the bot"
            await message.reply_text(
                f"⚠️ I couldn't send you a DM. Please "
                f"<a href='{link}'>start the bot</a> first, then try again.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
#  Redis spam-guard
# ═══════════════════════════════════════════════════════════════════════════

async def _check_spam_guard(key: str, ttl_seconds: int) -> bool:
    """
    Rate-limit guard backed by Redis.

    Returns True if the caller is within the cooldown window (should be
    blocked).  Returns False (allow) if Redis is unavailable — the guard
    is fault-tolerant so a Redis outage never blocks legitimate users.
    """
    try:
        from app.core.redis_client import RedisClient
        redis = await RedisClient.get_client()
        if redis is None:
            return False
        if await redis.exists(key):
            return True
        await redis.set(key, "1", ex=ttl_seconds)
        return False
    except Exception as e:
        logger.warning(
            "Redis spam guard unavailable — skipping check",
            extra={"ctx_key": key, "ctx_error": str(e)},
        )
        return False


# ═══════════════════════════════════════════════════════════════════════════
#  DB helpers
# ═══════════════════════════════════════════════════════════════════════════

async def _get_rules_text() -> str:
    """
    Fetch community rules text from bot_config collection.

    Falls back to a hard-coded default if the DB is unavailable or the
    document does not exist.  Never raises.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["bot_config"].find_one({"key": "rules"})
        if doc and doc.get("value"):
            return doc["value"]
    except Exception:
        pass

    return (
        "📜 <b>Community Rules</b>\n\n"
        "1. Respect all community members.\n"
        "2. No spam or unsolicited promotions.\n"
        "3. Keep content relevant to the community.\n"
        "4. Follow Telegram's Terms of Service at all times.\n"
        "5. Admins have final say on all moderation decisions.\n\n"
        "<i>Violation of rules may result in removal from the community.</i>"
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Referral channel-membership check
#  (Section 16 — credit only after referred user joins MAIN_CHANNEL_ID)
# ═══════════════════════════════════════════════════════════════════════════

async def _is_channel_member(client: Client, user_id: int, channel_id: int) -> bool:
    """
    Check whether `user_id` is an active member of `channel_id`.

    Uses Pyrogram get_chat_member().  Returns False on any error so that
    a channel check failure never blocks the /start flow.
    """
    try:
        member = await client.get_chat_member(channel_id, user_id)
        # "left", "banned", "kicked" mean not a member.
        return member.status.value not in ("left", "banned", "kicked")
    except UserNotParticipant:
        return False
    except ChatAdminRequired:
        # Bot lacks permission to check membership; assume not a member.
        logger.warning(
            "channel_membership_check_no_permission",
            extra={"ctx_user_id": user_id, "ctx_channel_id": channel_id},
        )
        return False
    except Exception as e:
        logger.warning(
            "channel_membership_check_failed",
            extra={
                "ctx_user_id": user_id,
                "ctx_channel_id": channel_id,
                "ctx_error": str(e),
            },
        )
        return False


async def _handle_referral_at_start(
    client: Client,
    user_id: int,
    referred_by: int,
) -> None:
    """
    Process a referral relationship detected on /start.

    Spec Section 16 requires:
      1. New user joined via referral link — satisfied (we are in /start).
      2. New user has joined Main Channel — verified here via API call.
      3. Duplicate prevented — enforced by unique index on referred_user_id
         in the referrals collection.

    Behaviour:
      • Save the referral pair to MongoDB immediately (points_awarded=False).
        This is restart-safe: if the bot crashes after the DB write but
        before the membership check, the pair is preserved for later credit.
      • If the user is already a member of MAIN_CHANNEL_ID: call
        ref_service.register_referral() to credit the referrer now.
      • If the user is NOT yet a member: leave points_awarded=False.
        A separate ChatMemberUpdated handler (in referral_join_handler.py)
        must sweep pending referrals and award credit on channel join.

    Never raises — all errors are logged.
    """
    try:
        from app.referral.repository import ReferralRepository
        from app.referral.service import ReferralService
        from app.bot.client import get_bot

        db = DatabaseManager.get_db()
        ref_repo = ReferralRepository(db)
        ref_service = ReferralService(ref_repo, get_bot())

        # ── Step 1: persist the referral pair (restart-safe, duplicate-safe)
        # This is written to MongoDB BEFORE any Telegram action.
        # The unique index on referred_user_id prevents double-credit even
        # if this code path is entered twice (idempotent).
        try:
            await ref_repo.create_pending(
                referrer_id=referred_by,
                referred_id=user_id,
            )
        except Exception as save_err:
            # Likely a DuplicateKeyError — referral already recorded.
            logger.debug(
                "referral_pair_already_recorded",
                extra={
                    "ctx_referrer": referred_by,
                    "ctx_referred": user_id,
                    "ctx_error": str(save_err),
                },
            )
            return  # Nothing more to do — already processed.

        # ── Step 2: check MAIN_CHANNEL_ID membership
        main_channel_id = await _get_hub_config_value("main_channel_id")
        if main_channel_id is None:
            logger.warning(
                "referral_deferred_no_main_channel_id",
                extra={"ctx_referred": user_id},
            )
            # Cannot verify membership — credit deferred to join handler.
            return

        is_member = await _is_channel_member(client, user_id, main_channel_id)

        if is_member:
            # ── Step 3a: user already in channel — credit immediately
            try:
                await ref_service.register_referral(referred_by, user_id)
                logger.info(
                    "referral_credited_on_start",
                    extra={"ctx_referrer": referred_by, "ctx_referred": user_id},
                )
            except Exception as credit_err:
                logger.warning(
                    "referral_credit_failed",
                    extra={
                        "ctx_referrer": referred_by,
                        "ctx_referred": user_id,
                        "ctx_error": str(credit_err),
                    },
                )
        else:
            # ── Step 3b: not yet in channel — leave pending for join handler
            logger.info(
                "referral_deferred_awaiting_channel_join",
                extra={"ctx_referrer": referred_by, "ctx_referred": user_id},
            )

    except Exception as e:
        logger.warning(
            "referral_handling_failed",
            extra={
                "ctx_referrer": referred_by,
                "ctx_referred": user_id,
                "ctx_error": str(e),
            },
        )


# ═══════════════════════════════════════════════════════════════════════════
#  Main menu keyboard builder (spec Section 6)
# ═══════════════════════════════════════════════════════════════════════════

def _build_main_menu_keyboard() -> InlineKeyboardMarkup:
    """
    Build the standard main-menu inline keyboard as defined in Section 6.

    Row 1: [ 💎 Premium Access ]
    Row 2: [ 📤 Submit Content Anonymously ]
    Row 3: [ 🎁 Referral Program ]  [ 📊 My Status ]
    Row 4: [ 🆘 Need Help ]
    """
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💎 Premium Access",               callback_data="menu:premium")],
        [InlineKeyboardButton("📤 Submit Content Anonymously",   callback_data="menu:submit")],
        [
            InlineKeyboardButton("🎁 Referral Program",          callback_data="menu:referrals"),
            InlineKeyboardButton("📊 My Status",                 callback_data="menu:mystatus"),
        ],
        [InlineKeyboardButton("🆘 Need Help",                    callback_data="menu:support")],
    ])


def _build_onboarding_text() -> str:
    """Return the first-time onboarding message (spec Section 5)."""
    return (
        "👋 <b>Welcome to BD Gone Wild Community!</b>\n\n"
        "This bot is your central hub for the community. It handles:\n"
        "• Premium Access controls\n"
        "• Anonymous Content Submission\n"
        "• Content Removal Requests\n"
        "• User Status & Dashboard\n"
        "• Support Requests\n\n"
        "<b>Community Rules:</b>\n"
        "1. Respect all community members.\n"
        "2. No spam or unsolicited promotions.\n"
        "3. Keep content relevant to the community.\n"
        "4. Follow Telegram's Terms of Service at all times.\n"
        "5. Admins have final say on all moderation decisions.\n\n"
        "<i>Violation of rules may result in removal.</i>"
    )


# ═══════════════════════════════════════════════════════════════════════════
#  /start handler
# ═══════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("start") & filters.private)
async def handle_start(client: Client, message: Message) -> None:
    """
    Entry point for all users.

    First-time users:
      1. DB user document written (restart-safe — DB before Telegram).
      2. Onboarding message sent once (never again).
      3. Main menu shown.
      4. Referral relationship recorded; credit deferred until channel join
         is confirmed (spec Section 16).

    Returning users:
      → Main menu shown directly.

    Users with onboarded=False (edge-case: record exists but onboarding
    was interrupted):
      → Onboarding re-shown, onboarded flag set to True.
    """
    if not message.from_user:
        return

    user_id = message.from_user.id
    first_name = message.from_user.first_name or "Creator"

    # ── Anti-spam / cooldown ───────────────────────────────────────────────
    spam_key = f"onboarding:spam:{user_id}"
    if await _check_spam_guard(spam_key, ttl_seconds=5):
        asyncio.create_task(_delete_after(message, delay=0))
        return

    logger.info("/start received", extra={"ctx_user_id": user_id})

    try:
        from app.repositories.user_repository import UserRepository
        user_repo = UserRepository()

        user_doc = await user_repo.get_user(user_id)

        if user_doc is None:
            logger.info("new_user_detected", extra={"ctx_user_id": user_id})

            # ── Parse referral payload ─────────────────────────────────────────
            referred_by: Optional[int] = None
            if len(message.command) > 1:
                payload = message.command[1]
                if payload.startswith("ref_"):
                    try:
                        referred_by = int(payload.split("_")[1])
                        if referred_by == user_id:
                            referred_by = None  # Self-referral not allowed.
                    except (IndexError, ValueError):
                        pass
            
            # ── DB write FIRST (restart-safe per spec Section 25) ──────────
            try:
                await user_repo.upsert_user(
                    user_id=user_id,
                    full_name=f"{first_name} {message.from_user.last_name or ''}".strip(),
                    username=message.from_user.username,
                    referred_by=referred_by,
                )
                await user_repo.set_onboarded(user_id, True)

                # ── Free subscription grant ────────────────────────────────────
                try:
                    sub_service = _get_sub_service()
                    from app.models.subscription import Plan
                    await sub_service.grant(
                        user_id=user_id,
                        plan=Plan.FREE,
                        duration_days=None,
                        granted_by=0,  # System
                        notes="Auto-registered on /start"
                    )
                except Exception as sub_err:
                    logger.warning(
                        "new_user_sub_grant_failed",
                        extra={"ctx_user_id": user_id, "ctx_error": str(sub_err)},
                    )

                # ── Referral processing (non-fatal; deferred per spec §16) ─────
                if referred_by:
                    asyncio.create_task(
                        _handle_referral_at_start(client, user_id, referred_by)
                    )
            except Exception as insert_err:
                 logger.warning(
                    "new_user_insert_failed",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_error": str(insert_err),
                    },
                )

        elif not user_doc.get("onboarded", False):
            logger.info(
                "resumed_onboarding",
                extra={"ctx_user_id": user_id},
            )
            await user_repo.set_onboarded(user_id, True)
        
        else: # RETURNING USER
            logger.info(
                "returning_user_menu",
                extra={"ctx_user_id": user_id},
            )

        # Common send logic for all cases
        onboarding_service = _get_onboarding_service()
        text, keyboard = await onboarding_service.render_start(user_id, first_name)
        await _reply_floodwait(message, text, reply_markup=keyboard)

    except Exception as e:
        logger.exception(
            "handle_start_crashed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        try:
            await message.reply_text(
                "⚠️ Something went wrong. Please try /start again."
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
#  Onboarding callbacks
# ═══════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r"^onboarding:accept_terms$"))
async def handle_accept_terms(client: Client, callback_query: CallbackQuery) -> None:
    """
    Handle the user accepting the community terms of service.

    Updates terms_accepted=True in MongoDB BEFORE sending any Telegram
    response (restart-safe, Section 25).
    """
    user_id = callback_query.from_user.id
    first_name = callback_query.from_user.first_name or "Creator"

    # ── Persist acceptance to DB first ───────────────────────────────────
    try:
        from app.repositories.user_repository import UserRepository
        db = DatabaseManager.get_db()
        user_repo = UserRepository(db)
        await user_repo.update_one(
            {"user_id": user_id},
            {"$set": {
                "terms_accepted": True,
                "updated_at": datetime.now(timezone.utc),
            }},
        )
    except Exception as e:
        logger.warning(
            "set_terms_accepted_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    # ── Notify onboarding service (may perform additional state work) ─────
    onboarding_service = _get_onboarding_service()
    try:
        await onboarding_service.complete_onboarding(user_id)
    except Exception as e:
        logger.warning(
            "complete_onboarding_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    await callback_query.answer("Terms Accepted! Welcome. ✅")

    # ── Render main menu ───────────────────────────────────────────────────
    try:
        text, keyboard = await onboarding_service.render_start(user_id, first_name)
        await callback_query.message.edit_text(
            text, reply_markup=keyboard, parse_mode=ParseMode.HTML
        )
    except MessageNotModified:
        pass
    except Exception as e:
        logger.warning(
            "onboarding_menu_render_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        # Fallback: build menu manually.
        try:
            from app.services.onboarding_service import UserState
            text = f"👋 <b>Welcome to BD Gone Wild, {first_name}!</b>\n\nUse the menu below to navigate."
            keyboard = _build_main_menu_keyboard()
            await callback_query.message.edit_text(
                text, reply_markup=keyboard, parse_mode=ParseMode.HTML
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
#  Main menu callbacks
# ═══════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(
    filters.regex(r"^menu:(mystatus|rules|home|queue|referrals|support|submit)$")
)
async def handle_menu_callbacks(client: Client, callback_query: CallbackQuery) -> None:
    """
    Dispatch main-menu inline-keyboard callbacks.

    Actions handled:
      home      → Main menu (re-render)
      referrals → Referral program dashboard
      queue     → User's active content queue (jobs in flight)
      submit    → Content submission entry (routes to submission flow)
      rules     → Community rules
      mystatus  → User account status card
      support   → Need Help (triggers support session flow)
    """
    action = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id

    # ── Rate-limit ─────────────────────────────────────────────────────────
    spam_key = f"menu:spam:{user_id}"
    if await _check_spam_guard(spam_key, ttl_seconds=1):
        await callback_query.answer("Slow down! Processing...", show_alert=False)
        return

    await callback_query.answer()

    text: str = ""
    keyboard = None

    try:
        onboarding_service = _get_onboarding_service()

        if action == "home":
            try:
                text, keyboard = await onboarding_service.render_start(
                    user_id,
                    callback_query.from_user.first_name or "Creator",
                )
            except Exception as e:
                logger.warning(
                    "home_render_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                text = (
                    f"👋 <b>Welcome to BD Gone Wild, "
                    f"{callback_query.from_user.first_name or 'Creator'}!</b>\n\n"
                    "Use the menu below to navigate."
                )
                keyboard = _build_main_menu_keyboard()

        elif action == "referrals":
            try:
                from app.referral.repository import ReferralRepository
                from app.referral.service import ReferralService
                from app.referral.handlers import show_referral_status
                from app.bot.client import get_bot

                ref_repo = ReferralRepository(DatabaseManager.get_db())
                ref_service = ReferralService(ref_repo, get_bot())
                await show_referral_status(client, callback_query.message, ref_service)
                return
            except Exception as e:
                logger.exception(
                    "referral_status_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                await callback_query.answer(
                    "Referral system temporarily unavailable.", show_alert=True
                )
                return

        elif action == "queue":
            # Shows the user's active content queue (jobs currently in flight).
            queue_repo = _get_queue_repo()
            try:
                jobs = await queue_repo.get_user_queue(user_id)
            except Exception as e:
                logger.warning(
                    "queue_fetch_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                jobs = []

            if not jobs:
                text = (
                    "⏳ <b>Active Queue</b>\n\n"
                    "Your queue is currently empty.\n\n"
                    "Submit new content to see it tracked here in real-time."
                )
            else:
                lines = ["⏳ <b>Active Queue</b>\n"]
                for i, job in enumerate(jobs, 1):
                    status = job.get("status", "pending").capitalize()
                    media_type = job.get("media_type", "content").capitalize()
                    created_at = job.get("created_at")
                    date_str = created_at.strftime("%H:%M") if created_at else "??"
                    icon = "🟢" if status.lower() == "delivering" else "🟡"
                    lines.append(
                        f"{i}. {icon} <b>{media_type}</b> — {status} "
                        f"<code>[{date_str}]</code>"
                    )
                text = "\n".join(lines)

            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data="menu:home")
            ]])

        elif action == "submit":
            # Entry point for the content submission flow (Section 10).
            # The full multi-step FSM is handled by the submission handler
            # module.  Here we forward to the submission entry callback.
            try:
                from app.handlers.submission_handler import handle_submit_menu
                await handle_submit_menu(client, callback_query)
                return
            except Exception as e:
                logger.exception(
                    "submission_entry_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                await callback_query.answer("Submission system unavailable.", show_alert=True)
                return

        elif action == "rules":
            text = await _get_rules_text()
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data="menu:home")
            ]])

        elif action == "mystatus":
            from app.ui.status_cards import build_user_status_card
            from app.models.subscription import SubscriptionStatus

            sub_service = _get_sub_service()
            sub = await sub_service.get_subscription(user_id)

            try:
                from app.referral.repository import ReferralRepository
                ref_repo = ReferralRepository(DatabaseManager.get_db())
                wallet = await ref_repo.get_wallet(user_id) or {}
            except Exception:
                wallet = {}

            user_state = await onboarding_service.get_user_state(user_id)

            sub_data = None
            if sub and sub.status == SubscriptionStatus.ACTIVE:
                sub_data = {
                    "plan_label": sub.plan.value.upper() if sub.plan else "PREMIUM",
                    "expiry": (
                        sub.expires_at.strftime("%Y-%m-%d")
                        if sub.expires_at
                        else "Lifetime"
                    ),
                }

            try:
                from app.services.trust_service import TrustService
                trust_service = TrustService()
                trust_metrics = await trust_service.get_user_metrics(user_id)
            except Exception:
                trust_metrics = {"level": "🆕 NEW MEMBER", "fraud_score": 0.0}

            try:
                queue_repo = _get_queue_repo()
                recent_jobs = await queue_repo.get_user_queue(user_id)
            except Exception:
                recent_jobs = []

            wallet.update({
                "trust_level": trust_metrics.get("level", "🆕 NEW MEMBER"),
                "fraud_score": trust_metrics.get("fraud_score", 0.0),
                "recent_jobs": recent_jobs,
            })

            text, keyboard = build_user_status_card(
                user_id=user_id,
                username=callback_query.from_user.username,
                state=user_state.value,
                subscription=sub_data,
                wallet=wallet,
            )

        elif action == "support":
            # Entry point for the support system (spec Section 15).
            # Delegated to the support handler module.
            try:
                from app.handlers.support_handler import handle_support_entry
                await handle_support_entry(client, callback_query)
                return
            except ImportError:
                text = (
                    "🆘 <b>Support</b>\n\n"
                    "Type your message and an admin will respond shortly."
                )
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅️ Back", callback_data="menu:home")
                ]])
            except Exception as e:
                logger.exception(
                    "support_entry_failed",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                await callback_query.answer("Support system unavailable.", show_alert=True)
                return

        else:
            logger.warning(
                "unhandled_menu_action",
                extra={"ctx_user_id": user_id, "ctx_action": action},
            )
            return

        if text:
            try:
                await callback_query.message.edit_text(
                    text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                )
            except MessageNotModified:
                pass
            except FloodWait as e:
                wait = int(e.value) + _flood_buffer()
                logger.warning(
                    "FloodWait on menu edit",
                    extra={"ctx_wait": wait, "ctx_action": action},
                )
                await asyncio.sleep(wait)
                try:
                    await callback_query.message.edit_text(
                        text,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

    except Exception as e:
        logger.exception(
            "menu_callback_failed",
            extra={
                "ctx_user_id": user_id,
                "ctx_action": action,
                "ctx_error": str(e),
            },
        )
        try:
            await callback_query.answer("An error occurred.", show_alert=True)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
#  /rules command
# ═══════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("rules"))
async def handle_rules(client: Client, message: Message) -> None:
    """
    Send community rules to the user's DMs.

    If triggered from a group, post a brief acknowledgement with a 10-second
    auto-delete.  If the bot is blocked, show an inline link to start the bot.
    """
    if not message.from_user:
        return

    user_id = message.from_user.id
    is_group = message.chat.id != user_id

    rules_text = await _get_rules_text()
    dm_sent = await _send_private(client, user_id, rules_text)

    if is_group:
        await _ack_in_group(
            client, message,
            ack_text="📩 Rules sent to your DMs!",
            dm_sent=dm_sent,
        )

    logger.info("/rules", extra={"ctx_user_id": user_id, "ctx_chat": message.chat.id})


# ═══════════════════════════════════════════════════════════════════════════
#  /mystatus command
# ═══════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("mystatus"))
async def handle_mystatus(client: Client, message: Message) -> None:
    """
    Display the user's full account status card (spec Section 17).

    Shows subscription status, referral points, trust / fraud scores, and
    recent queue jobs.  Handles all sub-system failures gracefully so that
    one unavailable service never prevents the dashboard from loading.
    """
    if not message.from_user:
        return

    user_id = message.from_user.id

    try:
        from app.ui.status_cards import build_user_status_card
        from app.models.subscription import SubscriptionStatus

        sub_service = _get_sub_service()
        sub = await sub_service.get_subscription(user_id)

        try:
            from app.referral.repository import ReferralRepository
            ref_repo = ReferralRepository(DatabaseManager.get_db())
            wallet = await ref_repo.get_wallet(user_id) or {}
        except Exception:
            wallet = {}

        onboarding_service = _get_onboarding_service()
        user_state = await onboarding_service.get_user_state(user_id)

        sub_data = None
        if sub and sub.status == SubscriptionStatus.ACTIVE:
            sub_data = {
                "plan_label": sub.plan.value.upper() if sub.plan else "PREMIUM",
                "expiry": (
                    sub.expires_at.strftime("%Y-%m-%d")
                    if sub.expires_at
                    else "Lifetime"
                ),
            }

        try:
            from app.services.trust_service import TrustService
            trust_service = TrustService(DatabaseManager.get_db())
            trust_metrics = await trust_service.get_user_metrics(user_id)
        except Exception:
            trust_metrics = {"level": "🆕 NEW MEMBER", "fraud_score": 0.0}

        try:
            queue_repo = _get_queue_repo()
            recent_jobs = await queue_repo.get_user_queue(user_id)
        except Exception:
            recent_jobs = []

        wallet.update({
            "trust_level": trust_metrics.get("level", "🆕 NEW MEMBER"),
            "fraud_score": trust_metrics.get("fraud_score", 0.0),
            "recent_jobs": recent_jobs,
        })

        text, keyboard = build_user_status_card(
            user_id=user_id,
            username=message.from_user.username,
            state=user_state.value,
            subscription=sub_data,
            wallet=wallet,
        )

        await _reply_floodwait(message, text, reply_markup=keyboard)
        logger.info("/mystatus", extra={"ctx_user_id": user_id})

    except Exception as e:
        logger.exception(
            "handle_mystatus_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        try:
            await message.reply_text("⚠️ Account dashboard is currently unavailable.")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
#  /ping — health check (ops / dev use; should be removed or admin-gated
#  before public launch)
# ═══════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("ping") & filters.private)
async def handle_ping_test(client: Client, message: Message) -> None:
    """Minimal liveness probe.  Responds 'pong'."""
    await message.reply_text("pong")
