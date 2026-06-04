from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone  # FIX: was missing — datetime.now(timezone.utc) used below
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError, UserIsBlocked, PeerIdInvalid, InputUserDeactivated, MessageNotModified
from pyrogram.types import CallbackQuery, Message, InlineKeyboardButton, InlineKeyboardMarkup

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

# ── Lazy getters — NO module-level instantiation ──────────────────────────────

def _get_flood_buffer() -> int:
    return getattr(settings, "FLOODWAIT_EXTRA_BUFFER", 2)

def _get_sub_repo():
    return SubscriptionRepository()

def _get_queue_repo():
    return QueueRepository(DatabaseManager.get_db())

def _get_onboarding_service():
    from app.repositories.user_repository import UserRepository
    return OnboardingService(_get_sub_repo(), UserRepository())

def _get_sub_service():
    return SubscriptionService()

_MAX_RETRIES = 3

# Cache bot username to avoid repeated get_me() calls
_bot_username: Optional[str] = None


async def _get_bot_username(client: Client) -> str:
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


# ── Redis spam guard — fault tolerant ────────────────────────────────────────

async def _check_spam_guard(key: str, ttl_seconds: int) -> bool:
    """
    Returns True if the user is within the cooldown window (should be blocked).
    Never raises.
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


# ── DM delivery helper ────────────────────────────────────────────────────────

async def _send_private(
    client: Client,
    user_id: int,
    text: str,
    reply_markup=None,
) -> bool:
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
            await asyncio.sleep(int(e.value) + _get_flood_buffer())
        except RPCError as e:
            logger.warning(
                "DM delivery failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(e), "ctx_attempt": attempt + 1},
            )
            if attempt == _MAX_RETRIES - 1:
                return False
            await asyncio.sleep(2 ** attempt)
    return False


async def _cleanup_messages(*messages: Optional[Message], delay: float = 10.0) -> None:
    await asyncio.sleep(delay)
    for msg in messages:
        if msg is None:
            continue
        try:
            await msg.delete()
        except Exception:
            pass


async def _ack_in_group(
    client: Client,
    message: Message,
    ack_text: str,
    blocked_text: str,
    dm_sent: bool,
) -> None:
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


# ── Rules DB helper ───────────────────────────────────────────────────────────

async def _get_rules_text() -> str:
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


async def _delete_after(message: Message, delay: float = 10.0) -> None:
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass


# ── /start ────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("start") & filters.private)
async def handle_start(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    user_id = message.from_user.id
    first_name = message.from_user.first_name or "Creator"

    # ── Anti-Spam / Cooldown
    spam_key = f"onboarding:spam:{user_id}"
    if await _check_spam_guard(spam_key, ttl_seconds=5):
        asyncio.create_task(_delete_after(message, delay=0))
        return

    logger.info("/start command received", extra={"ctx_user_id": user_id})

    try:
        from app.repositories.user_repository import UserRepository
        user_repo = UserRepository()
        
        try:
            user_doc = await user_repo.find_one({"_id": user_id})
        except Exception as repo_err:
            logger.error("start_repo_find_failed", extra={"ctx_error": str(repo_err)})
            user_doc = None

        referred_by = None
        if len(message.command) > 1:
            payload = message.command[1]
            if payload.startswith("ref_"):
                try:
                    referred_by = int(payload.split("_")[1])
                except (IndexError, ValueError):
                    pass

        # Define the exact main menu keyboard
        main_menu_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 Premium Access", callback_data="menu:premium")],
            [InlineKeyboardButton("📤 Submit Content Anonymously", callback_data="menu:queue")],
            [InlineKeyboardButton("🎁 Referral Program", callback_data="menu:referrals"),
             InlineKeyboardButton("📊 My Status", callback_data="menu:mystatus")],
            [InlineKeyboardButton("🆘 Need Help", callback_data="menu:support")]
        ])

        onboarding_text = (
            "👋 <b>Welcome to BD Gone Wild Community!</b>\n\n"
            "This bot is your central hub for the community. It handles:\n"
            "• Controls Premium Access\n"
            "• Handles Content Submission\n"
            "• Handles Content Removal Requests\n"
            "• Handles User Status\n"
            "• Handles Support Requests\n\n"
            "<b>Community Rules:</b>\n"
            "1. Respect all community members.\n"
            "2. No spam or unsolicited promotions.\n"
            "3. Keep content relevant to the community.\n"
            "4. Follow Telegram's Terms of Service at all times.\n"
            "5. Admins have final say on all moderation decisions.\n\n"
            "<i>Violation of rules may result in removal.</i>"
        )

        main_menu_text = f"👋 <b>Welcome to BD Gone Wild, {first_name}!</b>\n\nUse the menu below to navigate."

        if user_doc is None:
            # New User Registration
            logger.info("new_user_detected", extra={"ctx_user_id": user_id})
            try:
                await user_repo.insert_one({
                    "_id": user_id,
                    "first_name": first_name,
                    "full_name": f"{first_name} {message.from_user.last_name or ''}".strip(),
                    "referral_code": uuid.uuid4().hex[:8],
                    "last_name": message.from_user.last_name,
                    "username": message.from_user.username,
                    "onboarded": True,
                    "referred_by": referred_by,
                    "created_at": datetime.now(timezone.utc),
                    "join_date": datetime.now(timezone.utc),
                })
            except Exception as insert_err:
                logger.warning("new_user_insert_failed", extra={"ctx_error": str(insert_err)})

            # Ensure free sub exists
            try:
                sub_service = _get_sub_service()
                from app.models.subscription import Plan
                await sub_service.grant(user_id, Plan.FREE, None, 0, "Auto-registered on /start")
            except Exception as sub_err:
                logger.warning("new_user_sub_grant_failed", extra={"ctx_error": str(sub_err)})

            # Show onboarding message once
            try:
                await message.reply_text(onboarding_text, parse_mode=ParseMode.HTML)
                await message.reply_text(main_menu_text, reply_markup=main_menu_keyboard, parse_mode=ParseMode.HTML)
            except Exception as render_err:
                logger.error("new_user_render_failed", extra={"ctx_error": str(render_err)})

            # Referral registration (non-fatal)
            if referred_by:
                try:
                    from app.referral.repository import ReferralRepository
                    from app.referral.service import ReferralService
                    from app.bot.client import get_bot
                    ref_repo = ReferralRepository(DatabaseManager.get_db())
                    ref_service = ReferralService(ref_repo, get_bot())
                    await ref_service.register_referral(referred_by, user_id)
                except Exception as ref_err:
                    logger.warning("referral_registration_failed", extra={"ctx_error": str(ref_err)})

        elif not user_doc.get("onboarded", False):
            # Found AND onboarded=False
            logger.info("resumed_onboarding", extra={"ctx_user_id": user_id})
            try:
                await user_repo.update_one({"_id": user_id}, {"$set": {"onboarded": True, "updated_at": datetime.now(timezone.utc)}})
            except Exception as update_err:
                logger.warning("set_onboarded_failed", extra={"ctx_error": str(update_err)})

            try:
                await message.reply_text(onboarding_text, parse_mode=ParseMode.HTML)
                await message.reply_text(main_menu_text, reply_markup=main_menu_keyboard, parse_mode=ParseMode.HTML)
            except Exception as render_err:
                logger.error("resumed_onboarding_render_failed", extra={"ctx_error": str(render_err)})

        else:
            # Returning User → Main Menu directly
            logger.info("returning_user_menu", extra={"ctx_user_id": user_id})
            try:
                await message.reply_text(main_menu_text, reply_markup=main_menu_keyboard, parse_mode=ParseMode.HTML)
            except Exception as render_err:
                logger.error("returning_user_render_failed", extra={"ctx_error": str(render_err)})

    except Exception as e:
        logger.exception("handle_start_crashed", extra={"ctx_error": str(e)})
        try:
            await message.reply_text("⚠️ Something went wrong. Please try /start again.")
        except Exception:
            pass


# ── Onboarding Callbacks ──────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^onboarding:accept_terms$"))
async def handle_accept_terms(client: Client, callback_query: CallbackQuery) -> None:
    user_id = callback_query.from_user.id
    first_name = callback_query.from_user.first_name or "Creator"

    await callback_query.answer("Terms Accepted! Welcome. ✅")

    onboarding_service = _get_onboarding_service()
    try:
        await onboarding_service.complete_onboarding(user_id)
    except Exception as e:
        logger.warning(
            "complete_onboarding_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )

    try:
        text, keyboard = await onboarding_service.render_start(user_id, first_name)
        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(
            "onboarding_menu_render_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )


# ── Menu Callbacks ────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:(mystatus|rules|home|queue|referrals)$"))
async def handle_menu_callbacks(client: Client, callback_query: CallbackQuery) -> None:
    action = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id

    spam_key = f"menu:spam:{user_id}"
    if await _check_spam_guard(spam_key, ttl_seconds=1):
        await callback_query.answer("Slow down! Processing...", show_alert=False)
        return

    await callback_query.answer()

    text = ""
    keyboard = None

    try:
        onboarding_service = _get_onboarding_service()
        if action == "home":
            # FIX: render_onboarding does not exist — use render_start
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
                from app.services.onboarding_service import UserState
                text = "👋 Main Menu"
                keyboard = KeyboardBuilder.build_main_menu(UserState.RETURNING.value)

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
                    "Referral status unavailable",
                    extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                )
                await callback_query.answer("Referral system temporarily unavailable.", show_alert=True)
                return

        elif action == "queue":
            queue_repo = _get_queue_repo()
            jobs = await queue_repo.get_user_queue(user_id)
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
                    media_type = job.get("media_type", "text").capitalize()
                    created_at = job.get("created_at")
                    date_str = created_at.strftime("%H:%M") if created_at else "??"
                    icon = "🟢" if status == "Delivering" else "🟡"
                    lines.append(f"{i}. {icon} <b>{media_type}</b> — {status} <code>[{date_str}]</code>")
                text = "\n".join(lines)

            keyboard = KeyboardBuilder.build_back_button()

        elif action == "rules":
            text = await _get_rules_text()
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:home")]])

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
                    "expiry": sub.expires_at.strftime("%Y-%m-%d") if sub.expires_at else "Lifetime"
                }

            try:
                from app.services.trust_service import TrustService
                trust_service = TrustService()
                trust_metrics = await trust_service.get_user_metrics(user_id)
            except Exception:
                trust_metrics = {"level": "🆕 NEW MEMBER", "fraud_score": 0.0}

            queue_repo = _get_queue_repo()
            recent_jobs = await queue_repo.get_user_queue(user_id)

            wallet.update({
                "trust_level": trust_metrics["level"],
                "fraud_score": trust_metrics["fraud_score"],
                "recent_jobs": recent_jobs
            })

            text, keyboard = build_user_status_card(
                user_id=user_id,
                username=callback_query.from_user.username,
                state=user_state.value,
                subscription=sub_data,
                wallet=wallet
            )

        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

    except MessageNotModified:
        pass
    except Exception as e:
        logger.exception(
            "menu_callback_failed",
            extra={"ctx_user_id": user_id, "ctx_action": action, "ctx_error": str(e)},
        )
        await callback_query.answer("An error occurred.", show_alert=True)


# ── /rules ────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("rules"))
async def handle_rules(client: Client, message: Message) -> None:
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
            blocked_text="",
            dm_sent=dm_sent,
        )

    logger.info("/rules", extra={"ctx_user_id": user_id, "ctx_chat": message.chat.id})


# ── /mystatus ─────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("mystatus"))
async def handle_mystatus(client: Client, message: Message) -> None:
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
                "expiry": sub.expires_at.strftime("%Y-%m-%d") if sub.expires_at else "Lifetime"
            }

        try:
            from app.services.trust_service import TrustService
            trust_service = TrustService()
            trust_metrics = await trust_service.get_user_metrics(user_id)
        except Exception:
            trust_metrics = {"level": "🆕 NEW MEMBER", "fraud_score": 0.0}

        queue_repo = _get_queue_repo()
        recent_jobs = await queue_repo.get_user_queue(user_id)

        wallet.update({
            "trust_level": trust_metrics["level"],
            "fraud_score": trust_metrics["fraud_score"],
            "recent_jobs": recent_jobs
        })

        text, keyboard = build_user_status_card(
            user_id=user_id,
            username=message.from_user.username,
            state=user_state.value,
            subscription=sub_data,
            wallet=wallet
        )

        await message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        logger.info("/mystatus", extra={"ctx_user_id": user_id})

    except Exception as e:
        logger.exception(
            "handle_mystatus_failed",
            extra={"ctx_user_id": user_id, "ctx_error": str(e)},
        )
        await message.reply_text("⚠️ Account dashboard is currently unavailable.")


@Client.on_message(filters.command("ping") & filters.private)
async def handle_ping_test(client: Client, message: Message) -> None:
    await message.reply_text("pong")
