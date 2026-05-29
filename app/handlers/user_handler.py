from __future__ import annotations

import asyncio
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
    return OnboardingService(_get_sub_repo())

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
    Returns False if they should be allowed through — including when Redis is down.
    Never raises.
    """
    try:
        redis = get_redis()
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
    """
    Attempt to DM the user.
    Returns True on success, False if user has blocked the bot.
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
    """Delete messages after a delay. Best-effort — never raises."""
    await asyncio.sleep(delay)
    for msg in messages:
        if msg is None:
            continue
        try:
            await msg.delete()
        except Exception as e:
            logger.exception(
                "cleanup_message_delete_failed",
                extra={"ctx_error": str(e)},
            )
            pass


async def _ack_in_group(
    client: Client,
    message: Message,
    ack_text: str,
    blocked_text: str,
    dm_sent: bool,
) -> None:
    """
    Post a brief acknowledgement in the group then clean up both messages.
    If DM failed, post the blocked warning instead (no auto-delete for that).
    """
    if dm_sent:
        try:
            ack = await message.reply_text(ack_text, parse_mode=ParseMode.HTML)
            asyncio.create_task(_cleanup_messages(ack, message, delay=10.0))
        except Exception as e:
            logger.exception(
                "group_ack_send_failed",
                extra={"ctx_error": str(e)},
            )
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
        except Exception as e:
            logger.exception(
                "group_blocked_notice_failed",
                extra={"ctx_error": str(e)},
            )
            pass


# ── Rules DB helper ───────────────────────────────────────────────────────────

async def _get_rules_text() -> str:
    """
    Fetch custom rules from DB if set via admin command.
    Falls back to default text.
    """
    try:
        db = DatabaseManager.get_db()
        doc = await db["bot_config"].find_one({"key": "rules"})
        if doc and doc.get("value"):
            return doc["value"]
    except Exception as e:
        logger.exception(
            "rules_text_lookup_failed",
            extra={"ctx_error": str(e)},
        )
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


# ── Subscription status formatter ─────────────────────────────────────────────

def _format_status(sub, user_id: int) -> str:
    from app.models.subscription import SubscriptionStatus

    if sub is None:
        return (
            "📋 <b>Subscription Status</b>\n\n"
            "❌ No active subscription found.\n\n"
            "Contact an admin to subscribe."
        )

    status_icon = {
        SubscriptionStatus.ACTIVE: "✅",
        SubscriptionStatus.GRACE: "⚠️",
        SubscriptionStatus.EXPIRED: "❌",
        SubscriptionStatus.BANNED: "🚫",
    }.get(sub.status, "❓")

    lines = [
        "📋 <b>Subscription Status</b>\n",
        f"<b>Status:</b> {status_icon} {sub.status.value.capitalize()}",
        f"<b>Plan:</b> {sub.plan.value.capitalize()}",
        f"<b>Member since:</b> {sub.started_at.strftime('%Y-%m-%d')}",
    ]

    if sub.expires_at:
        lines.append(f"<b>Expires:</b> {sub.expires_at.strftime('%Y-%m-%d')}")
        if sub.remaining_days is not None:
            lines.append(f"<b>Remaining:</b> {sub.remaining_days} day(s)")
    else:
        lines.append("<b>Duration:</b> Lifetime ♾️")

    if sub.is_in_grace and sub.grace_until:
        lines.append(
            f"\n⚠️ <b>Grace period until:</b> {sub.grace_until.strftime('%Y-%m-%d')}\n"
            "Renew before grace expires to keep access."
        )

    return "\n".join(lines)


# ── /start ────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("start") & filters.private)
async def handle_start(client: Client, message: Message) -> None:
    try:
        if not message.from_user:
            return
        user_id = message.from_user.id
        first_name = message.from_user.first_name or "Creator"

        # ── Anti-Spam / Cooldown (best-effort — degrades gracefully if Redis down)
        spam_key = f"onboarding:spam:{user_id}"
        if await _check_spam_guard(spam_key, ttl_seconds=5):
            return

        logger.info("/start command received", extra={"ctx_user_id": user_id})

        # F-12: Auto-register user if not exists
        sub_service = _get_sub_service()
        existing_sub = await sub_service.get_subscription(user_id)
        if not existing_sub:
            from app.models.subscription import Plan
            await sub_service.grant(
                user_id=user_id,
                plan=Plan.FREE,
                duration_days=None,
                granted_by=0,
                notes="Auto-registered on /start"
            )
            logger.info("New user registered", extra={"ctx_user_id": user_id})

        # ── Referral Payload Handling ──
        if len(message.command) > 1:
            payload = message.command[1]
            if payload.startswith("ref_"):
                try:
                    referrer_id = int(payload.split("_")[1])
                    from app.referral.repository import ReferralRepository
                    from app.referral.service import ReferralService
                    from app.referral.handlers import process_referral_start

                    ref_repo = ReferralRepository(DatabaseManager.get_db())
                    ref_service = ReferralService(ref_repo, client)

                    await process_referral_start(client, message, referrer_id, ref_service)
                except (IndexError, ValueError):
                    pass
                except Exception as e:
                    # Referral failure must never block /start from completing
                    logger.warning(
                        "Referral processing failed — continuing start",
                        extra={"ctx_user_id": user_id, "ctx_error": str(e)},
                    )
            elif payload == "resubscribe":
                await handle_mystatus_direct(client, message)
                return

        onboarding_service = _get_onboarding_service()
        text, keyboard = await onboarding_service.render_onboarding(
            user_id,
            first_name
        )

        await message.reply_text(
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(
            "handle_start crashed",
            extra={"ctx_user_id": getattr(message.from_user, "id", "?"), "ctx_error": str(e)},
            exc_info=True,
        )
        try:
            await message.reply_text("❌ System busy. Please try again in a moment.")
        except Exception as e:
            logger.exception(
                "start_error_reply_failed",
                extra={"ctx_error": str(e)},
            )
            pass

async def handle_mystatus_direct(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    sub_service = _get_sub_service()
    sub = await sub_service.get_subscription(user_id)
    from app.referral.repository import ReferralRepository
    ref_repo = ReferralRepository(DatabaseManager.get_db())
    wallet = await ref_repo.get_wallet(user_id)
    queue_repo = _get_queue_repo()
    user_queue = await queue_repo.get_user_queue(user_id)
    from app.models.subscription import SubscriptionStatus
    plan_name = sub.plan.value.upper() if sub else "FREE"
    status_emoji = "✅" if sub and sub.status == SubscriptionStatus.ACTIVE else "⏳"
    points = wallet.get("points_balance", 0) if wallet else 0
    queue_text = ""
    if user_queue:
        queue_text = "\n\n<b>Recent Submissions:</b>\n"
        for j in user_queue[:5]:
            status = j.get("status", "pending").capitalize()
            queue_text += f"• {j.get('content_id', '???')[:8]}... [{status}]\n"
    else:
        queue_text = "\n\n<i>No recent submissions.</i>"
    text = (
        f"👤 <b>Account Status</b>\n\n"
        f"<b>Plan:</b> {plan_name} {status_emoji}\n"
        f"<b>ID:</b> <code>{user_id}</code>\n"
        f"<b>Points:</b> ৳{points}\n"
        f"{queue_text}"
    )
    await message.reply_text(text, reply_markup=KeyboardBuilder.build_back_button(), parse_mode=ParseMode.HTML)


# ── Menu Callbacks ────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^menu:(mystatus|rules|home|queue|referrals)$"))
async def handle_menu_callbacks(client: Client, callback_query: CallbackQuery) -> None:
    """Handles main menu callbacks, editing the message in-place."""
    action = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id

    # ── Anti-Spam / Debounce (best-effort)
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
            text, keyboard = await onboarding_service.render_onboarding(
                user_id,
                callback_query.from_user.first_name or "Creator"
            )

        elif action == "referrals":
            try:
                from app.referral.repository import ReferralRepository
                from app.referral.service import ReferralService
                from app.referral.handlers import show_referral_status

                ref_repo = ReferralRepository(DatabaseManager.get_db())
                ref_service = ReferralService(ref_repo, client)

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
            # F-04: Comprehensive Status Dashboard
            from app.ui.status_cards import build_user_status_card
            from app.models.subscription import SubscriptionStatus
            
            sub_service = _get_sub_service()
            sub = await sub_service.get_subscription(user_id)
            
            from app.referral.repository import ReferralRepository
            ref_repo = ReferralRepository(DatabaseManager.get_db())
            wallet = await ref_repo.get_wallet(user_id)
            
            onboarding_service = _get_onboarding_service()
            user_state = await onboarding_service.get_user_state(user_id)
            
            # Prepare data for new UI
            sub_data = None
            if sub and sub.status == SubscriptionStatus.ACTIVE:
                sub_data = {
                    "plan_label": sub.plan.value.upper(),
                    "expiry": sub.expires_at.strftime("%Y-%m-%d") if sub.expires_at else "Lifetime"
                }

            text, keyboard = build_user_status_card(
                user_id=user_id,
                username=callback_query.from_user.username,
                state=user_state.value,
                subscription=sub_data
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
    await handle_mystatus_direct(client, message)
    logger.info("/mystatus", extra={"ctx_user_id": message.from_user.id})


# ── /ping ─────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ping") & filters.private)
async def handle_ping_test(client: Client, message: Message) -> None:
    await message.reply_text("pong")
