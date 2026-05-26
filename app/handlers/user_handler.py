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

# ── Global Instances (Lazy Loaded) ───────────────────────────────────────────

def _get_sub_repo():
    return SubscriptionRepository(DatabaseManager.get_db())

def _get_queue_repo():
    return QueueRepository(DatabaseManager.get_db())

def _get_onboarding_service():
    return OnboardingService(_get_sub_repo())

_sub_service = SubscriptionService()
_FLOOD_BUFFER = settings.FLOODWAIT_EXTRA_BUFFER
_MAX_RETRIES = 3

# Cache bot username to avoid repeated get_me() calls
_bot_username: Optional[str] = None


async def _get_bot_username(client: Client) -> str:
    global _bot_username
    if _bot_username is None:
        try:
            me = await client.get_me()
            _bot_username = me.username or ""
        except Exception:
            _bot_username = ""
    return _bot_username


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
            await asyncio.sleep(int(e.value) + _FLOOD_BUFFER)
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
        except Exception:
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
    """
    Fetch custom rules from DB if set via admin command.
    Falls back to default text.
    """
    try:
        from app.core.database import DatabaseManager
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


# ── /start and Menu Callbacks ─────────────────────────────────────────────────

@Client.on_message(filters.command("start") & filters.private)
async def handle_start(client: Client, message: Message) -> None:
    try:
        if not message.from_user:
            return
        user_id = message.from_user.id

        # ── Anti-Spam / Cooldown ──
        redis = get_redis()
        spam_key = f"onboarding:spam:{user_id}"
        if await redis.exists(spam_key):
            # Lightweight refresh UX: brief toast or ignore
            # We ignore to avoid cluttering the chat with repeated /start responses
            return
        await redis.set(spam_key, "1", ex=5)  # 5 second cooldown

        logger.info("/start command received", extra={"ctx_user_id": user_id})

        # ── Referral Payload Handling ──
        if len(message.command) > 1:
            payload = message.command[1]
            if payload.startswith("ref_"):
                try:
                    referrer_id = int(payload.split("_")[1])
                    from app.referral.service import ReferralService
                    ref_service = ReferralService()
                    success, ref_text = await ref_service.handle_referral_start(client, referrer_id, user_id)
                    # We continue to show onboarding after processing referral
                    if success:
                        await message.reply_text(f"✨ <b>Referral System</b>\n\n{ref_text}")
                except (IndexError, ValueError):
                    pass
            elif payload == "resubscribe":
                await handle_mystatus(client, message)
                return

        onboarding_service = _get_onboarding_service()
        text, keyboard = await onboarding_service.render_onboarding(
            user_id, 
            message.from_user.first_name or "Creator"
        )
        
        await message.reply_text(
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
    except Exception as e:
        import traceback
        logger.error(f"HANDLE_START CRASH: {e}\n{traceback.format_exc()}")
        try:
            await message.reply_text("❌ System busy. Please try again in a moment.")
        except Exception:
            pass


@Client.on_callback_query(filters.regex(r"^menu:(mystatus|rules|home|premium|queue|referrals)$"))
async def handle_menu_callbacks(client: Client, callback_query: CallbackQuery) -> None:
    """Handles main menu callbacks, editing the message in-place."""
    action = callback_query.data.split(":")[1]
    user_id = callback_query.from_user.id
    
    # ── Anti-Spam / Debounce ──
    redis = get_redis()
    spam_key = f"menu:spam:{user_id}"
    if await redis.exists(spam_key):
        await callback_query.answer("Slow down! Processing...", show_alert=False)
        return
    await redis.set(spam_key, "1", ex=1)  # 1 second debounce
    
    text = ""
    keyboard = None

    try:
        onboarding_service = _get_onboarding_service()
        if action == "home":
            text, keyboard = await onboarding_service.render_onboarding(
                user_id, 
                callback_query.from_user.first_name or "Creator"
            )
        
        elif action == "premium":
            text = (
                "💎 <b>PREMIUM ACCESS</b>\n\n"
                "Unlock the full power of VaultFlow with our Premium tier.\n\n"
                "✨ <b>Exclusive Features:</b>\n"
                "• <b>Priority Delivery:</b> Jump to the front of the queue.\n"
                "• <b>Custom Watermarks:</b> Your brand on every piece of content.\n"
                "• <b>Multi-Channel Sync:</b> Distribute to unlimited targets.\n"
                "• <b>Advanced Analytics:</b> Track your content's performance.\n"
                "• <b>24/7 Priority Support:</b> Direct line to our engineers.\n\n"
                "<i>Join the elite circle of creators today.</i>"
            )
            keyboard = KeyboardBuilder.build_premium_conversion()

        elif action == "referrals":
            from app.referral.service import ReferralService
            ref_service = ReferralService()
            wallet = await ref_service.repo.get_wallet(user_id)
            if not wallet:
                await ref_service.repo.upsert_wallet(user_id)
                wallet = await ref_service.repo.get_wallet(user_id)

            bot_username = await _get_bot_username(client)
            ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
            
            text = (
                "📊 <b>Your Referral Status</b>\n\n"
                f"💰 <b>Points Balance:</b> <code>{wallet['points_balance']}</code>\n"
                f"📈 <b>Total Earned:</b> <code>{wallet['total_earned']}</code>\n"
                f"👥 <b>Active Referrals:</b> <code>{wallet['active_referrals']}</code>\n\n"
                "🔗 <b>Your Referral Link:</b>\n"
                f"<code>{ref_link}</code>\n\n"
                "<i>Share this link. Each qualified referral earns you 1 point toward Premium discounts!</i>"
            )
            keyboard = KeyboardBuilder.build_back_button()

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
            # This logic is adapted from the handle_mystatus command for an inline menu context
            if user_id == settings.OWNER_ID: role = "Owner"
            elif user_id in settings.SUDO_IDS: role = "Sudo Admin"
            elif user_id in settings.ADMIN_IDS: role = "Admin"
            else: role = None

            if role:
                text = (
                    "📋 <b>Subscription Status</b>\n\n"
                    f"✅ <b>Status:</b> Permanent Access\n"
                    f"🔑 <b>Role:</b> {role}"
                )
            else:
                sub = await _sub_service.get_subscription(user_id)
                text = _format_status(sub, user_id)
            
            back_button = [InlineKeyboardButton("⬅️ Back", callback_data="menu:home")]
            buttons = [back_button]
            
            sub = await _sub_service.get_subscription(user_id)
            if sub and (sub.is_expired or sub.is_in_grace):
                bot_username = await _get_bot_username(client)
                url = f"https://t.me/{bot_username}?start=resubscribe"
                # Use insert to put the resubscribe button at the top
                buttons.insert(0, [InlineKeyboardButton("🔄 Resubscribe", url=url)])

            keyboard = InlineKeyboardMarkup(buttons)

        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        await callback_query.answer()

    except MessageNotModified:
        await callback_query.answer() # User clicked the same button twice
    except Exception as e:
        logger.error("Error in menu callback", extra={"ctx_user_id": user_id, "ctx_action": action, "ctx_error": str(e)}, exc_info=True)
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
            blocked_text="",  # handled inside _ack_in_group
            dm_sent=dm_sent,
        )
    # In private chat DM already delivered above — nothing more to do

    logger.info("/rules", extra={"ctx_user_id": user_id, "ctx_chat": message.chat.id})


# ── /mystatus ─────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("mystatus"))
async def handle_mystatus(client: Client, message: Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id
    is_group = message.chat.id != user_id

    # Privileged users get a permanent-access message
    if user_id == settings.OWNER_ID:
        role = "Owner"
    elif user_id in settings.SUDO_IDS:
        role = "Sudo Admin"
    elif user_id in settings.ADMIN_IDS:
        role = "Admin"
    else:
        role = None

    if role:
        text = (
            "📋 <b>Subscription Status</b>\n\n"
            f"✅ <b>Status:</b> Permanent Access\n"
            f"🔑 <b>Role:</b> {role}"
        )
        dm_sent = await _send_private(client, user_id, text)
        if is_group:
            await _ack_in_group(client, message, "📩 Status sent to your DMs!", "", dm_sent)
        return

    # Regular user — fetch from DB
    sub = await _sub_service.get_subscription(user_id)
    text = _format_status(sub, user_id)

    keyboard = None
    if sub is None or sub.is_expired or sub.is_in_grace:
        bot_username = await _get_bot_username(client)
        url = f"https://t.me/{bot_username}?start=resubscribe" if bot_username else f"https://t.me/{bot_username}"
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Resubscribe", url=url),
        ]])

    dm_sent = await _send_private(client, user_id, text, reply_markup=keyboard)

    if is_group:
        await _ack_in_group(
            client, message,
            ack_text="📩 Your subscription status has been sent to your DMs!",
            blocked_text="",
            dm_sent=dm_sent,
        )

    logger.info("/mystatus", extra={"ctx_user_id": user_id})


@Client.on_message(filters.command("ping") & filters.private)
async def handle_ping_test(client: Client, message: Message) -> None:
    await message.reply_text("pong")