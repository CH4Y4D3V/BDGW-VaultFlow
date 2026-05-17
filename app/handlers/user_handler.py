from __future__ import annotations

import asyncio
from typing import Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, RPCError, UserIsBlocked, PeerIdInvalid, InputUserDeactivated
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup

from app.config import settings
from app.services.subscription_service import SubscriptionService
from app.utils.logger import get_logger

logger = get_logger(__name__)

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
