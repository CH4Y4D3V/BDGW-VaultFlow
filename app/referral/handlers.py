from __future__ import annotations
import structlog
from pyrogram import Client
from pyrogram.types import Message
from app.referral.service import ReferralService

logger = structlog.get_logger(__name__)

async def process_referral_start(client: Client, message: Message, referrer_id: int, service: ReferralService) -> None:
    # Called from the existing /start handler when a ref_ payload is detected.
    success = await service.register_referral(referrer_id, message.from_user.id)
    # Do NOT send any message here. Just process the referral silently.
    logger.info("process_referral_start", referrer_id=referrer_id, referred_id=message.from_user.id, success=success)

async def show_referral_status(client: Client, message: Message, service: ReferralService) -> None:
    # Called when user requests referral info.
    wallet = await service.get_wallet(message.from_user.id)
    
    if not wallet:
        points_balance = 0
        active_referrals = 0
        total_earned = 0
    else:
        points_balance = wallet.get('points_balance', 0)
        active_referrals = wallet.get('active_referrals', 0)
        total_earned = wallet.get('total_earned', 0)

    me = await client.get_me()
    user_id = message.from_user.id
    ref_link = f"https://t.me/{me.username}?start=ref_{user_id}"

    text = (
        "👥 **Referral Status**\n\n"
        f"💰 Points Balance: `{points_balance}`\n"
        f"📈 Active Referrals: `{active_referrals}`\n"
        f"💎 Total Earned: `{total_earned}`\n\n"
        "🔗 **Your Referral Link:**\n"
        f"`{ref_link}`"
    )
    # Thin handler. Logic is in service.
    await message.reply_text(text)
