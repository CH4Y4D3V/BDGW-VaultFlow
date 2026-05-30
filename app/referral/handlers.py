from __future__ import annotations

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from app.referral.service import ReferralService
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def process_referral_start(
    client: Client,
    message: Message,
    referrer_id: int,
    service: ReferralService,
) -> None:
    success = await service.register_referral(referrer_id, message.from_user.id)
    logger.info(
        'process_referral_start',
        extra={
            'ctx_referrer_id': referrer_id,
            'ctx_referred_id': message.from_user.id,
            'ctx_success': success,
        },
    )


async def show_referral_status(
    client: Client,
    message: Message,
    service: ReferralService,
) -> None:
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
    ref_link = f'https://t.me/{me.username}?start=ref_{user_id}'

    text = (
        "👥 <b>Referral Program</b>\n\n"
        f"<b>Points Balance:</b> ৳{points_balance}\n"
        f"<b>Active Referrals:</b> {active_referrals}\n"
        f"<b>Total Earned:</b> ৳{total_earned}\n\n"
        "🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        "🎁 <b>Rewards:</b>\n"
        "• 1 point for each qualified referral.\n"
        "• 1 point for every 2 approved submissions from your referrals.\n"
        "<i>(1 point = ৳1 discount on Premium)</i>"
    )

    from app.bot.keyboards import KeyboardBuilder
    await message.edit_text(text, reply_markup=KeyboardBuilder.build_back_button(), parse_mode=ParseMode.HTML)

