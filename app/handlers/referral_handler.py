from __future__ import annotations
from pyrogram import Client, filters
from pyrogram.types import Message
from app.referral.service import ReferralService
from app.config import settings

_service = ReferralService()

@Client.on_message(filters.command("start") & filters.regex(r"ref_\d+") & filters.private)
async def handle_start_with_ref(client: Client, message: Message):
    payload = message.command[1] # 'ref_12345'
    try:
        referrer_id = int(payload.split("_")[1])
        success, text = await _service.handle_referral_start(client, referrer_id, message.from_user.id)
        await message.reply_text(f"✨ **Referral System**\n\n{text}")
    except (IndexError, ValueError):
        pass

@Client.on_message(filters.command("myreferrals") & filters.private)
async def cmd_myreferrals(client: Client, message: Message):
    user_id = message.from_user.id
    wallet = await _service.repo.get_wallet(user_id)
    
    if not wallet:
        await _service.repo.upsert_wallet(user_id)
        wallet = await _service.repo.get_wallet(user_id)

    me = await client.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref_{user_id}"
    
    text = (
        "📊 **Your Referral Status**\n\n"
        f"💰 **Points Balance:** `{wallet['points_balance']}`\n"
        f"📈 **Total Earned:** `{wallet['total_earned']}`\n"
        f"👥 **Active Referrals:** `{wallet['active_referrals']}`\n\n"
        "🔗 **Your Referral Link:**\n"
        f"{ref_link}\n\n"
        "*Share this link. Each qualified referral earns you 1 point toward Premium discounts!*"
    )
    await message.reply_text(text)

@Client.on_message(filters.command("applypoints") & filters.private)
async def cmd_applypoints(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("Usage: `/applypoints <price>`")
        
    try:
        price = float(message.command[1])
        wallet = await _service.repo.get_wallet(message.from_user.id)
        points = wallet['points_balance'] if wallet else 0
        
        # Logic: 1 point = 10% discount, max 5 points (50%)
        discount_pct = min(points * 0.1, 0.5) 
        discount_amount = price * discount_pct
        final_price = price - discount_amount
        
        text = (
            "💎 **Points Discount Preview**\n\n"
            f"Available Points: `{points}`\n"
            f"Original Price: `${price:.2f}`\n"
            f"Discount Applied: `{discount_pct*100:.0f}%` (-${discount_amount:.2f})\n"
            f"━━━━━━━━━━━━━━\n"
            f"✨ **Final Price: `${final_price:.2f}`**\n\n"
            "*Discount will be locked during checkout.*"
        )
        await message.reply_text(text)
    except ValueError:
        await message.reply_text("Invalid price format.")
