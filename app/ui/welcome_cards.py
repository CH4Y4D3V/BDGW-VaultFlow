from __future__ import annotations

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from app.ui.common import SECTION_DIVIDER, THIN_DIVIDER


def build_welcome_card_v3(first_name: str) -> str:
    """
    Final Production Welcome Card (v3).
    """
    header = f"👋 <b>Welcome to BD Gone Wild, {first_name}!</b>"
    
    body = (
        f"{SECTION_DIVIDER}\n"
        "Your central hub for the most exclusive BD community content.\n\n"
        "🛡 <b>Core Features:</b>\n"
        "• <b>Premium Vault</b> — Permanent access to curated media.\n"
        "• <b>Anonymous Submissions</b> — Share safely with the community.\n"
        "• <b>Real-time Delivery</b> — Zero delay on premium updates.\n"
        "• <b>Fraud Protection</b> — Secure TXID verification system.\n"
        "• <b>Referral Rewards</b> — Earn points for every new member.\n"
        f"{THIN_DIVIDER}\n"
        "<i>By continuing, you agree to our community rules and terms of service.</i>"
    )
    
    return f"{header}\n{body}"


def build_onboarding_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for first-time users."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I Accept the Rules & Terms", callback_data="onboarding:accept_terms")],
        [InlineKeyboardButton("📜 View Community Rules", callback_data="menu:rules")]
    ])
