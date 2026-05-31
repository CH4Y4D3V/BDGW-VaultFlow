from __future__ import annotations

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from app.ui.common import SECTION_DIVIDER


def build_help_card_v2() -> str:
    """
    Final Production Help Card (v2).
    """
    header = "📖 <b>VaultFlow Help Center</b>"
    
    body = (
        f"{SECTION_DIVIDER}\n"
        "💡 <b>Common Commands:</b>\n"
        "• /start — Open main menu & status\n"
        "• /mystatus — Check sub & points\n"
        "• /takedown — Report content removal\n"
        "• /help — Show this guide\n\n"
        "🛡 <b>Premium Access:</b>\n"
        "Use the 💎 <b>Premium Access</b> button in the main menu to upgrade. "
        "We support bKash, Nagad, and Crypto.\n\n"
        "📤 <b>Submitting Content:</b>\n"
        "Forward any media to the bot to start the submission process. "
        "All submissions are anonymous by default.\n\n"
        "🆘 <b>Need Human Support?</b>\n"
        "Click the button below to open a direct ticket with our staff."
    )
    
    return f"{header}\n{body}"


def build_help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆘 Open Support Ticket", callback_data="menu:support")],
        [InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu:home")]
    ])
