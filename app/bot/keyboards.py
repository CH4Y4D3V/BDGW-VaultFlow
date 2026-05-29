from __future__ import annotations

from typing import TYPE_CHECKING

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

if TYPE_CHECKING:
    from app.services.onboarding_service import UserState


class KeyboardBuilder:
    @staticmethod
    def build_main_menu(state: str) -> InlineKeyboardMarkup:
        """
        Builds the streamlined vertical button layout.
        
        Row 1: [ 💎 Premium Access ]
        Row 2: [ 📨 Submit Content ]  [ 👤 Anonymous ]
        Row 3: [ 👥 Referral ]  [ 📊 My Status ]
        Row 4: [ 🆘 Support ]
        """
        # Banned users only get support
        if state == "banned":
            return InlineKeyboardMarkup([
                [InlineKeyboardButton("🆘 Support & Appeal", callback_data="menu:support")]
            ])

        # Streamlined layout
        buttons = [
            [InlineKeyboardButton("💎 PREMIUM ACCESS", callback_data="menu:premium")],
            [
                InlineKeyboardButton("📨 SUBMIT", callback_data="menu:submit"),
                InlineKeyboardButton("👤 ANONYMOUS", callback_data="menu:anonymous"),
            ],
            [
                InlineKeyboardButton("👥 REFERRAL", callback_data="menu:referrals"),
                InlineKeyboardButton("📊 MY STATUS", callback_data="menu:mystatus")
            ],
            [InlineKeyboardButton("🆘 CUSTOMER SUPPORT", callback_data="menu:support")]
        ]

        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def build_back_button(target: str = "home") -> InlineKeyboardMarkup:
        from app.ui.common import build_back_button
        return InlineKeyboardMarkup([build_back_button(target)])

    @staticmethod
    def build_premium_conversion() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 UPGRADE TO PREMIUM", callback_data="menu:pay_premium")],
            [InlineKeyboardButton("⬅️ BACK TO MENU", callback_data="menu:home")]
        ])
