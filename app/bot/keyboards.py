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
        Row 2: [ 📨 Submit Content ]
        Row 3: [ 👥 Referral ]  [ 📊 My Status ]
        Row 4: [ 🆘 Support ]
        """
        # Banned users only get support
        if state == "banned":
            return InlineKeyboardMarkup([
                [InlineKeyboardButton("🆘 Support", callback_data="menu:support")]
            ])

        # Streamlined layout
        buttons = [
            [InlineKeyboardButton("💎 Premium Access", callback_data="menu:premium")],
            [InlineKeyboardButton("📨 Submit Content", callback_data="menu:submit")],
            [
                InlineKeyboardButton("👥 Referral", callback_data="menu:referrals"),
                InlineKeyboardButton("📊 My Status", callback_data="menu:mystatus")
            ],
            [InlineKeyboardButton("🆘 Support", callback_data="menu:support")]
        ]

        # Admins get an extra row at the top
        if state == "admin":
            buttons.insert(0, [
                InlineKeyboardButton("🛡 Admin Panel", callback_data="admin:dashboard"),
                InlineKeyboardButton("📥 Moderation", callback_data="admin:moderation")
            ])

        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def build_back_button(target: str = "home") -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ Back to Menu", callback_data=f"menu:{target}")
        ]])

    @staticmethod
    def build_premium_conversion() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Upgrade to Premium", callback_data="menu:pay_premium")],
            [InlineKeyboardButton("⬅️ Back", callback_data="menu:home")]
        ])
