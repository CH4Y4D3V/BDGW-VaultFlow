from __future__ import annotations

from typing import TYPE_CHECKING

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

if TYPE_CHECKING:
    from app.services.onboarding_service import UserState


class KeyboardBuilder:
    @staticmethod
    def build_main_menu(state: str) -> InlineKeyboardMarkup:
        """
        Builds a premium, hierarchy-optimized keyboard based on user state.
        Using string for state to avoid circular imports.
        """
        buttons = []

        # Row 1: Primary Actions
        if state == "banned":
            buttons.append([
                InlineKeyboardButton("🆘 Appeal / Support", callback_data="menu:support")
            ])
            return InlineKeyboardMarkup(buttons)

        # Row 1: High-value CTAs
        if state == "admin":
            buttons.append([
                InlineKeyboardButton("🛡 Admin Panel", callback_data="admin:dashboard"),
                InlineKeyboardButton("📥 Moderation", callback_data="admin:moderation")
            ])
        else:
            buttons.append([
                InlineKeyboardButton("📤 Submit Content", callback_data="menu:submit"),
                InlineKeyboardButton("💎 Premium", callback_data="menu:premium")
            ])

        # Row 2: Management & Info
        buttons.append([
            InlineKeyboardButton("📊 My Status", callback_data="menu:mystatus"),
            InlineKeyboardButton("⏳ Queue", callback_data="menu:queue")
        ])

        # Row 3: Support & Rules
        buttons.append([
            InlineKeyboardButton("📜 Rules", callback_data="menu:rules"),
            InlineKeyboardButton("🆘 Support", callback_data="menu:support")
        ])

        # Row 4: Secondary Growth & Privacy
        if state != "admin":
            buttons.append([
                InlineKeyboardButton("👥 Referrals", callback_data="menu:referrals"),
                InlineKeyboardButton("🕵️ Anonymous", callback_data="menu:anonymous")
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
