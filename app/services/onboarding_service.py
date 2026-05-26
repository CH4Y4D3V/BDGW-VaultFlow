from __future__ import annotations

from enum import Enum
from typing import Tuple

from pyrogram.types import InlineKeyboardMarkup

from app.bot.keyboards import KeyboardBuilder
from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.repositories.subscription_repository import SubscriptionRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class UserState(str, Enum):
    NEW = "new"
    RETURNING = "returning"
    PREMIUM = "premium"
    ADMIN = "admin"
    BANNED = "banned"


class OnboardingService:
    def __init__(self, subscription_repo: SubscriptionRepository):
        self.subscription_repo = subscription_repo

    async def get_user_state(self, user_id: int) -> UserState:
        sub = await self.subscription_repo.get_by_user_id(user_id)
        
        if not sub:
            return UserState.NEW
        
        if sub.status == SubscriptionStatus.BANNED:
            return UserState.BANNED
            
        if sub.plan in [Plan.OWNER, Plan.SUDO, Plan.ADMIN]:
            return UserState.ADMIN
            
        if sub.status == SubscriptionStatus.ACTIVE and sub.plan != Plan.FREE:
            return UserState.PREMIUM
            
        if sub.status in [SubscriptionStatus.EXPIRED, SubscriptionStatus.GRACE]:
            return UserState.RETURNING
            
        return UserState.RETURNING  # Default for free or existing users

    async def render_onboarding(self, user_id: int, first_name: str) -> Tuple[str, InlineKeyboardMarkup]:
        state = await self.get_user_state(user_id)
        
        text = self._get_template(state, first_name)
        keyboard = KeyboardBuilder.build_main_menu(state)
        
        return text, keyboard

    def _get_template(self, state: UserState, first_name: str) -> str:
        header = "✨ <b>VAULTFLOW PREMIER</b>\n\n"
        
        if state == UserState.NEW:
            body = (
                f"Welcome, <b>{first_name}</b>.\n"
                "You've entered the premier automation ecosystem for content creators and distributors.\n\n"
                "🚀 <b>Getting Started</b>\n"
                "VaultFlow streamlines your workflow, offering high-speed delivery, "
                "automated watermarking, and cross-channel distribution.\n\n"
                "<i>Tap below to explore our features.</i>"
            )
        elif state == UserState.PREMIUM:
            body = (
                f"Welcome back, <b>{first_name}</b>.\n"
                "Your <b>Premium Access</b> is active. You have full priority "
                "on all distribution pipelines.\n\n"
                "💎 <b>Member Benefits</b>\n"
                "• Instant delivery\n"
                "• Custom watermarking\n"
                "• Extended storage\n"
                "• Priority support\n\n"
                "What would you like to manage today?"
            )
        elif state == UserState.ADMIN:
            body = (
                f"Greetings, <b>{first_name}</b>.\n"
                "System console active. All infrastructure modules are nominal.\n\n"
                "🛠 <b>Management</b>\n"
                "Access administrative tools and moderation queues via the menu below."
            )
        elif state == UserState.BANNED:
            body = (
                "🚫 <b>Access Restricted</b>\n\n"
                "Your account has been suspended for violating our terms of service.\n\n"
                "If you believe this is a mistake, contact support."
            )
        else:  # RETURNING or Default
            body = (
                f"Welcome back, <b>{first_name}</b>.\n"
                "Ready to resume your content operations?\n\n"
                "⚡️ <b>Quick Actions</b>\n"
                "Submit new content or check your existing queue status below.\n\n"
                "⭐️ <i>Upgrade to Premium for 10x faster delivery.</i>"
            )
            
        footer = "\n\n━━━━━━━━━━━━━━━━━━━━━━"
        return f"{header}{body}{footer}"
