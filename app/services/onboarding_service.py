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
        if state == UserState.BANNED:
            return (
                "🚫 <b>Access Restricted</b>\n\n"
                "Your account has been suspended.\n"
                "Contact support if you believe this is a mistake."
            )

        return (
            "Welcome to BDGW.\n\n"
            "Send content, stay anonymous, access premium — all in one place.\n\n"
            "Use the menu below."
        )
