from __future__ import annotations

from enum import Enum
from typing import Tuple, Optional

from pyrogram.types import InlineKeyboardMarkup

from app.bot.keyboards import KeyboardBuilder
from app.models.subscription import Plan, Subscription, SubscriptionStatus
from app.repositories.subscription_repository import SubscriptionRepository
from app.repositories.user_repository import UserRepository
from app.ui.welcome_cards import build_welcome_card_v3, build_onboarding_keyboard
from app.utils.logger import get_logger

logger = get_logger(__name__)


class UserState(str, Enum):
    NEW = "new"
    RETURNING = "returning"
    PREMIUM = "premium"
    ADMIN = "admin"
    BANNED = "banned"


class OnboardingService:
    def __init__(self, subscription_repo: SubscriptionRepository, user_repo: UserRepository):
        self.subscription_repo = subscription_repo
        self.user_repo = user_repo

    async def get_user_state(self, user_id: int) -> UserState:
        user = await self.user_repo.get_user_model(user_id)
        if user and user.is_banned:
            return UserState.BANNED
            
        sub = await self.subscription_repo.get_by_user_id(user_id)
        if not sub:
            return UserState.NEW
        
        if sub.status == SubscriptionStatus.BANNED:
            return UserState.BANNED
            
        if sub.plan in [Plan.OWNER, Plan.SUDO, Plan.ADMIN]:
            return UserState.ADMIN
            
        if sub.status == SubscriptionStatus.ACTIVE and sub.plan != Plan.FREE:
            return UserState.PREMIUM
            
        return UserState.RETURNING

    async def render_start(self, user_id: int, first_name: str) -> Tuple[str, InlineKeyboardMarkup]:
        """
        Determines whether to show onboarding or main menu.
        """
        user = await self.user_repo.get_user_model(user_id)
        
        # If not onboarded (new user or hasnt accepted terms), show welcome card v3
        if not user or not user.onboarded:
            text = build_welcome_card_v3(first_name)
            keyboard = build_onboarding_keyboard()
            return text, keyboard
            
        # If onboarded, show main menu
        state = await self.get_user_state(user_id)
        text = self._get_main_menu_text(state, first_name)
        keyboard = KeyboardBuilder.build_main_menu(state.value)
        
        return text, keyboard

    async def complete_onboarding(self, user_id: int) -> bool:
        """Mark user as onboarded."""
        return await self.user_repo.set_onboarded(user_id, True)

    def _get_main_menu_text(self, state: UserState, first_name: str) -> str:
        if state == UserState.BANNED:
            return (
                "🚫 <b>Access Restricted</b>\n\n"
                "Your account has been suspended.\n"
                "Contact support if you believe this is a mistake."
            )

        return (
            f"👋 <b>Welcome back, {first_name}!</b>\n\n"
            "What would you like to do today?\n"
            "Use the vertical menu below to navigate."
        )
