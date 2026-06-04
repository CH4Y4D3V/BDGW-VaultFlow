from __future__ import annotations

import asyncio
import functools
from enum import Enum
from typing import Callable, List

from pyrogram.types import CallbackQuery, Message

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class Role(str, Enum):
    OWNER = "owner"
    SUPER_ADMIN = "super_admin"
    MODERATOR = "moderator"
    SUPPORT_ADMIN = "support_admin"
    PAYMENT_ADMIN = "payment_admin"
    SCHEDULER_ADMIN = "scheduler_admin"


def get_user_roles(user_id: int) -> List[Role]:
    roles: List[Role] = []
    if user_id == settings.OWNER_ID:
        roles.append(Role.OWNER)
    if user_id in settings.SUDO_IDS:
        roles.append(Role.SUPER_ADMIN)
    if user_id in settings.ADMIN_IDS or user_id in settings.MODERATOR_IDS:
        roles.append(Role.MODERATOR)
    if user_id in settings.SUPPORT_ADMIN_IDS:
        roles.append(Role.SUPPORT_ADMIN)
    if user_id in settings.PAYMENT_ADMIN_IDS:
        roles.append(Role.PAYMENT_ADMIN)
    if user_id in settings.SCHEDULER_ADMIN_IDS:
        roles.append(Role.SCHEDULER_ADMIN)
    return roles


def has_role(user_id: int, role: Role) -> bool:
    """OWNER and SUPER_ADMIN inherit all roles."""
    if user_id == settings.OWNER_ID:
        return True
    if user_id in settings.SUDO_IDS:
        return True
    return role in get_user_roles(user_id)


def is_moderator(user_id: int) -> bool:
    return has_role(user_id, Role.MODERATOR)


def is_support_admin(user_id: int) -> bool:
    return has_role(user_id, Role.SUPPORT_ADMIN)


def is_payment_admin(user_id: int) -> bool:
    return has_role(user_id, Role.PAYMENT_ADMIN)


def is_scheduler_admin(user_id: int) -> bool:
    return has_role(user_id, Role.SCHEDULER_ADMIN)


def is_any_admin(user_id: int) -> bool:
    return bool(get_user_roles(user_id))


# ── Permission guard decorator ────────────────────────────────────────────────

def permission_required(role: Role, silent: bool = False):
    """
    Pyrogram handler decorator that enforces role-based access control.

    Works on both Message and CallbackQuery handlers.

    Args:
        role:   The minimum Role required to execute the handler.
        silent: If True, unauthorized calls are dropped without any reply.
                If False (default), unauthorized users receive a denial message.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(client, update, *args, **kwargs):
            if isinstance(update, Message):
                from_user = update.from_user
            elif isinstance(update, CallbackQuery):
                from_user = update.from_user
            else:
                logger.warning(
                    "permission_required: unknown update type",
                    extra={"ctx_type": type(update).__name__, "ctx_role": role.value},
                )
                return

            if not from_user:
                return

            user_id = from_user.id

            if not has_role(user_id, role):
                logger.warning(
                    "permission_required: access denied",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_required_role": role.value,
                        "ctx_handler": func.__name__,
                    },
                )
                if not silent:
                    try:
                        if isinstance(update, CallbackQuery):
                            await update.answer(
                                "⛔ You are not authorised to perform this action.",
                                show_alert=True,
                            )
                        elif isinstance(update, Message):
                            await update.reply_text(
                                "⛔ You are not authorised to perform this action."
                            )
                    except Exception as e:
                        logger.debug(
                            "permission_required: could not send denial",
                            extra={"ctx_error": str(e)},
                        )
                return

            return await func(client, update, *args, **kwargs)

        return wrapper
    return decorator