from __future__ import annotations

import functools
from enum import Enum
from typing import Callable, Set

from pyrogram.types import CallbackQuery, Message

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class Role(str, Enum):
    OWNER = "owner"
    SUDO = "sudo"
    MODERATOR = "moderator"
    SUPPORT_ADMIN = "support_admin"
    PAYMENT_ADMIN = "payment_admin"
    SCHEDULER_ADMIN = "scheduler_admin"


def get_user_roles(user_id: int) -> Set[Role]:
    """Returns all roles assigned to a given user."""
    roles: Set[Role] = set()
    if user_id == settings.OWNER_ID:
        roles.add(Role.OWNER)

    # For backward compatibility and migration, ADMIN_IDS grants SUDO role
    if user_id in settings.ADMIN_IDS:
        roles.add(Role.SUDO)

    if user_id in settings.SUDO_IDS:
        roles.add(Role.SUDO)
    if user_id in settings.MODERATOR_IDS:
        roles.add(Role.MODERATOR)
    if user_id in settings.SUPPORT_ADMIN_IDS:
        roles.add(Role.SUPPORT_ADMIN)
    if user_id in settings.PAYMENT_ADMIN_IDS:
        roles.add(Role.PAYMENT_ADMIN)
    if user_id in settings.SCHEDULER_ADMIN_IDS:
        roles.add(Role.SCHEDULER_ADMIN)
    return roles


def has_role(user_id: int, required_role: Role) -> bool:
    """
    Checks if a user has the required role, respecting hierarchy.
    - OWNER has all permissions.
    - SUDO has all permissions except OWNER.
    """
    user_roles = get_user_roles(user_id)
    if not user_roles:
        return False

    if Role.OWNER in user_roles:
        return True

    if Role.SUDO in user_roles and required_role != Role.OWNER:
        return True

    return required_role in user_roles


def permission_required(role: Role, silent: bool = False):
    """
    Pyrogram handler decorator that enforces role-based access control.

    Checks if a user has the specified `role` or a higher-ranking role.
    Works on both Message and CallbackQuery handlers.

    Args:
        role:   The minimum role required to access the decorated handler.
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

            if not has_role(from_user.id, role):
                logger.warning(
                    "permission_required: access denied",
                    extra={
                        "ctx_user_id": from_user.id,
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
