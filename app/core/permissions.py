from __future__ import annotations

import functools
from enum import Enum
from typing import Callable

from pyrogram.types import CallbackQuery, Message

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class Role(str, Enum):
    OWNER = "owner"
    ADMIN = "admin"


def get_user_roles(user_id: int) -> list[Role]:
    roles: list[Role] = []
    if user_id == settings.OWNER_ID:
        roles.append(Role.OWNER)
        roles.append(Role.ADMIN)  # Owner is always Admin
    elif user_id in (settings.ADMIN_IDS or []):
        roles.append(Role.ADMIN)
    return roles


def has_role(user_id: int, role: Role) -> bool:
    """Owner inherits all roles. Admin check covers ADMIN_IDS."""
    if user_id == settings.OWNER_ID:
        return True
    
    user_roles = get_user_roles(user_id)
    return role in user_roles


def is_admin(user_id: int) -> bool:
    """Is the user the Owner or a listed Admin?"""
    return user_id == settings.OWNER_ID or user_id in (settings.ADMIN_IDS or [])


# ── Compatibility Aliases ─────────────────────────────────────────────────────

def is_sudo(user_id: int) -> bool:
    return is_admin(user_id)

def is_super_admin(user_id: int) -> bool:
    return is_admin(user_id)

def is_moderator(user_id: int) -> bool:
    return is_admin(user_id)

def is_support_admin(user_id: int) -> bool:
    return is_admin(user_id)

def is_payment_admin(user_id: int) -> bool:
    return is_admin(user_id)

def is_scheduler_admin(user_id: int) -> bool:
    return is_admin(user_id)

def is_any_admin(user_id: int) -> bool:
    return is_admin(user_id)


# ── Startup Validation ────────────────────────────────────────────────────────

REQUIRED_ROLES = ["OWNER", "ADMIN"]
for req_role in REQUIRED_ROLES:
    if not hasattr(Role, req_role):
        raise RuntimeError(f"Invalid role reference: Role.{req_role} used but not defined.")


# ── Permission guard decorator ────────────────────────────────────────────────

def permission_required(role: Role, silent: bool = False):
    """
    Pyrogram handler decorator that enforces role-based access control.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(client, update, *args, **kwargs):
            if isinstance(update, Message):
                from_user = update.from_user
            elif isinstance(update, CallbackQuery):
                from_user = update.from_user
            else:
                return

            if not from_user:
                return

            user_id = from_user.id

            if not has_role(user_id, role):
                logger.warning(
                    "permission_access_denied",
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
                    except Exception:
                        pass
                return

            return await func(client, update, *args, **kwargs)

        return wrapper
    return decorator
