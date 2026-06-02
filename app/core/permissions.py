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
    ADMIN = "admin"


def _is_owner_or_admin(user_id: int) -> bool:
    """Core check: is this user the owner or in ADMIN_IDS?"""
    if user_id == settings.OWNER_ID:
        return True
    if user_id in settings.ADMIN_IDS:
        return True
    return False


def get_user_roles(user_id: int) -> List[Role]:
    roles: List[Role] = []
    if user_id == settings.OWNER_ID:
        roles.append(Role.OWNER)
    if user_id in settings.ADMIN_IDS:
        roles.append(Role.ADMIN)
    return roles


def has_role(user_id: int, role: Role) -> bool:
    """
    Simplified two-tier access model.
    OWNER and ADMIN both pass every role check.
    Any non-owner, non-admin user fails every role check.
    """
    return _is_owner_or_admin(user_id)


def is_sudo(user_id: int) -> bool:
    """Returns True if user is OWNER or ADMIN. Backward-compat alias."""
    return _is_owner_or_admin(user_id)


def is_moderator(user_id: int) -> bool:
    return _is_owner_or_admin(user_id)


def is_support_admin(user_id: int) -> bool:
    return _is_owner_or_admin(user_id)


def is_payment_admin(user_id: int) -> bool:
    return _is_owner_or_admin(user_id)


def is_scheduler_admin(user_id: int) -> bool:
    return _is_owner_or_admin(user_id)


def is_any_admin(user_id: int) -> bool:
    return _is_owner_or_admin(user_id)


# ── Permission guard decorator ────────────────────────────────────────────────

def permission_required(role: Role, silent: bool = False):
    """
    Pyrogram handler decorator that enforces access control.

    Works on both Message and CallbackQuery handlers.
    With the simplified two-tier model, any Role value passes for OWNER or ADMIN.
    Everyone else is denied.

    Args:
        role:   Accepted for API compatibility — ignored in logic.
                Access is granted solely based on OWNER_ID or ADMIN_IDS membership.
        silent: If True, unauthorized calls are dropped without any reply.
                If False (default), unauthorized users receive a denial message.

    Usage:
        @Client.on_message(filters.command("ban"))
        @permission_required(Role.ADMIN)
        async def handle_ban(client, message): ...

        @Client.on_callback_query(filters.regex(r"^mod_"))
        @permission_required(Role.ADMIN)
        async def handle_moderation_callback(client, callback): ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(client, update, *args, **kwargs):
            # Resolve user_id from either Message or CallbackQuery
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

            if not _is_owner_or_admin(user_id):
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