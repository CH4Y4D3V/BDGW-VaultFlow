"""
app/core/permissions.py
=======================
Role-based access control for the BDGW VaultFlow platform.

Spec reference: Section 19 — Admin Roles
  Exactly TWO roles exist: OWNER and ADMIN. No others.
  Role assignments are persisted in MongoDB (admins collection,
  Section 25A.18) and are the single source of truth at runtime.

Design notes:
  • has_role() is async — it queries the admins collection via Motor.
  • permission_required() is a Pyrogram handler decorator; its inner
    wrapper is already async, so the await is cost-free from the
    Pyrogram dispatcher's perspective.
  • settings.OWNER_ID is honoured as a bootstrap fallback so that the
    platform owner can operate the bot before any DB records exist
    (e.g. first startup).  Once seeded, the DB record takes precedence.
  • FloodWait is caught and retried once when sending denial messages.
  • All DB failures default to DENY (fail-closed), never fail-open.
"""

from __future__ import annotations

import asyncio
import functools
from enum import Enum
from typing import Callable, Optional

from pyrogram.errors import FloodWait
from pyrogram.types import CallbackQuery, Message

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Role definition
# ──────────────────────────────────────────────────────────────────────────────

class Role(str, Enum):
    """
    Platform roles as defined in spec Section 19.

    Exactly two roles exist; all previously defined roles (SUPER_ADMIN,
    SUDO, MODERATOR, SUPPORT_ADMIN, PAYMENT_ADMIN, SCHEDULER_ADMIN) have
    been removed from the spec and must not exist in code.

    Hierarchy:
        OWNER  – full platform control; inherits all ADMIN capabilities.
        ADMIN  – moderation and operational access; cannot assign roles.
    """

    OWNER = "owner"
    ADMIN = "admin"


# ──────────────────────────────────────────────────────────────────────────────
# DB-backed role resolution
# ──────────────────────────────────────────────────────────────────────────────

async def get_admin_record(user_id: int) -> Optional[dict]:
    """
    Fetch the active admin document for *user_id* from the MongoDB
    ``admins`` collection (spec Section 25A.18).

    Returns the raw document dict if the user is an active admin/owner,
    or ``None`` if not found, not active, or on any DB error.

    DB errors are logged at ERROR level and cause a ``None`` return so
    that the caller (``has_role``) can default to DENY safely.

    Args:
        user_id: The Telegram user ID to look up.

    Returns:
        The admin document dict, or ``None``.
    """
    # Deferred import prevents circular dependencies (AdminRepository
    # may import helpers from this module indirectly).
    from app.repositories.admin_repository import AdminRepository

    repo = AdminRepository()
    try:
        record = await repo.get_active_by_user_id(user_id)
        return record
    except Exception as exc:
        logger.error(
            "get_admin_record: DB query failed — defaulting to no-role",
            extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
        )
        return None


async def has_role(user_id: int, required_role: Role) -> bool:
    """
    Return ``True`` if *user_id* holds at least *required_role*.

    Role hierarchy (spec Section 19):
        OWNER   → satisfies any role check (inherits ADMIN).
        ADMIN   → satisfies Role.ADMIN checks only.

    Bootstrap fallback:
        If ``settings.OWNER_ID`` matches *user_id*, access is granted
        immediately without a DB round-trip.  This ensures the platform
        owner can always operate the bot, even before the admins
        collection is seeded.

    Fail-closed:
        Any DB error or unrecognised role value results in ``False``.

    Args:
        user_id:       The Telegram user ID being checked.
        required_role: The minimum ``Role`` needed.

    Returns:
        ``True`` if the user holds the required role, ``False`` otherwise.
    """
    # Bootstrap: owner bypass before any DB I/O
    if user_id == settings.OWNER_ID:
        return True

    record = await get_admin_record(user_id)
    if record is None:
        return False

    role_str: str = record.get("role", "")
    try:
        user_role = Role(role_str)
    except ValueError:
        logger.warning(
            "has_role: unrecognised role string in DB — denying",
            extra={"ctx_user_id": user_id, "ctx_role_value": role_str},
        )
        return False

    # OWNER inherits every role
    if user_role == Role.OWNER:
        return True

    # ADMIN satisfies an ADMIN requirement only
    return user_role == required_role


# ──────────────────────────────────────────────────────────────────────────────
# Convenience helpers
# ──────────────────────────────────────────────────────────────────────────────

async def is_owner(user_id: int) -> bool:
    """
    Return ``True`` if *user_id* is the platform OWNER.

    Uses the bootstrap shortcut (``settings.OWNER_ID``) first, then
    falls back to a DB lookup for cases where the owner record is
    stored in MongoDB with ``role="owner"`` under a different user_id
    than the ENV setting.

    Args:
        user_id: The Telegram user ID to check.

    Returns:
        ``True`` if the user is the OWNER.
    """
    if user_id == settings.OWNER_ID:
        return True
    record = await get_admin_record(user_id)
    if record is None:
        return False
    return record.get("role") == Role.OWNER.value


async def is_admin_or_owner(user_id: int) -> bool:
    """
    Return ``True`` if *user_id* is an active ADMIN or OWNER.

    This is the standard check for all hub commands (spec Section 9.5).

    Args:
        user_id: The Telegram user ID to check.

    Returns:
        ``True`` if the user is OWNER or ADMIN.
    """
    return await has_role(user_id, Role.ADMIN)


async def is_moderator(user_id: int) -> bool:
    """
    Legacy alias for is_admin_or_owner.
    Required for backward compatibility in several handlers.
    """
    return await is_admin_or_owner(user_id)


# ──────────────────────────────────────────────────────────────────────────────
# Denial helper (FloodWait-safe)
# ──────────────────────────────────────────────────────────────────────────────

_DENIAL_TEXT = "⛔ You are not authorised to perform this action."


async def _send_denial(update: Message | CallbackQuery) -> None:
    """
    Send an access-denied message to the user.

    Handles ``FloodWait`` by sleeping for the required duration and
    retrying once.  All remaining failures are caught and logged at
    DEBUG level so that a denial-send failure never propagates to the
    Pyrogram dispatcher.

    Args:
        update: The ``Message`` or ``CallbackQuery`` that triggered
                the denied handler.
    """

    async def _attempt() -> None:
        if isinstance(update, CallbackQuery):
            await update.answer(_DENIAL_TEXT, show_alert=True)
        elif isinstance(update, Message):
            await update.reply_text(_DENIAL_TEXT)

    try:
        await _attempt()
    except FloodWait as fw:
        logger.warning(
            "_send_denial: FloodWait encountered — sleeping then retrying",
            extra={"ctx_wait_seconds": fw.value},
        )
        await asyncio.sleep(fw.value)
        try:
            await _attempt()
        except Exception as retry_exc:
            logger.debug(
                "_send_denial: retry after FloodWait also failed",
                extra={"ctx_error": str(retry_exc)},
            )
    except Exception as exc:
        logger.debug(
            "_send_denial: could not deliver denial message",
            extra={"ctx_error": str(exc)},
        )


# ──────────────────────────────────────────────────────────────────────────────
# Decorator
# ──────────────────────────────────────────────────────────────────────────────

def permission_required(role: Role, silent: bool = False) -> Callable:
    """
    Pyrogram handler decorator that enforces role-based access control.

    Behaviour:
        1. Extracts ``from_user`` from a ``Message`` or ``CallbackQuery``.
        2. Awaits an async, DB-backed ``has_role()`` check.
        3. On success, delegates to the decorated handler.
        4. On failure:
             - Logs the denial at WARNING level.
             - Unless ``silent=True``, sends a denial reply with
               explicit ``FloodWait`` handling.
             - Returns ``None`` (suppresses the handler invocation).
        5. Any unexpected exception from ``has_role`` is caught, logged
           at ERROR level, and treated as a denial (fail-closed).

    Compatible with both ``Message`` handlers and ``CallbackQuery``
    handlers.  Unknown update types are silently skipped with a WARNING
    log.

    Args:
        role:   The minimum ``Role`` required to invoke the handler.
        silent: When ``True``, denied users receive no reply.

    Returns:
        A decorator that wraps the target Pyrogram handler.

    Example::

        @permission_required(Role.ADMIN)
        async def handle_ban(client, message):
            ...

        @permission_required(Role.OWNER, silent=True)
        async def handle_owner_only(client, callback_query):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(client, update, *args, **kwargs):
            # ── Resolve sender ────────────────────────────────────────
            if isinstance(update, (Message, CallbackQuery)):
                from_user = update.from_user
            else:
                logger.warning(
                    "permission_required: unknown update type — handler skipped",
                    extra={
                        "ctx_update_type": type(update).__name__,
                        "ctx_required_role": role.value,
                        "ctx_handler": func.__name__,
                    },
                )
                return

            if not from_user:
                logger.warning(
                    "permission_required: update has no from_user — handler skipped",
                    extra={"ctx_handler": func.__name__},
                )
                return

            user_id: int = from_user.id

            # ── Async DB-backed role check (fail-closed) ──────────────
            try:
                authorised: bool = await has_role(user_id, role)
            except Exception as exc:
                logger.error(
                    "permission_required: has_role raised unexpectedly — denying",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_required_role": role.value,
                        "ctx_handler": func.__name__,
                        "ctx_error": str(exc),
                    },
                )
                authorised = False

            if not authorised:
                logger.warning(
                    "permission_required: access denied",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_required_role": role.value,
                        "ctx_handler": func.__name__,
                    },
                )
                if not silent:
                    await _send_denial(update)
                return

            # ── Delegate to handler ───────────────────────────────────
            return await func(client, update, *args, **kwargs)

        return wrapper

    return decorator
