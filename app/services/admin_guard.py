"""
admin_guard.py
──────────────
Atomic admin ownership locking.

Uses claim_admin_action() — atomic SQLite UPDATE WHERE handled_at IS NULL.
Restart-safe: persisted in admin_action_state table.
Call claim_callback_action() at the start of every admin callback handler.

Returns True  → this admin owns the action, proceed.
Returns False → already claimed, admin shown an alert, handler must return.
"""

import logging
from typing import Optional

from aiogram.types import CallbackQuery

from database.repository import Database

log = logging.getLogger(__name__)


async def claim_callback_action(
    callback: CallbackQuery,
    db: Database,
    entity_type: str,
    entity_id: int,
    action: str,
    target_user_id: Optional[int] = None,
) -> bool:
    """
    Atomically claim ownership of an admin action.

    Safe to call from multiple concurrent handlers — only one will return True.
    The losing admin sees a popup showing who already claimed it.

    Args:
        callback:       The callback query being processed.
        db:             Database instance.
        entity_type:    Ownership namespace e.g. "payment_review".
        entity_id:      Unique ID within that namespace e.g. payment_id.
        action:         Human-readable action name e.g. "APPROVED".
        target_user_id: Optional: the user this action affects.

    Returns:
        True  if this admin successfully claimed ownership.
        False if another admin already holds ownership (alert sent to callback).
    """
    admin_id = callback.from_user.id
    admin_username = callback.from_user.username

    claimed = await db.claim_admin_action(
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        admin_id=admin_id,
        admin_username=admin_username,
        target_user_id=target_user_id,
    )

    if not claimed:
        state = await db.get_admin_action_state(entity_type, entity_id)
        handler_info = _format_handler(state)
        log.info(
            "[GUARD] Rejected admin %d on %s:%s — already claimed by %s",
            admin_id, entity_type, entity_id, handler_info,
        )
        await callback.answer(
            f"⚠️ Already being handled by {handler_info}. No action taken.",
            show_alert=True,
        )
        return False

    log.info(
        "[GUARD] Admin %d (@%s) claimed %s:%s — action=%s",
        admin_id, admin_username or "?", entity_type, entity_id, action,
    )
    return True


def _format_handler(state: Optional[dict]) -> str:
    if not state:
        return "another admin"
    username = state.get("handled_by_username")
    admin_id = state.get("handled_by")
    action = state.get("action", "")
    name = f"@{username}" if username else (f"Admin {admin_id}" if admin_id else "another admin")
    return f"{name} ({action})" if action else name