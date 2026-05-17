from __future__ import annotations

from pyrogram import Client
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import ChatMemberUpdated

from app.utils.logger import get_logger

logger = get_logger(__name__)

# ── Status category sets ──────────────────────────────────────────────────────

_ACTIVE_STATUSES: frozenset[ChatMemberStatus] = frozenset({
    ChatMemberStatus.OWNER,
    ChatMemberStatus.ADMINISTRATOR,
    ChatMemberStatus.MEMBER,
    ChatMemberStatus.RESTRICTED,
})

_INACTIVE_STATUSES: frozenset[ChatMemberStatus] = frozenset({
    ChatMemberStatus.LEFT,
    ChatMemberStatus.BANNED,
})


# ── Handler ───────────────────────────────────────────────────────────────────

@Client.on_chat_member_updated()
async def handle_chat_member_updated(
    client: Client,
    update: ChatMemberUpdated,
) -> None:
    """
    Log all membership state transitions as structured events.

    Detected transitions:
    - join        : inactive/unknown → active (member/admin/restricted)
    - left        : active → LEFT
    - kicked/ban  : active → BANNED
    - status_change: any other old→new status change (e.g. member→admin)

    No business logic is executed here.  This handler is the data-capture
    layer for future analytics or enforcement pipelines.
    """
    old_member = update.old_chat_member
    new_member = update.new_chat_member

    # Guard: both sides must be populated for a meaningful diff
    if old_member is None or new_member is None:
        return

    old_status: ChatMemberStatus = old_member.status
    new_status: ChatMemberStatus = new_member.status

    # No transition to log
    if old_status == new_status:
        return

    # Resolve the member being acted upon; prefer new_member.user (always present)
    member_user = new_member.user or old_member.user
    if member_user is None:
        return

    user_id: int = member_user.id
    chat_id: int = update.chat.id
    chat_title: str = getattr(update.chat, "title", None) or str(chat_id)
    actor_id: int | None = update.from_user.id if update.from_user else None

    _status_str = lambda s: s.value if isinstance(s, ChatMemberStatus) else str(s)  # noqa: E731

    # ── Join ─────────────────────────────────────────────────────────────────
    if old_status in _INACTIVE_STATUSES and new_status in _ACTIVE_STATUSES:
        logger.info(
            "Member joined chat",
            extra={
                "ctx_event": "join",
                "ctx_user_id": user_id,
                "ctx_chat_id": chat_id,
                "ctx_chat_title": chat_title,
                "ctx_new_status": _status_str(new_status),
                "ctx_actor_id": actor_id,
            },
        )
        return

    # ── Leave / Kick / Ban ───────────────────────────────────────────────────
    if old_status in _ACTIVE_STATUSES and new_status in _INACTIVE_STATUSES:
        event = "kicked" if new_status == ChatMemberStatus.BANNED else "left"
        logger.info(
            "Member removed or left chat",
            extra={
                "ctx_event": event,
                "ctx_user_id": user_id,
                "ctx_chat_id": chat_id,
                "ctx_chat_title": chat_title,
                "ctx_old_status": _status_str(old_status),
                "ctx_new_status": _status_str(new_status),
                "ctx_actor_id": actor_id,
            },
        )
        return

    # ── Role / permission change ─────────────────────────────────────────────
    logger.info(
        "Member status changed in chat",
        extra={
            "ctx_event": "status_change",
            "ctx_user_id": user_id,
            "ctx_chat_id": chat_id,
            "ctx_chat_title": chat_title,
            "ctx_old_status": _status_str(old_status),
            "ctx_new_status": _status_str(new_status),
            "ctx_actor_id": actor_id,
        },
    )
