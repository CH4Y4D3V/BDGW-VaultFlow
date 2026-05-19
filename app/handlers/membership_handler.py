from __future__ import annotations

from pyrogram import Client
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import ChatMemberUpdated

from app.config import settings
from app.repositories.invite_repository import InviteRepository
from app.services.membership_service import MembershipService
from app.services.audit_service import get_audit
from app.utils.logger import get_logger

logger = get_logger(__name__)

_membership_service = MembershipService()
_invite_repo = InviteRepository()

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

# ── Managed destination chats ─────────────────────────────────────────────────

def _get_managed_destination_ids() -> frozenset[int]:
    ids = set()
    if settings.NSFW_GROUP_ID:
        ids.add(settings.NSFW_GROUP_ID)
    if settings.PREMIUM_GROUP_ID:
        ids.add(settings.PREMIUM_GROUP_ID)
    return frozenset(ids)


# ── B-02 Step 1: Identity verification helper ─────────────────────────────────

async def _verify_joiner_identity(
    client: Client,
    joiner_user_id: int,
    chat_id: int,
    actor_id: int | None,
) -> None:
    """
    B-02 Step 1: When a user joins a managed chat via invite link, verify
    that they are the intended recipient of the invite.

    Look up an active invite in our DB for (joiner_user_id, chat_id).
    If no matching invite is found → the joiner used someone else's link.
    Action:
      1. Kick (ban + immediate unban) via Telegram
      2. Revoke the invite link so it can't be used again
      3. Log INVITE_MISMATCH_KICK to the audit collection
      4. Notify the owner
    """
    try:
        invite = await _invite_repo.get_active_invite_for_user_chat(
            user_id=joiner_user_id,
            chat_id=chat_id,
        )
    except Exception as e:
        logger.error(
            "_verify_joiner_identity: invite lookup failed — skipping verification",
            extra={
                "ctx_user_id": joiner_user_id,
                "ctx_chat_id": chat_id,
                "ctx_error": str(e),
            },
            exc_info=True,
        )
        return

    if invite is not None:
        # Legitimate join — the joiner matches the intended recipient
        logger.info(
            "_verify_joiner_identity: join verified against active invite",
            extra={
                "ctx_user_id": joiner_user_id,
                "ctx_chat_id": chat_id,
                "ctx_token_prefix": invite.token[:8],
            },
        )
        return

    # No matching invite found — this joiner should not be here
    logger.warning(
        "_verify_joiner_identity: MISMATCH — no active invite for joiner; kicking",
        extra={
            "ctx_user_id": joiner_user_id,
            "ctx_chat_id": chat_id,
            "ctx_actor_id": actor_id,
        },
    )

    # 1. Kick: ban + immediate unban (kick without permanent ban)
    kicked = False
    try:
        await client.ban_chat_member(chat_id=chat_id, user_id=joiner_user_id)
        await client.unban_chat_member(chat_id=chat_id, user_id=joiner_user_id)
        kicked = True
        logger.warning(
            "_verify_joiner_identity: mismatch joiner kicked from chat",
            extra={"ctx_user_id": joiner_user_id, "ctx_chat_id": chat_id},
        )
    except Exception as e:
        logger.error(
            "_verify_joiner_identity: failed to kick mismatch joiner",
            extra={
                "ctx_user_id": joiner_user_id,
                "ctx_chat_id": chat_id,
                "ctx_error": str(e),
            },
        )

    # 2. Revoke any active invites for this chat (the one used may still be ACTIVE)
    try:
        all_active = await _invite_repo.get_active_for_chat(chat_id)
        for active_invite in all_active:
            if active_invite.telegram_link:
                try:
                    await client.revoke_chat_invite_link(
                        chat_id=chat_id,
                        invite_link=active_invite.telegram_link,
                    )
                    logger.warning(
                        "_verify_joiner_identity: revoked leaked invite link",
                        extra={
                            "ctx_chat_id": chat_id,
                            "ctx_link_prefix": active_invite.telegram_link[:30],
                        },
                    )
                except Exception as e:
                    logger.warning(
                        "_verify_joiner_identity: could not revoke invite on Telegram",
                        extra={"ctx_error": str(e)},
                    )
    except Exception as e:
        logger.error(
            "_verify_joiner_identity: failed to revoke invite links after mismatch",
            extra={"ctx_chat_id": chat_id, "ctx_error": str(e)},
        )

    # 3. Audit log
    try:
        await get_audit().log(
            action="INVITE_MISMATCH_KICK",
            performed_by=0,  # system action
            target_user_id=joiner_user_id,
            details={
                "chat_id": chat_id,
                "kicked": kicked,
                "actor_id": actor_id,
                "reason": "No active invite found for joining user — potential invite link abuse",
            },
        )
    except Exception as e:
        logger.warning(
            "_verify_joiner_identity: audit log failed (non-fatal)",
            extra={"ctx_error": str(e)},
        )

    # 4. Notify owner
    if settings.OWNER_ID:
        try:
            await client.send_message(
                chat_id=settings.OWNER_ID,
                text=(
                    f"⚠️ <b>Invite Mismatch Detected</b>\n\n"
                    f"User <code>{joiner_user_id}</code> joined chat "
                    f"<code>{chat_id}</code> without a valid invite link.\n"
                    f"Action taken: {'Kicked ✅' if kicked else 'Kick FAILED ❌'}\n\n"
                    f"All active invite links for this chat have been revoked."
                ),
                parse_mode="html",
            )
        except Exception as e:
            logger.warning(
                "_verify_joiner_identity: failed to notify owner",
                extra={"ctx_owner_id": settings.OWNER_ID, "ctx_error": str(e)},
            )


# ── Handler ───────────────────────────────────────────────────────────────────

@Client.on_chat_member_updated()
async def handle_chat_member_updated(
    client: Client,
    update: ChatMemberUpdated,
) -> None:
    """
    Track membership state transitions and persist them to the memberships
    collection via MembershipService.

    Detected transitions:
    - join        : inactive/unknown → active (member/admin/restricted)
    - left        : active → LEFT
    - kicked/ban  : active → BANNED
    - status_change: any other old→new status change (e.g. member→admin)

    B-02 Step 1: On join to a managed destination chat, verify the joiner
    matches the intended invite recipient. Mismatches are kicked immediately.

    Membership persistence is scoped to managed destination chats (NSFW, PREMIUM).
    Transitions in other chats are logged only — no DB write.
    """
    old_member = update.old_chat_member
    new_member = update.new_chat_member

    if old_member is None or new_member is None:
        return

    old_status: ChatMemberStatus = old_member.status
    new_status: ChatMemberStatus = new_member.status

    if old_status == new_status:
        return

    member_user = new_member.user or old_member.user
    if member_user is None:
        return

    user_id: int = member_user.id
    chat_id: int = update.chat.id
    chat_title: str = getattr(update.chat, "title", None) or str(chat_id)
    actor_id: int | None = update.from_user.id if update.from_user else None

    _status_str = lambda s: s.value if isinstance(s, ChatMemberStatus) else str(s)  # noqa: E731

    managed_ids = _get_managed_destination_ids()
    is_managed = chat_id in managed_ids

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
                "ctx_managed": is_managed,
            },
        )
        if is_managed:
            # B-02 Step 1: verify the joiner is the intended invite recipient
            # before recording the membership. This runs first so that mismatch
            # joiners are kicked before we write their membership to the DB.
            await _verify_joiner_identity(
                client=client,
                joiner_user_id=user_id,
                chat_id=chat_id,
                actor_id=actor_id,
            )

            try:
                await _membership_service.record_join(user_id, chat_id)
            except Exception as e:
                logger.error(
                    "handle_chat_member_updated: record_join failed",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_chat_id": chat_id,
                        "ctx_error": str(e),
                    },
                    exc_info=True,
                )
        return

    # ── Leave / Kick / Ban ───────────────────────────────────────────────────
    if old_status in _ACTIVE_STATUSES and new_status in _INACTIVE_STATUSES:
        event = "kicked" if new_status == ChatMemberStatus.BANNED else "left"
        reason = "kicked" if new_status == ChatMemberStatus.BANNED else "left"
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
                "ctx_managed": is_managed,
            },
        )
        if is_managed:
            try:
                await _membership_service.record_leave(user_id, chat_id, reason=reason)
            except Exception as e:
                logger.error(
                    "handle_chat_member_updated: record_leave failed",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_chat_id": chat_id,
                        "ctx_error": str(e),
                    },
                    exc_info=True,
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