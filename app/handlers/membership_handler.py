from __future__ import annotations
from pyrogram import Client, filters
from pyrogram.types import ChatMemberUpdated
from pyrogram.enums import ChatMemberStatus
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# This handler will be initialized by AppLifecycle
referral_service = None

def init_membership_handler(service):
    global referral_service
    referral_service = service

# FIX: was filters.chat(settings.VAULT_CHANNEL_ID) — the vault channel is the
# admin content store, not the user-facing community channel. Referral credits
# are triggered by users joining MAIN_CHANNEL_ID (spec §16). This handler was
# watching the wrong channel and has never fired for any real user join event.
@Client.on_chat_member_updated(filters.chat(settings.MAIN_CHANNEL_ID))
async def on_channel_member_update(client: Client, update: ChatMemberUpdated):
    logger.info(
        "HANDLER: on_channel_member_update entered",
        extra={
            "ctx_chat_id": update.chat.id if update.chat else None,
            "ctx_user_id": (
                update.new_chat_member.user.id
                if update.new_chat_member and update.new_chat_member.user
                else None
            ),
        },
    )
    if referral_service is None:
        return

    new_status = update.new_chat_member.status if update.new_chat_member else None
    old_status = update.old_chat_member.status if update.old_chat_member else None

    # FIX: was update.from_user.id if update.from_user else (update.chat.id if
    # update.chat else None). The fallback to update.chat.id returned the
    # CHANNEL's own Telegram ID (not a user ID), causing handle_member_left()
    # and handle_member_rejoined() to be called with the channel ID as user_id.
    # Correct resolution order: prefer new_chat_member.user (most reliable for
    # join/leave events), then from_user (updater/actor), then give up.
    if update.new_chat_member and update.new_chat_member.user:
        user_id = update.new_chat_member.user.id
    elif update.from_user:
        user_id = update.from_user.id
    else:
        return  # Cannot identify the user — skip

    member_statuses = [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]
    left_statuses = [ChatMemberStatus.LEFT, ChatMemberStatus.BANNED, ChatMemberStatus.RESTRICTED]

    # If old_status was member/admin/creator AND new_status is LEFT/BANNED
    if old_status in member_statuses and new_status in left_statuses:
        await referral_service.handle_member_left(user_id)

    # If old_status was LEFT/BANNED AND new_status is member
    if (old_status in left_statuses or old_status is None) and new_status in member_statuses:
        await referral_service.handle_member_rejoined(user_id, settings.MAIN_CHANNEL_ID)
