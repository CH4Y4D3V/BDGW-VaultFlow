from __future__ import annotations
from pyrogram import Client, filters
from pyrogram.types import ChatMemberUpdated
from pyrogram.enums import ChatMemberStatus
from app.config import settings

# This handler will be initialized by AppLifecycle
referral_service = None

def init_membership_handler(service):
    global referral_service
    referral_service = service

@Client.on_chat_member_updated(filters.chat(settings.VAULT_CHANNEL_ID))
async def on_channel_member_update(client: Client, update: ChatMemberUpdated):
    if referral_service is None:
        return

    new_status = update.new_chat_member.status if update.new_chat_member else None
    old_status = update.old_chat_member.status if update.old_chat_member else None
    user_id = update.from_user.id if update.from_user else (update.chat.id if update.chat else None)
    
    if not user_id:
        return

    member_statuses = [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]
    left_statuses = [ChatMemberStatus.LEFT, ChatMemberStatus.BANNED, ChatMemberStatus.RESTRICTED]

    # If old_status was member/admin/creator AND new_status is LEFT/BANNED
    if old_status in member_statuses and new_status in left_statuses:
        await referral_service.handle_member_left(user_id)

    # If old_status was LEFT/BANNED AND new_status is member
    if (old_status in left_statuses or old_status is None) and new_status in member_statuses:
        await referral_service.handle_member_rejoined(user_id, settings.VAULT_CHANNEL_ID)
