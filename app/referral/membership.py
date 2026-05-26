from __future__ import annotations
from pyrogram import Client
from pyrogram.enums import ChatMemberStatus
from app.config import settings

async def is_channel_member(bot: Client, user_id: int) -> bool:
    """
    Checks if a user is currently a member of the main vault channel.
    """
    try:
        # Use VAULT_CHANNEL_ID as the requirement for referral qualification
        member = await bot.get_chat_member(settings.VAULT_CHANNEL_ID, user_id)
        return member.status in [
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER
        ]
    except Exception:
        return False
