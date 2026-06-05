from __future__ import annotations

from .support_service import get_support_service, build_accept_markup

async def forward_to_topic(bot, db, settings, message):
    return await get_support_service().handle_user_message(bot, message)

async def notify_to_topic(bot, db, settings, user_id, text, reply_markup=None, **kwargs):
    return await get_support_service().notify_to_topic(bot, user_id, text, reply_markup, **kwargs)
