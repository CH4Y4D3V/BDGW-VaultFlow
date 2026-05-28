# -*- coding: utf-8 -*-
import asyncio, sys, random
sys.path.insert(0, '.')

async def main():
    from app.config import settings
    from pyrogram import Client, raw
    client = Client(
        name=settings.SESSION_NAME,
        api_id=settings.API_ID,
        api_hash=settings.API_HASH,
        bot_token=settings.BOT_TOKEN,
    )
    async with client:
        chat = await client.get_chat(settings.VERIFICATION_GROUP_ID)
        print(f'Chat: {chat.title} | Type: {chat.type}')
        try:
            peer = await client.resolve_peer(settings.VERIFICATION_GROUP_ID)
            result = await client.invoke(
                raw.functions.channels.CreateForumTopic(
                    channel=peer,
                    title='Test Topic delete me',
                    random_id=random.randint(1, 2**31 - 1),
                )
            )
            topic_id = None
            for update in result.updates:
                if hasattr(update, 'id'):
                    topic_id = update.id
                    break
            print(f'SUCCESS topic_id={topic_id}')
            await client.invoke(
                raw.functions.channels.DeleteTopicHistory(
                    channel=peer,
                    top_msg_id=topic_id,
                )
            )
            print('Topic deleted. Raw API works correctly.')
        except Exception as e:
            print(f'FAILED: {type(e).__name__}: {e}')

asyncio.run(main())
