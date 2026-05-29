import asyncio
import sys
from pyrogram import Client
from pyrogram.enums import ChatType
from app.config import settings
from app.bot.client import get_bot

async def run_audit():
    bot = get_bot()
    await bot.start()
    print("✅ Bot connected")
    
    channels = {
        "VAULT": settings.VAULT_CHANNEL_ID,
        "VERIFICATION": settings.VERIFICATION_GROUP_ID,
        "NSFW": settings.NSFW_GROUP_ID,
        "PREMIUM": settings.PREMIUM_GROUP_ID,
    }
    
    for name, cid in channels.items():
        if not cid:
            print(f"❌ {name}: ID not configured")
            continue
            
        try:
            chat = await bot.get_chat(cid)
            print(f"✅ {name}: Access confirmed ('{chat.title}')")
            
            # Specific checks for Verification Group
            if name == "VERIFICATION":
                if not getattr(chat, "is_forum", False):
                    print("❌ VERIFICATION: Group is NOT a forum! Enable topics.")
                else:
                    print("✅ VERIFICATION: Forum enabled")
                
                # Check permissions
                me = await chat.get_member("me")
                if not me.privileges:
                    print("❌ VERIFICATION: Bot is not an admin!")
                elif not me.privileges.can_manage_topics:
                    print("❌ VERIFICATION: Bot lacks 'can_manage_topics' permission!")
                else:
                    print("✅ VERIFICATION: Admin with manage_topics permission")
            
            # Check vault permissions
            if name == "VAULT":
                me = await chat.get_member("me")
                if not me.privileges or not me.privileges.can_post_messages:
                    print("❌ VAULT: Bot cannot post messages!")
                else:
                    print("✅ VAULT: Can post messages")
                    
        except Exception as e:
            print(f"❌ {name}: Error: {e}")

    await bot.stop()

if __name__ == "__main__":
    asyncio.run(run_audit())
