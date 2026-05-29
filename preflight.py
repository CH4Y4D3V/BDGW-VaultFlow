import asyncio
import sys
from pyrogram import Client
from pyrogram.enums import ChatType, ChatMemberStatus
from pyrogram.errors import RPCError, Forbidden
from app.config import settings
from app.bot.client import get_bot

async def verify_write(bot: Client, chat_id: int, name: str, topic_id: int = None) -> bool:
    """Authority check: Try to send and delete a message."""
    try:
        test_msg = await bot.send_message(
            chat_id=chat_id,
            text=f"🛠 <b>Preflight Write Test</b> for {name}",
            message_thread_id=topic_id,
            disable_notification=True
        )
        await test_msg.delete()
        print(f"✅ {name}: LIVE WRITE TEST SUCCESSFUL")
        return True
    except Exception as e:
        print(f"❌ {name}: LIVE WRITE TEST FAILED: {e}")
        return False

async def run_audit():
    bot = get_bot()
    try:
        await bot.start()
        print("✅ Bot connected")
    except Exception as e:
        print(f"❌ Bot connection failed: {e}")
        return
    
    destinations = {
        "VAULT": settings.VAULT_CHANNEL_ID,
        "VERIFICATION": settings.VERIFICATION_GROUP_ID,
        "NSFW": settings.NSFW_GROUP_ID,
        "PREMIUM": settings.PREMIUM_GROUP_ID,
    }
    
    print("\n--- Configuration Audit ---")
    for name, cid in destinations.items():
        print(f"{name}_ID: {cid}")

    print("\n--- Access & Permission Matrix Audit ---")
    
    # Permission Matrix Logic:
    # Chat Type  | Required Permission | Logic
    # -----------|---------------------|---------------------------------------
    # CHANNEL    | Post Messages       | me.privileges.can_post_messages
    # SUPERGROUP | Send Messages       | me.status in (ADMINISTRATOR, OWNER)
    # FORUM      | Manage Topics       | me.privileges.can_manage_topics
    
    for name, cid in destinations.items():
        if not cid:
            print(f"⚠️ {name}: ID not configured (skipping)")
            continue
            
        try:
            chat = await bot.get_chat(cid)
            
            # 1. Forum Detection (Fix Task 1)
            is_forum = (chat.type == ChatType.FORUM) or getattr(chat, "is_forum", False)
            print(f"✅ {name}: Access confirmed ('{chat.title}') type={chat.type} is_forum={is_forum}")

            if name == "VERIFICATION" and not is_forum:
                print(f"❌ {name}: Group is NOT a forum! Current type is {chat.type}. Topics will fail.")

            # 2. Permission Validation (Fix Task 2)
            try:
                me = await chat.get_member("me")
                is_admin = me.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]
                privs = me.privileges if is_admin else None
                
                if not is_admin:
                    print(f"❌ {name}: Bot is NOT an administrator!")
                else:
                    print(f"✅ {name}: Bot is an administrator")
                    
                    if chat.type == ChatType.CHANNEL:
                        if privs and privs.can_post_messages:
                            print(f"✅ {name}: Channel posting permission confirmed")
                        else:
                            print(f"❌ {name}: Bot lacks 'can_post_messages' in this channel")
                    
                    elif is_forum:
                        if privs and privs.can_manage_topics:
                            print(f"✅ {name}: Forum 'can_manage_topics' permission confirmed")
                        else:
                            print(f"❌ {name}: Bot lacks 'can_manage_topics' in this forum")
                            
                    # Note: For Supergroups, if is_admin is True, the bot can generally send messages.
                    # can_post_messages is FALSE for supergroups in Telegram API.
            
            except Exception as perm_err:
                print(f"❌ {name}: Could not check permissions: {perm_err}")

            # 3. Live Write Test (Fix Task 3)
            # For verification, we try to write to the main chat.
            # Topic-specific write tests happen during runtime, but we verify general write here.
            await verify_write(bot, cid, name)

        except Forbidden:
            print(f"❌ {name}: Bot is not a member or is banned.")
        except RPCError as e:
            print(f"❌ {name}: Telegram RPC Error: {e}")
        except Exception as e:
            print(f"❌ {name}: Unexpected Error: {e}")

    await bot.stop()
    print("\n--- Audit Complete ---")

if __name__ == "__main__":
    asyncio.run(run_audit())
