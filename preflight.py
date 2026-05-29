import asyncio
import sys
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import RPCError, Forbidden
from app.config import settings
from app.bot.client import get_bot

async def run_audit():
    bot = get_bot()
    try:
        await bot.start()
        print("✅ Bot connected")
    except Exception as e:
        print(f"❌ Bot connection failed: {e}")
        return
    
    channels = {
        "VAULT": settings.VAULT_CHANNEL_ID,
        "VERIFICATION": settings.VERIFICATION_GROUP_ID,
        "NSFW": settings.NSFW_GROUP_ID,
        "PREMIUM": settings.PREMIUM_GROUP_ID,
        "PREMIUM_CHANNEL": settings.PREMIUM_CHANNEL_ID,
    }
    
    print("\n--- Configuration Audit ---")
    for name, cid in channels.items():
        print(f"{name}_ID: {cid}")

    print("\n--- Access & Permission Audit ---")
    for name, cid in channels.items():
        if not cid:
            print(f"⚠️ {name}: ID not configured (skipping)")
            continue
            
        try:
            chat = await bot.get_chat(cid)
            print(f"✅ {name}: Access confirmed ('{chat.title}') type={chat.type}")
            
            # Specific checks for Verification Group
            if name == "VERIFICATION":
                if not getattr(chat, "is_forum", False):
                    print("❌ VERIFICATION: Group is NOT a forum! Enable topics in group settings.")
                else:
                    print("✅ VERIFICATION: Forum mode is ENABLED")
                
                # Check permissions
                try:
                    me = await chat.get_member("me")
                    if not me.privileges:
                        print("❌ VERIFICATION: Bot is NOT an admin in this group!")
                    else:
                        privs = me.privileges
                        if not privs.can_manage_topics:
                            print("❌ VERIFICATION: Bot lacks 'can_manage_topics' permission!")
                        else:
                            print("✅ VERIFICATION: Bot has 'can_manage_topics' permission")
                        
                        if not privs.can_post_messages:
                            print("❌ VERIFICATION: Bot cannot post messages (required for card creation)!")
                except Exception as perm_err:
                    print(f"❌ VERIFICATION: Could not check permissions: {perm_err}")
            
            # Check vault permissions
            if name == "VAULT":
                try:
                    me = await chat.get_member("me")
                    if not me.privileges or not me.privileges.can_post_messages:
                        print("❌ VAULT: Bot cannot post messages to this channel!")
                    else:
                        print("✅ VAULT: Bot has posting permissions")
                        
                    # Attempt a test post
                    try:
                        test_msg = await bot.send_message(cid, "🛠 <b>Preflight Check:</b> Vault write test.")
                        print(f"✅ VAULT: Test post successful (msg_id={test_msg.id})")
                        await test_msg.delete()
                        print("✅ VAULT: Test post cleanup successful")
                    except Exception as post_err:
                        print(f"❌ VAULT: Test post failed: {post_err}")
                except Exception as perm_err:
                    print(f"❌ VAULT: Could not check permissions: {perm_err}")

            if name == "NSFW" or name == "PREMIUM":
                 try:
                    me = await chat.get_member("me")
                    if not me.privileges or not me.privileges.can_post_messages:
                        print(f"❌ {name}: Bot cannot post messages!")
                    else:
                         print(f"✅ {name}: Bot has posting permissions")
                 except Exception as e:
                     print(f"❌ {name}: Permission check failed: {e}")

        except Forbidden:
            print(f"❌ {name}: Bot is not a member or is banned from this chat.")
        except RPCError as e:
            print(f"❌ {name}: Telegram RPC Error: {e}")
        except Exception as e:
            print(f"❌ {name}: Unexpected Error: {e}")

    await bot.stop()
    print("\n--- Audit Complete ---")

if __name__ == "__main__":
    asyncio.run(run_audit())
