from __future__ import annotations

from typing import Dict, Any, List, Optional
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.ui.common import (
    SECTION_DIVIDER, 
    THIN_DIVIDER, 
    get_status_badge, 
    format_header, 
    format_info_block
)

def build_user_status_card(
    user_id: int,
    username: Optional[str],
    state: str,
    membership: Optional[Dict[str, Any]] = None,
    subscription: Optional[Dict[str, Any]] = None
) -> tuple[str, InlineKeyboardMarkup]:
    """Universal status card for the user."""
    header = format_header("My Status", "📊")
    
    user_line = f"👤 <b>User:</b> @{username} (<code>{user_id}</code>)" if username else f"👤 <b>User ID:</b> <code>{user_id}</code>"
    
    # Map state to badge
    state_badge = get_status_badge(state)
    
    sub_text = "None"
    if subscription:
        sub_text = f"💎 {subscription['plan_label']} (Expires: {subscription['expiry']})"
    
    body = (
        f"{user_line}\n"
        f"🏷 <b>Global Status:</b> {state_badge}\n"
        f"💳 <b>Subscription:</b> {sub_text}\n"
        f"{THIN_DIVIDER}\n"
    )
    
    if membership:
        body += "✅ <b>Active Memberships:</b>\n"
        for chat in membership.get("active_chats", []):
            body += f" ┣ {chat['title']}\n"
    
    buttons = [
        [InlineKeyboardButton("💎 Upgrade Premium", callback_data="menu:premium")],
        [InlineKeyboardButton("🆘 Get Support", callback_data="menu:support")],
        [InlineKeyboardButton("← Back", callback_data="menu:home")]
    ]
    
    return f"{header}\n{body}", InlineKeyboardMarkup(buttons)
