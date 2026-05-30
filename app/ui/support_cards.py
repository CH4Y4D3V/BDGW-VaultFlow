from __future__ import annotations

from typing import Dict, Any, List, Optional
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, User

from app.ui.common import (
    SECTION_DIVIDER, 
    THIN_DIVIDER, 
    get_status_badge, 
    format_header, 
    format_info_block
)

def build_support_welcome_card() -> tuple[str, InlineKeyboardMarkup]:
    """Redesigned support landing card."""
    header = format_header("Support Center", "🛟")
    
    body = (
        "Welcome to the BDGW Support Center. Our team is here to help you.\n\n"
        "<b>Response Expectations:</b>\n"
        "┣ 🕒 Average response: 15-60 mins\n"
        "┣ 📅 Working hours: 10 AM - 12 AM (UTC+6)\n"
        "┗ 📑 Process: Send your issue below to open a ticket\n\n"
        "<i>Please describe your issue in a single message with as much detail as possible.</i>"
    )
    
    buttons = [
        [InlineKeyboardButton("← Back to Menu", callback_data="menu:home")]
    ]
    
    return f"{header}\n{body}", InlineKeyboardMarkup(buttons)

def build_ticket_created_card(ticket_id: str) -> str:
    """Card shown after user sends an issue."""
    header = format_header("Ticket Created", "🎫")
    
    now = "Just now" # simplified for card
    
    body = (
        "Your support ticket has been successfully opened.\n\n"
        f"┣ 🆔 <b>Ticket ID:</b> <code>{ticket_id}</code>\n"
        f"┣ 🏷 <b>Status:</b> ⏳ <b>Awaiting Response</b>\n"
        f"┗ 🕒 <b>Created:</b> <code>{now}</code>\n\n"
        "<i>Our team has been notified. You can send additional details at any time.</i>"
    )
    
    return f"{header}\n{body}"

def build_admin_support_card(
    user: User, 
    ticket_id: str, 
    issue_summary: str,
    status: str = "pending",
    stats: Optional[dict] = None
) -> str:
    """Admin notification for new support issues."""
    header = format_header("Support Ticket", "🆘")
    
    # ── SYSTEM 10: USER PROFILE ──
    join_date = stats.get("join_date", "Unknown") if stats else "Unknown"
    sub_status = stats.get("subscription", "Free") if stats else "Free"
    total_subs = stats.get("total_submissions", 0) if stats else 0
    total_bans = stats.get("ban_count", 0) if stats else 0

    user_info = (
        "👤 <b>USER PROFILE</b>\n"
        f"┣ {format_info_block('Name', user.first_name + (' ' + user.last_name if user.last_name else ''))}\n"
        f"┣ {format_info_block('Username', '@' + user.username if user.username else 'N/A')}\n"
        f"┣ {format_info_block('User ID', user.id, code=True)}\n"
        f"┣ {format_info_block('Joined', join_date)}\n"
        f"┣ {format_info_block('Plan', sub_status.upper())}\n"
        f"┣ {format_info_block('Submissions', total_subs)}\n"
        f"┗ {format_info_block('Prev Bans', total_bans)}\n"
    )
    
    ticket_info = (
        "🎫 <b>TICKET DETAILS</b>\n"
        f"┣ {format_info_block('Ticket ID', ticket_id, code=True)}\n"
        f"┗ {format_info_block('Status', get_status_badge(status))}\n"
    )
    
    issue_box = (
        "📝 <b>ISSUE SUMMARY</b>\n"
        f"<blockquote>{issue_summary}</blockquote>"
    )
    
    return f"{header}\n{user_info}\n{ticket_info}\n{issue_box}"

def build_admin_support_actions(ticket_id: str, user_id: int, status: str = "pending") -> InlineKeyboardMarkup:
    """Buttons for admin to manage tickets."""
    buttons = []
    
    if status == "pending":
        buttons.append([
            InlineKeyboardButton("✅ Accept Support", callback_data=f"support:accept:{user_id}")
        ])
    
    buttons.append([
        InlineKeyboardButton("💬 Reply", callback_data=f"support:reply:{user_id}"),
        InlineKeyboardButton("✅ Resolve", callback_data=f"support:resolve:{ticket_id}")
    ])
    
    buttons.append([
        InlineKeyboardButton("👤 User Profile", url=f"tg://user?id={user_id}"),
        InlineKeyboardButton("🚫 Close Ticket", callback_data=f"support:close:{ticket_id}")
    ])
    
    return InlineKeyboardMarkup(buttons)
