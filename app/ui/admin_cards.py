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

def build_admin_payment_review_card(
    user: User, 
    session: Any, 
    plan: Dict[str, Any],
    support_status: str = "available"
) -> str:
    """The master admin review card with all requested sections."""
    header = format_header("Admin Payment Review", "🔎")
    
    # 1. User Information Section
    user_info = (
        "👤 <b>USER INFORMATION</b>\n"
        f"┣ {format_info_block('Name', user.first_name + (' ' + user.last_name if user.last_name else ''))}\n"
        f"┣ {format_info_block('Username', '@' + user.username if user.username else 'N/A')}\n"
        f"┣ {format_info_block('User ID', user.id, code=True)}\n"
        f"┗ 🔗 <a href='tg://user?id={user.id}'>Account Link</a>\n"
    )
    
    # 2. Subscription Information
    price_val = plan.get('price', '0')
    sub_info = (
        "💎 <b>SUBSCRIPTION</b>\n"
        f"┣ {format_info_block('Plan', plan['label'])}\n"
        f"┣ {format_info_block('Duration', plan.get('duration', '30 days'))}\n"
        f"┗ {format_info_block('Price', f'৳{price_val}')}\n"
    )
    
    # 3. Payment Information
    pay_info = (
        "💸 <b>PAYMENT DETAILS</b>\n"
        f"┣ {format_info_block('Method', session.payment_method.upper() if session.payment_method else 'N/A')}\n"
        f"┣ {format_info_block('TXID', session.txid or 'N/A', code=True)}\n"
        f"┗ {format_info_block('Session', session.id, code=True)}\n"
    )
    
    # 4. Status & Support
    status_badge = get_status_badge(session.status.value)
    support_badge = get_status_badge(support_status)
    
    meta_info = (
        "📊 <b>STATUS & SUPPORT</b>\n"
        f"┣ {format_info_block('Proof Status', status_badge)}\n"
        f"┗ {format_info_block('Support Team', support_badge)}\n"
    )
    
    # 5. Time Information
    created_at = session.created_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(session, 'created_at') else "N/A"
    updated_at = session.updated_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(session, 'updated_at') else "N/A"
    
    time_info = (
        "🕒 <b>TIMESTAMPS (UTC)</b>\n"
        f"┣ {format_info_block('Requested', created_at)}\n"
        f"┗ {format_info_block('Last Update', updated_at)}\n"
    )
    
    return f"{header}\n{user_info}\n{sub_info}\n{pay_info}\n{meta_info}\n{time_info}"

def build_admin_payment_request_actions(session_id: str, user_id: int) -> InlineKeyboardMarkup:
    """Action buttons for initial payment request."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📤 Send Payment Details", callback_data=f"pay:admin:send:{session_id}")
        ],
        [
            InlineKeyboardButton("❌ Reject", callback_data=f"pay:admin:reject:{session_id}")
        ]
    ])

def build_admin_payment_actions(session_id: str, user_id: int) -> InlineKeyboardMarkup:
    """Action buttons for admin review."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"pay:admin:approve:{session_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"pay:admin:reject:{session_id}")
        ],
        [
            InlineKeyboardButton("👤 User Profile", url=f"tg://user?id={user_id}"),
            InlineKeyboardButton("💬 Contact User", callback_data=f"pay:admin:contact:{user_id}")
        ]
    ])

def build_admin_rejection_reasons(session_id: str) -> InlineKeyboardMarkup:
    """Pre-defined rejection reasons for speed."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❗ Invalid TXID", callback_data=f"pay:admin:rej_rsn:txid:{session_id}")],
        [InlineKeyboardButton("❗ Wrong Amount", callback_data=f"pay:admin:rej_rsn:amount:{session_id}")],
        [InlineKeyboardButton("❗ Duplicate TX", callback_data=f"pay:admin:rej_rsn:dup:{session_id}")],
        [InlineKeyboardButton("❗ Screenshot Unclear", callback_data=f"pay:admin:rej_rsn:unclear:{session_id}")],
        [InlineKeyboardButton("← Back", callback_data=f"pay:admin:back:{session_id}")]
    ])

def format_user_profile_card(user_doc: dict, sub: Optional[dict], metrics: dict) -> str:
    """Formats the user profile card for the /profile command."""
    user_id = user_doc.get("user_id")
    plan_label = "None"
    expiry = "N/A"
    if sub:
        plan_label = sub.plan.value.upper()
        expiry = sub.expires_at.strftime("%Y-%m-%d %H:%M") if sub.expires_at else "Lifetime"

    text = (
        f"👤 <b>User Profile:</b> <code>{user_id}</code>\n"
        f"📛 Name: {user_doc.get('full_name', 'N/A')}\n"
        f"🔗 Username: @{user_doc.get('username', 'N/A')}\n\n"
        f"💎 <b>Plan:</b> {plan_label}\n"
        f"⏰ Expiry: {expiry}\n\n"
        f"🛡 <b>Trust Level:</b> {metrics.get('level', 'NEW')}\n"
        f"📊 Trust Score: {metrics.get('trust_score', 0)}\n"
        f"🚩 Fraud Score: {metrics.get('fraud_score', 0)}\n\n"
        f"🚫 Banned: {'Yes' if user_doc.get('is_banned') else 'No'}\n"
        f"🔇 Muted: {'Yes' if user_doc.get('is_muted') else 'No'}"
    )
    return text
