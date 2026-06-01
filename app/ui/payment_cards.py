from __future__ import annotations

from typing import Dict, Any, List
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.ui.common import (
    SECTION_DIVIDER, 
    THIN_DIVIDER, 
    get_status_badge, 
    format_header, 
    format_info_block
)

def build_plan_selection_card(plans: Dict[str, Any]) -> tuple[str, InlineKeyboardMarkup]:
    """Redesigned premium plan selection card."""
    header = format_header("Premium Access", "💎")
    body = (
        "Premium gives you access to exclusive BDGW content channels "
        "and priority support.\n\n"
        "<b>Select a plan to continue:</b>"
    )
    
    buttons = []
    for plan_id, plan in plans.items():
        buttons.append([
            InlineKeyboardButton(
                f"💳 {plan['label']} — ৳{plan['price']}",
                callback_data=f"pay:select:{plan_id}"
            )
        ])
    
    buttons.append([
        InlineKeyboardButton("📊 My Status", callback_data="pay:status"),
        InlineKeyboardButton("🔄 Refresh", callback_data="menu:premium")
    ])
    buttons.append([InlineKeyboardButton("← Back", callback_data="menu:home")])
    
    return f"{header}\n{body}", InlineKeyboardMarkup(buttons)

def build_payment_instruction_card(session: Any, plan: Dict[str, Any]) -> tuple[str, InlineKeyboardMarkup]:
    """Step 2: Redesigned payment instructions card."""
    header = format_header("Payment Instructions", "💸")
    
    plan_info = (
        f"💎 <b>Plan:</b> {plan['label']}\n"
        f"💰 <b>Amount:</b> ৳{session.locked_amount}\n"
        f"📅 <b>Duration:</b> {plan.get('duration', '30 days')}\n"
        f"{THIN_DIVIDER}"
    )
    
    instructions = (
        "<b>Please select your preferred payment method:</b>\n"
        "<i>Admin will provide specific number/details after selection.</i>"
    )
    
    footer = (
        f"\n{THIN_DIVIDER}\n"
        "⚠️ <b>Important:</b> After payment, you must submit a screenshot of the "
        "successful transaction proof."
    )
    
    buttons = [
        [
            InlineKeyboardButton("📱 bKash", callback_data=f"pay:method:bkash:{session.id}")
        ],
        [InlineKeyboardButton("₿ Crypto (USDT)", callback_data=f"pay:method:crypto:{session.id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"pay:cancel:{session.id}")]
    ]
    
    text = f"{header}\n{plan_info}\n{instructions}{footer}"
    return text, InlineKeyboardMarkup(buttons)

def build_payment_status_card(session: Any, plan: Dict[str, Any]) -> tuple[str, InlineKeyboardMarkup]:
    """Redesigned payment status card."""
    header = format_header("Payment Status", "📊")
    
    status_badge = get_status_badge(session.status.value)
    
    info = (
        f"👤 <b>Session ID:</b> <code>{session.id}</code>\n"
        f"💎 <b>Plan:</b> {plan['label']}\n"
        f"💰 <b>Amount:</b> ৳{session.locked_amount}\n"
        f"🏷 <b>Status:</b> {status_badge}\n"
        f"{THIN_DIVIDER}"
    )
    
    buttons = []
    if session.status.value in ["waiting_payment_details", "requested", "awaiting_payment", "waiting_txid"]:
         buttons.append([InlineKeyboardButton("❌ Cancel Session", callback_data=f"pay:cancel:{session.id}")])
    
    buttons.append([InlineKeyboardButton("← Back", callback_data="menu:premium")])
    
    return f"{header}\n{info}", InlineKeyboardMarkup(buttons)

def build_proof_received_card(session_id: str) -> str:
    """Card shown to user after uploading screenshot."""
    header = format_header("Payment Proof Received", "✅")
    
    body = (
        "Your payment proof has been successfully submitted to our team.\n\n"
        "🏷 <b>Status:</b> ⏳ <b>Pending Admin Review</b>\n"
        f"🆔 <b>Session:</b> <code>{session_id}</code>\n\n"
        "<i>You will be notified once the review is complete. Usually takes 5-30 minutes.</i>"
    )
    
    return f"{header}\n{body}"

def build_premium_activated_card(plan_label: str, expiry_date: str) -> tuple[str, InlineKeyboardMarkup]:
    """Redesigned success card for activated premium."""
    header = format_header("Premium Activated", "🎉")
    
    body = (
        "Congratulations! Your premium subscription is now active.\n\n"
        f"💎 <b>Plan:</b> {plan_label}\n"
        f"📅 <b>Expires:</b> <code>{expiry_date}</code>\n"
        "✨ <b>Status:</b> 🟢 <b>Active Member</b>\n\n"
        "You can now join all premium groups and channels."
    )
    
    buttons = [
        [InlineKeyboardButton("🚀 Join Premium Groups", callback_data="menu:premium_links")],
        [InlineKeyboardButton("📋 My Subscription", callback_data="pay:status")],
        [InlineKeyboardButton("🆘 Support", callback_data="menu:support")]
    ]
    
    return f"{header}\n{body}", InlineKeyboardMarkup(buttons)

def build_payment_rejected_card(reason: str, session_id: str) -> tuple[str, InlineKeyboardMarkup]:
    """Redesigned rejection card."""
    header = format_header("Verification Failed", "❌")
    
    body = (
        "Unfortunately, your payment could not be verified.\n\n"
        f"❗ <b>Reason:</b> {reason}\n"
        f"🆔 <b>Session:</b> <code>{session_id}</code>\n\n"
        "<b>What to do next?</b>\n"
        "1. Check the reason above.\n"
        "2. If it was a wrong screenshot, submit a new one.\n"
        "3. Contact support if you believe this is a mistake."
    )
    
    buttons = [
        [InlineKeyboardButton("📤 Submit New Proof", callback_data="menu:premium")], # Routes back to start for now
        [InlineKeyboardButton("🆘 Contact Support", callback_data="menu:support")]
    ]
    
    return f"{header}\n{body}", InlineKeyboardMarkup(buttons)
