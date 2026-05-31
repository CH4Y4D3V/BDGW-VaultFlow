from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# ── Spacers & Dividers ────────────────────────────────────────────────────────

SECTION_DIVIDER = "━━━━━━━━━━━━━━━━━━━━━━"
THIN_DIVIDER = "──────────────────────"

# ── Status Badges ─────────────────────────────────────────────────────────────

def get_status_badge(status: str) -> str:
    """Returns a formatted status badge with appropriate emoji."""
    status_map = {
        "awaiting_payment": "⏳ <b>Awaiting Payment</b>",
        "pending_details": "🔎 <b>Pending Details</b>",
        "under_review": "🔎 <b>Under Review</b>",
        "approved": "✅ <b>Approved</b>",
        "rejected": "❌ <b>Rejected</b>",
        "expired": "⌛ <b>Expired</b>",
        "cancelled": "🚫 <b>Cancelled</b>",
        "pending": "⏳ <b>Pending</b>",
        "active": "🟢 <b>Active</b>",
        "busy": "🟡 <b>Busy</b>",
        "offline": "🔴 <b>Offline</b>",
        # Trust Levels (Flow P)
        "🚨 high risk": "🚨 <b>HIGH RISK</b>",
        "⚠️ suspicious": "⚠️ <b>SUSPICIOUS</b>",
        "🏅 veteran": "🏅 <b>VETERAN</b>",
        "✅ trusted": "✅ <b>TRUSTED</b>",
        "👤 verified": "👤 <b>VERIFIED</b>",
        "🆕 new member": "🆕 <b>NEW MEMBER</b>",
    }
    return status_map.get(status.lower(), f"❔ <b>{status.upper()}</b>")

# ── Common UI Components ──────────────────────────────────────────────────────

def format_header(title: str, icon: str = "") -> str:
    """Formats a consistent card header."""
    icon_str = f"{icon} " if icon else ""
    return f"{icon_str}<b>{title.upper()}</b>\n{SECTION_DIVIDER}"

def format_info_block(label: str, value: Any, code: bool = False) -> str:
    """Formats a single information line."""
    val_str = f"<code>{value}</code>" if code else str(value)
    return f"<b>{label}:</b> {val_str}"

def build_back_button(target: str = "home", label: str = "← Back") -> List[InlineKeyboardButton]:
    """Builds a consistent back button."""
    return [InlineKeyboardButton(label, callback_data=f"menu:{target}")]
