# app/ui/submission_cards.py
from __future__ import annotations
from typing import Optional


def format_submission_card(
    user_id: int,
    full_name: str,
    username: Optional[str],
    count: int,
    media_type: str,
) -> str:
    username_str = f"@{username}" if username else "no username"
    media_label = "album" if count > 1 else media_type.capitalize()
    return (
        f"📬 <b>New Submission</b>\n\n"
        f"👤 <b>User:</b> {full_name} ({username_str})\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"📦 <b>Content:</b> {count} × {media_label}\n\n"
        f"<b>Actions:</b>"
    )