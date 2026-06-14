# app/handlers/admin_handler.py — COMPLETE FIXED FILE
"""
Admin-only command handlers for the verification hub.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from app.config import settings
from app.core.database import DatabaseManager
from app.core.permissions import Role, permission_required
from app.services.admin_logger import get_admin_logger
from app.services.subscription_service import SubscriptionService
from app.services.trust_service import TrustService
from app.ui.admin_cards import format_user_profile_card
from app.utils.logger import get_logger

logger = get_logger(__name__)


@Client.on_message(
    filters.command("accept")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_accept_command(client: Client, message: Message) -> None:
    await message.reply_text(
        "ℹ️ Please use the <b>✅ Accept Support</b> button on the request card.",
        parse_mode=ParseMode.HTML,
    )


@Client.on_message(
    filters.command("close")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_close_redirect(client: Client, message: Message) -> None:
    from app.handlers.support_handler import handle_close_command
    await handle_close_command(client, message)


@Client.on_message(
    filters.command("ban")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_ban(client: Client, message: Message) -> None:
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/ban <user_id> <reason>`")
        return

    try:
        target_id = int(message.command[1])
        reason = " ".join(message.command[2:])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    await db["users"].update_one(
        {"_id": target_id},
        {
            "$set": {
                "is_banned": True,
                "ban_reason": reason,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    premium_channel = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(settings, "PREMIUM_GROUP_ID", None)
    chats_to_kick = [
        c for c in [settings.NSFW_GROUP_ID, settings.PREMIUM_GROUP_ID, premium_channel]
        if c
    ]
    for group in set(chats_to_kick):
        try:
            await client.ban_chat_member(group, target_id)
        except Exception as e:
            logger.warning("ban_kick_failed", extra={"ctx_user_id": target_id, "ctx_chat": group, "ctx_error": str(e)})

    await message.reply_text(
        f"✅ User <code>{target_id}</code> has been banned.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="USER BANNED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details=f"Reason: {reason}",
    )


@Client.on_message(
    filters.command("unban")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_unban(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/unban <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    await db["users"].update_one(
        {"_id": target_id},
        {"$set": {"is_banned": False, "updated_at": datetime.now(timezone.utc)}},
    )

    premium_channel = getattr(settings, "PREMIUM_CHANNEL_ID", None) or getattr(settings, "PREMIUM_GROUP_ID", None)
    chats_to_unban = [
        c for c in [settings.NSFW_GROUP_ID, settings.PREMIUM_GROUP_ID, premium_channel]
        if c
    ]
    for group in set(chats_to_unban):
        try:
            await client.unban_chat_member(group, target_id)
        except Exception as e:
            logger.warning("unban_failed", extra={"ctx_user_id": target_id, "ctx_chat": group, "ctx_error": str(e)})

    await message.reply_text(
        f"✅ User <code>{target_id}</code> unbanned.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="USER UNBANNED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details="Manual unban by admin",
    )


@Client.on_message(
    filters.command("mute")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_mute(client: Client, message: Message) -> None:
    if len(message.command) < 4:
        await message.reply_text("❌ Usage: `/mute <user_id> <minutes> <reason>`")
        return

    try:
        target_id = int(message.command[1])
        minutes = int(message.command[2])
        reason = " ".join(message.command[3:])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid input.")
        return

    db = DatabaseManager.get_db()
    mute_until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    await db["users"].update_one(
        {"_id": target_id},
        {"$set": {"is_muted": True, "mute_until": mute_until}},
    )

    await message.reply_text(
        f"✅ User <code>{target_id}</code> muted for {minutes}m.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="USER MUTED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details=f"Muted for {minutes}m. Reason: {reason}",
    )


@Client.on_message(
    filters.command("paymentdone")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_paymentdone(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/paymentdone <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    from app.payments import get_payment_service
    service = get_payment_service()
    session = await service.get_active_session(target_id)

    if not session:
        await message.reply_text("❌ No active payment session found for this user.")
        return

    success = await service.approve_payment(client, session.id, message.from_user.id)
    if success:
        await message.reply_text(
            f"✅ Payment for user <code>{target_id}</code> approved.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await message.reply_text("❌ Approval failed. Session might be already processed.")


@Client.on_message(
    filters.command("profile")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_profile(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/profile <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    user_doc = await db["users"].find_one({"_id": target_id})
    if not user_doc:
        await message.reply_text("❌ User not found in database.")
        return

    sub_service = SubscriptionService()
    sub = await sub_service.get_subscription(target_id)

    trust_service = TrustService()
    metrics = await trust_service.get_user_metrics(target_id)

    card = format_user_profile_card(user_doc, sub, metrics)
    await message.reply_text(card, parse_mode=ParseMode.HTML)


@Client.on_message(
    filters.command("grant")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_grant(client: Client, message: Message) -> None:
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/grant <user_id> <plan_id> [days]`")
        return

    try:
        target_id = int(message.command[1])
        plan_str = message.command[2].lower()
        days = int(message.command[3]) if len(message.command) > 3 else None
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid input. Days must be a number.")
        return

    from app.models.subscription import Plan
    try:
        plan = Plan(plan_str)
    except ValueError:
        valid_plans = ", ".join([p.value for p in Plan])
        await message.reply_text(f"❌ Invalid plan. Choose from: {valid_plans}")
        return

    service = SubscriptionService()
    await service.grant(
        user_id=target_id,
        plan=plan,
        duration_days=days,
        granted_by=message.from_user.id,
        notes="Manual grant by admin",
    )

    await message.reply_text(
        f"✅ Granted <b>{plan.value.upper()}</b> to <code>{target_id}</code>.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="SUBSCRIPTION GRANTED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details=f"Manually granted {plan.value} ({days or 'lifetime'} days)",
    )


@Client.on_message(
    filters.command("revoke")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_revoke(client: Client, message: Message) -> None:
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/revoke <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    service = SubscriptionService()
    await service.revoke(target_id, revoked_by=message.from_user.id)

    await message.reply_text(
        f"✅ Subscription revoked for <code>{target_id}</code>.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="SUBSCRIPTION REVOKED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details="Manual revocation by admin",
    )

# ── Missing hub commands (spec Section 9.5) ─────────────────────────────────
# FIX L5-004 through L5-008: /warn, /unmute, /history, /note, /notes were
# absent from admin_handler.py but required by spec Section 9.5.


@Client.on_message(
    filters.command("warn")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_warn(client: Client, message: Message) -> None:
    """Issue a warning to a user. Spec §9.5 — /warn command."""
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/warn <user_id> <reason>`")
        return

    try:
        target_id = int(message.command[1])
        reason = " ".join(message.command[2:])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    result = await db["users"].update_one(
        {"_id": target_id},
        {
            "$inc": {"warn_count": 1},
            "$set": {"updated_at": datetime.now(timezone.utc)},
        },
    )

    if result.matched_count == 0:
        await message.reply_text("❌ User not found in database.")
        return

    # Log punishment record
    await db["punishments"].insert_one({
        "user_id": target_id,
        "type": "warning",
        "reason": reason,
        "issued_by": message.from_user.id,
        "issued_at": datetime.now(timezone.utc),
        "active": True,
        "resolved_at": None,
        "resolved_by": None,
    })

    await message.reply_text(
        f"⚠️ Warning issued to <code>{target_id}</code>.\nReason: {reason}",
        parse_mode=ParseMode.HTML,
    )

    # Notify user silently — do not error if user blocked bot
    try:
        await client.send_message(
            chat_id=target_id,
            text=f"⚠️ You have received a warning.\nReason: {reason}",
        )
    except Exception:
        pass

    await get_admin_logger().log(
        client=client,
        action="USER WARNED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details=f"Reason: {reason}",
    )


@Client.on_message(
    filters.command("unmute")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_unmute(client: Client, message: Message) -> None:
    """Remove a mute from a user. Spec §9.5 — /unmute command."""
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/unmute <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    await db["users"].update_one(
        {"_id": target_id},
        {
            "$set": {
                "is_muted": False,
                "mute_until": None,
                "mute_reason": None,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    # Mark all active mutes for this user as resolved
    await db["punishments"].update_many(
        {"user_id": target_id, "type": "mute", "active": True},
        {
            "$set": {
                "active": False,
                "resolved_at": datetime.now(timezone.utc),
                "resolved_by": message.from_user.id,
            }
        },
    )

    await message.reply_text(
        f"✅ User <code>{target_id}</code> unmuted.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="USER UNMUTED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details="Manual unmute by admin",
    )


@Client.on_message(
    filters.command("history")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_history(client: Client, message: Message) -> None:
    """Show event history summary for a user. Spec §9.5 — /history command."""
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/history <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    user_doc = await db["users"].find_one({"_id": target_id})
    if not user_doc:
        await message.reply_text("❌ User not found in database.")
        return

    # Gather punishments
    punishments = await db["punishments"].find(
        {"user_id": target_id}
    ).sort("issued_at", -1).limit(10).to_list(length=10)

    # Gather payment history
    payments = await db["payment_history"].find(
        {"user_id": target_id}
    ).sort("reviewed_at", -1).limit(5).to_list(length=5)

    # Gather content submissions count
    sub_count = await db["content_submissions"].count_documents({"user_id": target_id})
    approved_nsfw = await db["content_submissions"].count_documents(
        {"user_id": target_id, "status": "APPROVED_NSFW"}
    )
    approved_premium = await db["content_submissions"].count_documents(
        {"user_id": target_id, "status": "APPROVED_PREMIUM"}
    )
    rejected = await db["content_submissions"].count_documents(
        {"user_id": target_id, "status": "REJECTED"}
    )

    # Format punishments
    pun_lines = []
    for p in punishments:
        ts = p.get("issued_at")
        ts_str = ts.strftime("%Y-%m-%d") if ts else "?"
        active_mark = "🔴" if p.get("active") else "✅"
        pun_lines.append(
            f"  {active_mark} [{ts_str}] {p['type'].upper()}: {p.get('reason', 'N/A')}"
        )

    # Format payments
    pay_lines = []
    for pay in payments:
        ts = pay.get("reviewed_at")
        ts_str = ts.strftime("%Y-%m-%d") if ts else "?"
        pay_lines.append(
            f"  [{ts_str}] {pay.get('status', '?')} — {pay.get('package_id', '?')}"
        )

    text = (
        f"📋 <b>History for <code>{target_id}</code></b>\n"
        f"👤 {user_doc.get('full_name', '?')} (@{user_doc.get('username', 'N/A')})\n\n"
        f"<b>📤 Submissions:</b> {sub_count} total | "
        f"{approved_nsfw} NSFW | {approved_premium} Premium | {rejected} rejected\n\n"
    )

    if pun_lines:
        text += "<b>⚠️ Punishments (last 10):</b>\n" + "\n".join(pun_lines) + "\n\n"
    else:
        text += "<b>⚠️ Punishments:</b> None\n\n"

    if pay_lines:
        text += "<b>💳 Payments (last 5):</b>\n" + "\n".join(pay_lines)
    else:
        text += "<b>💳 Payments:</b> None"

    await message.reply_text(text, parse_mode=ParseMode.HTML)


@Client.on_message(
    filters.command("note")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_note(client: Client, message: Message) -> None:
    """Add an admin note about a user. Spec §9.5 — /note command."""
    if len(message.command) < 3:
        await message.reply_text("❌ Usage: `/note <user_id> <note text>`")
        return

    try:
        target_id = int(message.command[1])
        note_text = " ".join(message.command[2:])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid input.")
        return

    db = DatabaseManager.get_db()

    # Verify user exists
    user_doc = await db["users"].find_one({"_id": target_id})
    if not user_doc:
        await message.reply_text("❌ User not found in database.")
        return

    await db["user_notes"].insert_one({
        "user_id": target_id,
        "note": note_text,
        "written_by": message.from_user.id,
        "written_by_name": message.from_user.first_name,
        "written_at": datetime.now(timezone.utc),
    })

    await message.reply_text(
        f"✅ Note added for <code>{target_id}</code>.",
        parse_mode=ParseMode.HTML,
    )

    await get_admin_logger().log(
        client=client,
        action="NOTE ADDED",
        admin_id=message.from_user.id,
        admin_name=message.from_user.first_name,
        target_user_id=target_id,
        details=f"Note: {note_text[:100]}",
    )


@Client.on_message(
    filters.command("notes")
    & filters.group
    & filters.chat(settings.VERIFICATION_GROUP_ID)
)
@permission_required(Role.ADMIN)
async def handle_notes(client: Client, message: Message) -> None:
    """List all admin notes about a user. Spec §9.5 — /notes command."""
    if len(message.command) < 2:
        await message.reply_text("❌ Usage: `/notes <user_id>`")
        return

    try:
        target_id = int(message.command[1])
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid User ID.")
        return

    db = DatabaseManager.get_db()
    notes = await db["user_notes"].find(
        {"user_id": target_id}
    ).sort("written_at", -1).limit(20).to_list(length=20)

    if not notes:
        await message.reply_text(
            f"📝 No notes found for <code>{target_id}</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    lines = [f"📝 <b>Notes for <code>{target_id}</code></b> ({len(notes)} found)\n"]
    for i, note in enumerate(notes, start=1):
        ts = note.get("written_at")
        ts_str = ts.strftime("%Y-%m-%d %H:%M") if ts else "?"
        admin_name = note.get("written_by_name", "Admin")
        lines.append(
            f"<b>{i}.</b> [{ts_str}] by {admin_name}:\n"
            f"  {note['note']}\n"
        )

    await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
