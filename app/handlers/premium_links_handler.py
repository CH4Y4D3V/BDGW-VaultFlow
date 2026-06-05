"""
app/handlers/premium_links_handler.py

PURPOSE
-------
Handles the "menu:premium_links" callback query that is emitted by the
inline keyboard button in the premium activation card
(built by payment_cards.build_premium_activated_card).

This handler is triggered when a user clicks "💎 My Premium Access" or
"🔗 Get My Links" after their subscription has been approved and activated.

BEHAVIOUR BY CASE
-----------------
  Case 1 — Valid invite (not used, not expired):
    → Show the invite link with remaining time.
    → Remind user it is one-time use only.

  Case 2 — Invite expired (past 30-minute window), subscription still active:
    → Tell user the link expired.
    → Direct user to support for a new invite.

  Case 3 — Invite already used, subscription still active:
    → Confirm they are already a member.
    → Direct to support if they have access issues.

  Case 4 — No valid invite record found, subscription active:
    → Treat same as Case 2 (expired / missing).

  Case 5 — No active subscription:
    → Inform user they do not have an active premium subscription.
    → Offer the "💎 Premium Access" menu button.

  Case 6 — DB or Telegram API error:
    → Answer callback with generic error message.
    → Log full traceback.

RULES
-----
  - Every callback must be answered (answer_callback_query) in all paths.
  - FloodWait is caught and retried with explicit sleep.
  - No group IDs or channel IDs are hardcoded — all come from hub_config.
  - DB state is read-only in this handler (no writes except audit log).
  - Audit log written for INVITE_LINK_VIEWED regardless of case.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified, UserNotParticipant
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.core.database import DatabaseManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum number of FloodWait retry attempts before giving up.
MAX_FLOOD_RETRIES: int = 3

# Callback data prefix — must match what payment_cards.py builds.
CALLBACK_PREMIUM_LINKS: str = "menu:premium_links"


# ---------------------------------------------------------------------------
# FloodWait-safe Telegram wrappers
# ---------------------------------------------------------------------------


async def _safe_answer_callback(
    callback_query: CallbackQuery,
    text: str = "",
    show_alert: bool = False,
    cache_time: int = 0,
) -> None:
    """
    Answer a callback query with explicit FloodWait handling.

    Retries up to MAX_FLOOD_RETRIES times, sleeping e.value seconds
    between each attempt as required by the Telegram API.

    Args:
        callback_query: The Pyrogram CallbackQuery object to answer.
        text: Optional short text to show in the Telegram notification popup.
        show_alert: If True, shows a full-screen alert instead of a toast.
        cache_time: Seconds Telegram may cache the answer. Default 0.
    """
    for attempt in range(1, MAX_FLOOD_RETRIES + 1):
        try:
            await callback_query.answer(
                text=text,
                show_alert=show_alert,
                cache_time=cache_time,
            )
            return
        except FloodWait as exc:
            if attempt == MAX_FLOOD_RETRIES:
                logger.error(
                    "[PremiumLinks] FloodWait on answer_callback exhausted "
                    "after %d retries. user_id=%s wait=%ds",
                    MAX_FLOOD_RETRIES,
                    callback_query.from_user.id if callback_query.from_user else "unknown",
                    exc.value,
                )
                return  # Best-effort — do not crash handler
            logger.warning(
                "[PremiumLinks] FloodWait on answer_callback: sleeping %ds "
                "(attempt %d/%d)",
                exc.value,
                attempt,
                MAX_FLOOD_RETRIES,
            )
            await asyncio.sleep(exc.value)
        except Exception as exc:
            logger.error(
                "[PremiumLinks] Failed to answer callback query: %s", exc
            )
            return


async def _safe_edit_message(
    callback_query: CallbackQuery,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """
    Edit the message attached to a callback query with FloodWait handling.

    Silently ignores MessageNotModified (text already identical — idempotent).
    Retries up to MAX_FLOOD_RETRIES times on FloodWait.

    Args:
        callback_query: The Pyrogram CallbackQuery containing the message.
        text: New message text (HTML parse mode).
        reply_markup: Optional updated inline keyboard markup.
    """
    for attempt in range(1, MAX_FLOOD_RETRIES + 1):
        try:
            await callback_query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode="html",
            )
            return
        except MessageNotModified:
            # Text is already identical — not an error.
            return
        except FloodWait as exc:
            if attempt == MAX_FLOOD_RETRIES:
                logger.error(
                    "[PremiumLinks] FloodWait on edit_message exhausted "
                    "after %d retries. user_id=%s wait=%ds",
                    MAX_FLOOD_RETRIES,
                    callback_query.from_user.id if callback_query.from_user else "unknown",
                    exc.value,
                )
                return
            logger.warning(
                "[PremiumLinks] FloodWait on edit_message: sleeping %ds "
                "(attempt %d/%d)",
                exc.value,
                attempt,
                MAX_FLOOD_RETRIES,
            )
            await asyncio.sleep(exc.value)
        except Exception as exc:
            logger.error(
                "[PremiumLinks] Failed to edit message: %s", exc
            )
            return


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


async def _get_active_subscription(user_id: int) -> Optional[dict]:
    """
    Fetch the user's current ACTIVE subscription record.

    Queries the subscriptions collection for a document where
    user_id matches, status == "ACTIVE", and expires_at is in the future.

    Args:
        user_id: Telegram user ID.

    Returns:
        Subscription document dict if found, or None.

    Raises:
        Exception: Any Motor DB exception is propagated to the caller.
    """
    db = DatabaseManager.get_db()
    now = datetime.now(tz=timezone.utc)

    subscription = await db["subscriptions"].find_one(
        {
            "user_id": user_id,
            "status": "ACTIVE",
            "expires_at": {"$gt": now},
        },
        projection={
            "_id": 1,
            "user_id": 1,
            "package_id": 1,
            "started_at": 1,
            "expires_at": 1,
            "status": 1,
        },
        sort=[("expires_at", -1)],  # most recently expiring first
    )
    return subscription


async def _get_latest_invite(user_id: int) -> Optional[dict]:
    """
    Fetch the most recently created invite record for a user.

    Does NOT filter by expiry or used status — the caller inspects those
    fields to determine which case applies.

    Args:
        user_id: Telegram user ID.

    Returns:
        Most recent invite document dict, or None if no invite found.

    Raises:
        Exception: Any Motor DB exception is propagated to the caller.
    """
    db = DatabaseManager.get_db()

    invite = await db["invites"].find_one(
        {"user_id": user_id},
        projection={
            "_id": 1,
            "user_id": 1,
            "subscription_id": 1,
            "invite_link": 1,
            "created_at": 1,
            "expires_at": 1,
            "used": 1,
        },
        sort=[("created_at", -1)],
    )
    return invite


async def _write_audit_log(
    user_id: int,
    detail: dict,
) -> None:
    """
    Write an INVITE_LINK_VIEWED entry to the audit_logs collection.

    Non-raising: any exception is caught and logged without propagation.

    Args:
        user_id: Telegram user ID of the user who triggered the view.
        detail: Dict of event-specific data for audit purposes.
    """
    db = DatabaseManager.get_db()
    try:
        await db["audit_logs"].insert_one(
            {
                "action": "INVITE_LINK_VIEWED",
                "admin_user_id": None,   # user-initiated action, not admin
                "target_user_id": user_id,
                "detail": detail,
                "timestamp": datetime.now(tz=timezone.utc),
            }
        )
    except Exception as exc:
        logger.error(
            "[PremiumLinks] Audit log write failed — non-critical. "
            "user_id=%s error=%s",
            user_id,
            exc,
        )


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------


def _build_valid_invite_card(
    invite: dict,
    subscription: dict,
) -> tuple[str, InlineKeyboardMarkup]:
    """
    Build the message text and keyboard for a valid (unused, non-expired) invite.

    Args:
        invite: Invite document from MongoDB.
        subscription: Subscription document from MongoDB.

    Returns:
        Tuple of (html_text, InlineKeyboardMarkup).
    """
    expires_at: datetime = invite["expires_at"]
    now = datetime.now(tz=timezone.utc)
    remaining_minutes = max(0, int((expires_at - now).total_seconds() / 60))
    sub_expires: datetime = subscription["expires_at"]
    package_id: str = subscription.get("package_id", "unknown")

    text = (
        "<b>💎 Your Premium Access Link</b>\n\n"
        f"<b>Package     :</b> {_format_package(package_id)}\n"
        f"<b>Expires On  :</b> {sub_expires.strftime('%d %b %Y')}\n\n"
        f"<b>🔗 Invite Link:</b>\n"
        f"<code>{invite['invite_link']}</code>\n\n"
        f"⏳ <b>Link expires in {remaining_minutes} minute(s).</b>\n\n"
        "⚠️ <i>This link is one-time use only. "
        "Do not share it. Once used, it cannot be reactivated.</i>\n\n"
        "If you have already joined, no further action is needed."
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🆘 Need Help", callback_data="menu:support"
                )
            ],
            [
                InlineKeyboardButton(
                    "🏠 Main Menu", callback_data="menu:main"
                )
            ],
        ]
    )
    return text, keyboard


def _build_expired_invite_card(subscription: dict) -> tuple[str, InlineKeyboardMarkup]:
    """
    Build the message text and keyboard for an expired or used invite.

    The user's subscription is still active but they need a new invite link
    via support.

    Args:
        subscription: Active subscription document from MongoDB.

    Returns:
        Tuple of (html_text, InlineKeyboardMarkup).
    """
    sub_expires: datetime = subscription["expires_at"]
    package_id: str = subscription.get("package_id", "unknown")

    text = (
        "<b>💎 Your Premium Subscription</b>\n\n"
        f"<b>Package     :</b> {_format_package(package_id)}\n"
        f"<b>Expires On  :</b> {sub_expires.strftime('%d %b %Y')}\n"
        f"<b>Status      :</b> ✅ Active\n\n"
        "<b>🔗 Invite Link Status: Expired or Already Used</b>\n\n"
        "Your one-time invite link has either expired or was already used.\n\n"
        "If you have <b>already joined</b> the premium group, "
        "no action is needed.\n\n"
        "If you have <b>not yet joined</b> or need a new link, "
        "please contact support below and an admin will generate a new link for you."
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🆘 Contact Support", callback_data="menu:support"
                )
            ],
            [
                InlineKeyboardButton(
                    "🏠 Main Menu", callback_data="menu:main"
                )
            ],
        ]
    )
    return text, keyboard


def _build_no_subscription_card() -> tuple[str, InlineKeyboardMarkup]:
    """
    Build the message text and keyboard when the user has no active subscription.

    Returns:
        Tuple of (html_text, InlineKeyboardMarkup).
    """
    text = (
        "<b>💎 No Active Subscription</b>\n\n"
        "You do not currently have an active premium subscription.\n\n"
        "To get access to premium content and groups, "
        "please subscribe via the Premium Access menu."
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "💎 Get Premium Access", callback_data="menu:premium"
                )
            ],
            [
                InlineKeyboardButton(
                    "🏠 Main Menu", callback_data="menu:main"
                )
            ],
        ]
    )
    return text, keyboard


def _format_package(package_id: str) -> str:
    """
    Return a human-readable package label for a package_id code.

    Args:
        package_id: Package identifier string ("1m", "3m", "6m").

    Returns:
        Human-readable label string.
    """
    labels = {
        "1m": "1 Month",
        "3m": "3 Months",
        "6m": "6 Months",
    }
    return labels.get(package_id, package_id.upper())


# ---------------------------------------------------------------------------
# Main callback handler
# ---------------------------------------------------------------------------


async def handle_premium_links(client: Client, callback_query: CallbackQuery) -> None:
    """
    Handle the "menu:premium_links" callback query.

    Registered by register_handlers().  Called when the user taps the
    premium links button in the activation card or main menu.

    Flow:
      1. Answer the callback immediately (stops Telegram spinner).
      2. Retrieve subscription and invite from MongoDB.
      3. Determine which case applies (valid invite / expired / no subscription).
      4. Edit the existing message with the appropriate card.
      5. Write audit log entry.

    All exceptions are caught.  The user always receives a response.

    Args:
        client: The active Pyrogram Client instance.
        callback_query: The incoming CallbackQuery from Telegram.
    """
    user = callback_query.from_user
    if user is None:
        # Should never happen in a private chat, but guard defensively.
        await _safe_answer_callback(
            callback_query,
            text="Could not identify your account. Please try again.",
            show_alert=True,
        )
        return

    user_id: int = user.id

    # Step 1 — Answer immediately to stop Telegram spinner.
    await _safe_answer_callback(callback_query, text="")

    try:
        # Step 2 — Fetch subscription and invite from DB.
        subscription = await _get_active_subscription(user_id)
        invite = await _get_latest_invite(user_id) if subscription else None

    except Exception as exc:
        logger.exception(
            "[PremiumLinks] DB error fetching subscription/invite. "
            "user_id=%s error=%s",
            user_id,
            exc,
        )
        await _safe_edit_message(
            callback_query,
            text=(
                "⚠️ <b>Temporary Error</b>\n\n"
                "Could not retrieve your subscription details. "
                "Please try again in a moment or contact support."
            ),
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🆘 Contact Support", callback_data="menu:support"
                        )
                    ]
                ]
            ),
        )
        return

    # Step 3 — Determine case and build response.
    now = datetime.now(tz=timezone.utc)

    if subscription is None:
        # Case 5: No active subscription.
        logger.info(
            "[PremiumLinks] No active subscription for user_id=%s.", user_id
        )
        text, keyboard = _build_no_subscription_card()
        audit_case = "no_subscription"

    elif invite is None:
        # Case 4: Active subscription but no invite record found.
        logger.info(
            "[PremiumLinks] No invite record found for user_id=%s "
            "(subscription active). Showing expired card.",
            user_id,
        )
        text, keyboard = _build_expired_invite_card(subscription)
        audit_case = "no_invite_record"

    elif invite.get("used") is True:
        # Case 3: Invite already used — user is (should be) a member.
        logger.info(
            "[PremiumLinks] Invite already used for user_id=%s.", user_id
        )
        text, keyboard = _build_expired_invite_card(subscription)
        audit_case = "invite_used"

    elif invite["expires_at"] <= now:
        # Case 2: Invite expired.
        logger.info(
            "[PremiumLinks] Invite expired for user_id=%s. "
            "Expired at=%s",
            user_id,
            invite["expires_at"].isoformat(),
        )
        text, keyboard = _build_expired_invite_card(subscription)
        audit_case = "invite_expired"

    else:
        # Case 1: Valid invite — not used, not expired.
        logger.info(
            "[PremiumLinks] Serving valid invite to user_id=%s. "
            "Expires at=%s",
            user_id,
            invite["expires_at"].isoformat(),
        )
        text, keyboard = _build_valid_invite_card(invite, subscription)
        audit_case = "valid_invite_served"

    # Step 4 — Edit the original message with the appropriate card.
    await _safe_edit_message(callback_query, text=text, reply_markup=keyboard)

    # Step 5 — Write audit log (non-critical, wrapped internally).
    await _write_audit_log(
        user_id=user_id,
        detail={
            "case": audit_case,
            "has_subscription": subscription is not None,
            "subscription_id": (
                str(subscription["_id"]) if subscription else None
            ),
            "invite_id": str(invite["_id"]) if invite else None,
            "invite_expires_at": (
                invite["expires_at"].isoformat() if invite else None
            ),
            "invite_used": invite.get("used") if invite else None,
        },
    )


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------


def register_handlers(client: Client) -> None:
    """
    Register all premium_links callback query handlers with the Pyrogram client.

    Must be called during application startup, after the Pyrogram client has
    been initialised but before it starts polling.

    Args:
        client: The active Pyrogram Client instance.

    Example:
        from app.handlers.premium_links_handler import register_handlers
        register_handlers(pyrogram_client)
    """
    client.add_handler(
        # Import here to avoid circular-import issues at module load time.
        __import__(
            "pyrogram.handlers",
            fromlist=["CallbackQueryHandler"],
        ).CallbackQueryHandler(
            callback=handle_premium_links,
            filters=filters.regex(f"^{CALLBACK_PREMIUM_LINKS}$"),
        )
    )

    logger.info(
        "[PremiumLinks] Registered callback handler for '%s'.",
        CALLBACK_PREMIUM_LINKS,
    )