from __future__ import annotations

"""
Pyrogram plugin shim for payment handlers.

The bot loads plugins from app.handlers. Importing app.payments.handlers here
registers the payment callbacks without broadening the plugin root to app.
"""

from app.payments.handlers import (  # noqa: F401
    handle_admin_back_to_main,
    handle_admin_cancel_send,
    handle_admin_decision,
    handle_admin_reject_request,
    handle_admin_send_details,
    handle_payment_cancel,
    handle_payment_inputs,
    handle_payment_method,
    handle_payment_status,
    handle_plan_selection,
    handle_premium_menu,
    handle_rejection_reason,
)
