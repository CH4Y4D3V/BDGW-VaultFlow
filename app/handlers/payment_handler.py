from __future__ import annotations

"""
Pyrogram plugin shim for payment handlers.

The bot loads plugins from app.handlers. Importing app.payments.handlers here
registers the payment callbacks without broadening the plugin root to app.
"""

from app.payments.handlers import (  # noqa: F401
    handle_admin_decision,
    handle_payment_inputs,
    handle_payment_method,
    handle_plan_selection,
    handle_premium_menu,
    handle_rejection_reason,
)
