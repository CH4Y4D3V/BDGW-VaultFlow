from __future__ import annotations

"""
app/models/payment.py
Thin compatibility shim.
Canonical models are in app/payments/models.py
"""

from app.payments.models import (
    PaymentStatus,
    PaymentSession,
    TXIDRegistry
)

__all__ = [
    "PaymentStatus",
    "PaymentSession",
    "TXIDRegistry"
]
