from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from app.payments.service import PaymentService

_payment_service: "Optional[PaymentService]" = None


def get_payment_service() -> "PaymentService":
    global _payment_service
    if _payment_service is None:
        from app.payments.repository import PaymentRepository
        from app.payments.service import PaymentService
        from app.referral.repository import ReferralRepository
        from app.core.database import DatabaseManager

        db = DatabaseManager.get_db()
        repo = PaymentRepository(db)
        referral_repo = ReferralRepository(db)
        _payment_service = PaymentService(repo, referral_repo)
    return _payment_service
