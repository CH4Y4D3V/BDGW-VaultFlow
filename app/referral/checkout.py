from __future__ import annotations
from datetime import datetime, timezone
from app.referral.db import ReferralRepository

_repo = ReferralRepository()

async def lock_referral_discount(user_id: int, plan_price: float, points_to_use: int) -> dict:
    """
    Atomically deducts points and returns a locked price snapshot.
    """
    # 1. Validate points available
    wallet = await _repo.get_wallet(user_id)
    if not wallet or wallet["points_balance"] < points_to_use:
        raise ValueError("Insufficient referral points.")

    # 2. Points to percentage (Example: 1 pt = 10% off)
    discount_pct = points_to_use * 0.1
    discount_amount = plan_price * discount_pct
    final_price = plan_price - discount_amount

    # 3. Deduct points immediately (Snapshot)
    success = await _repo.spend_wallet_points(user_id, points_to_use)
    if not success:
        raise RuntimeError("Point deduction failed.")

    return {
        "original_price": plan_price,
        "discount_applied": discount_amount,
        "final_price": final_price,
        "points_used": points_to_use,
        "locked_at": datetime.now(timezone.utc)
    }

async def refund_discount_snapshot(user_id: int, points_refunded: int):
    """Restores points if checkout is abandoned or fails."""
    await _repo.increment_wallet_points(user_id, points_refunded)
