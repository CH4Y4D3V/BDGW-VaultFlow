from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

class ReferralStatus(str, Enum):
    PENDING = 'pending'
    QUALIFIED = 'qualified'
    INVALIDATED = 'invalidated'

@dataclass
class ReferralDocument:
    referrer_user_id: int
    referred_user_id: int
    status: ReferralStatus
    qualified: bool
    channel_member: bool
    bot_active: bool
    created_at: datetime
    qualified_at: Optional[datetime] = None
    invalidated_at: Optional[datetime] = None

@dataclass
class ReferralWallet:
    user_id: int
    points_balance: int
    total_earned: int
    total_spent: int
    active_referrals: int
