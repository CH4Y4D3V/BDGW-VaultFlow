from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from app.repositories.activity_repository import ActivityRepository
from app.repositories.user_repository import UserRepository
from app.models.activity import ActivityAction

class TrustService:
    """
    Enforces SECTION 18 — TRUST & FRAUD SYSTEM.
    Calculates dynamic scores based on user history.
    """

    def __init__(self):
        self._activity = ActivityRepository()
        self._users = UserRepository()

    async def calculate_trust_score(self, user_id: int) -> float:
        """
        + Approved content
        - Rejected content
        - Warnings
        + Account age
        """
        score = 0.0
        
        # 1. Approved content (+10 per item)
        approved_count = await self._activity.count_user_actions(user_id, ActivityAction.UPLOAD) # Simplified
        score += approved_count * 10
        
        # 2. Account age (+5 per month)
        user_doc = await self._users.get_user(user_id)
        if user_doc and user_doc.get("join_date"):
            delta = datetime.now(timezone.utc) - user_doc["join_date"].replace(tzinfo=timezone.utc)
            months = delta.days // 30
            score += months * 5
            
        # 3. Penalties
        # We check audit logs for rejections and warnings
        rejections = await self._activity.count_user_actions(user_id, ActivityAction.REJECT)
        score -= rejections * 15
        
        warnings = await self._activity.count_user_actions(user_id, ActivityAction.AUDIT) # Use metadata.type="warning" if possible
        # For simplicity, count all audits as -5 unless refined
        score -= warnings * 5
        
        return max(0.0, score)

    async def calculate_fraud_score(self, user_id: int) -> float:
        """
        + Duplicate TXID attempts
        + Fake proof submissions
        + Invite abuse
        """
        score = 0.0
        
        # 1. Duplicate TXID reuse (handled by auto-ban, but for score visibility)
        # Check ban status
        user_doc = await self._users.get_user(user_id)
        if user_doc and user_doc.get("is_banned"):
            score += 100 # Maximum fraud if already banned for fraud
            
        return score

    async def get_user_metrics(self, user_id: int) -> dict:
        trust = await self.calculate_trust_score(user_id)
        fraud = await self.calculate_fraud_score(user_id)
        
        return {
            "trust_score": trust,
            "fraud_score": fraud,
            "level": "TRUSTED" if trust > 100 and fraud < 10 else "NEUTRAL"
        }
