from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING

from app.models.payment import TXIDRegistry
from app.repositories.base import BaseRepository


class TXIDRepository(BaseRepository):
    collection_name = "txid_registry"

    async def register(self, txid: str, user_id: int, payment_id: str) -> bool:
        """Register a TXID. Returns False if already exists."""
        try:
            doc = TXIDRegistry(
                _id=txid,
                user_id=user_id,
                payment_id=payment_id
            )
            await self.collection.insert_one(doc.to_dict())
            return True
        except Exception:
            return False

    async def get_by_txid(self, txid: str) -> Optional[TXIDRegistry]:
        doc = await self.collection.find_one({"_id": txid})
        return TXIDRegistry.from_dict(doc) if doc else None

    async def create_indexes(self) -> None:
        await self.collection.create_index([("user_id", ASCENDING)])
        await self.collection.create_index([("payment_id", ASCENDING)])
