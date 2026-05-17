from __future__ import annotations

from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase


class BaseRepository:
    """Generic async repository. All subclasses declare `collection_name`."""

    collection_name: str

    def __init__(self) -> None:
        self._db: Optional[AsyncIOMotorDatabase] = None

    # ── Lazy DB access ────────────────────────────────────────────────────────

    @property
    def db(self) -> AsyncIOMotorDatabase:
        if self._db is None:
            from app.core.database import get_database
            self._db = get_database()
        return self._db

    @property
    def collection(self) -> AsyncIOMotorCollection:
        return self.db[self.collection_name]

    # ── Primitives ────────────────────────────────────────────────────────────

    async def find_one(
        self,
        filter: dict,
        projection: Optional[dict] = None,
    ) -> Optional[dict]:
        return await self.collection.find_one(filter, projection)

    async def find_many(
        self,
        filter: dict,
        projection: Optional[dict] = None,
        sort: Optional[list] = None,
        limit: int = 0,
        skip: int = 0,
    ) -> list[dict]:
        cursor = self.collection.find(filter, projection)
        if sort:
            cursor = cursor.sort(sort)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        return await cursor.to_list(length=limit or None)

    async def insert_one(self, document: dict) -> Any:
        result = await self.collection.insert_one(document)
        return result.inserted_id

    async def update_one(
        self,
        filter: dict,
        update: dict,
        upsert: bool = False,
    ) -> int:
        result = await self.collection.update_one(filter, update, upsert=upsert)
        return result.modified_count + (1 if result.upserted_id else 0)

    async def update_many(self, filter: dict, update: dict) -> int:
        result = await self.collection.update_many(filter, update)
        return result.modified_count

    async def delete_one(self, filter: dict) -> int:
        result = await self.collection.delete_one(filter)
        return result.deleted_count

    async def count(self, filter: dict) -> int:
        return await self.collection.count_documents(filter)

    async def exists(self, filter: dict) -> bool:
        doc = await self.collection.find_one(filter, {"_id": 1})
        return doc is not None