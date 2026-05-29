import asyncio
from app.config import settings
from motor.motor_asyncio import AsyncIOMotorClient

async def inspect():
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB_NAME]
    timeouts = await db["payment_timeouts"].find().to_list(length=10)
    print("Timeouts:", timeouts)
    payments = await db["payments"].find().to_list(length=10)
    print("Payments:", payments)
    client.close()

if __name__ == "__main__":
    asyncio.run(inspect())
