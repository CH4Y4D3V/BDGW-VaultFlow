from __future__ import annotations

import time
from datetime import datetime, timezone

from aiohttp import web

from app.core.database import DatabaseManager
from app.utils.logger import get_logger

logger = get_logger(__name__)

_start_time = time.monotonic()


async def health_handler(request: web.Request) -> web.Response:
    uptime = round(time.monotonic() - _start_time, 1)
    db_status = "connected"
    try:
        db = DatabaseManager.get_db()
        await db.command("ping")
    except Exception:
        db_status = "disconnected"

    return web.json_response({
        "status": "ok",
        "uptime_seconds": uptime,
        "db": db_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


async def start_health_server(port: int = 8080) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", health_handler)  # Railway root probe
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health server started", extra={"ctx_port": port})
    return runner