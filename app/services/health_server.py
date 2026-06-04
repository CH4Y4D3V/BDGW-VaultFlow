"""
health_server.py
────────────────
Minimal aiohttp HTTP server that keeps Railway from sleeping the deployment.

Railway sleeps services with no inbound HTTP traffic. Telegram bots using
long-polling never receive HTTP requests, so Railway treats them as idle.

This runs a tiny /health endpoint on PORT (Railway injects this env var).
The bot process stays alive because Railway sees an open HTTP port.

NEVER import handlers or DB here — this must start even if bot init fails,
so Railway can confirm the service is running.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from aiohttp import web

log = logging.getLogger("health_server")

_START_TIME = datetime.now(timezone.utc)


async def _handle_health(request: web.Request) -> web.Response:
    uptime = int((datetime.now(timezone.utc) - _START_TIME).total_seconds())
    return web.json_response({
        "status": "ok",
        "uptime_seconds": uptime,
        "service": "vault_bot",
    })


async def _handle_root(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def start_health_server() -> None:
    """
    Start the HTTP health server as a background coroutine.
    Call with asyncio.create_task() from main().

    PORT is injected by Railway automatically. Default 8080 for local dev.
    Binds to 0.0.0.0 — required for Railway's internal routing to reach it.
    """
    port = int(os.getenv("PORT", "8080"))

    app = web.Application()
    app.router.add_get("/",       _handle_root)
    app.router.add_get("/health", _handle_health)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()

    log.info("[HEALTH] HTTP server listening on 0.0.0.0:%d", port)

    # Keep running indefinitely alongside the bot
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        log.info("[HEALTH] Health server shutting down.")
        await runner.cleanup()