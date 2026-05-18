from __future__ import annotations

from aiohttp import web

from app.utils.logger import get_logger

logger = get_logger(__name__)

async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
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