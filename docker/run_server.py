"""Container entrypoint for BlueRed Central Server.

Runs ``build_server_app`` and ``uvicorn.Server.serve`` on the exact same
``asyncio`` event loop so the ``asyncpg`` pool and ``redis.asyncio`` client
remain bound to the active loop for the entire server lifetime.
"""

from __future__ import annotations

import asyncio
import os

import uvicorn
from bluered_server.main import build_server_app


async def _serve() -> None:
    app = await build_server_app()
    config = uvicorn.Config(
        app,
        host=os.getenv("SERVER_HOST", "0.0.0.0"),
        port=int(os.getenv("SERVER_PORT", "8000")),
        log_level=os.getenv("SERVER_LOG_LEVEL", "info"),
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(_serve())
