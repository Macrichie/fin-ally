"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 15.0


def create_stream_router(price_cache: PriceCache, interval: float = 0.5) -> APIRouter:
    """Create the SSE streaming router bound to a PriceCache.

    A fresh APIRouter is built on every call, so multiple apps (e.g. in tests)
    each stream from their own cache.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Each event is the full price map for every tracked ticker:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        An empty map ({}) is sent when the last ticker is removed. A `: ping`
        comment is sent after HEARTBEAT_SECONDS without data so proxies don't
        close an idle stream. A `retry:` directive tells EventSource how fast
        to reconnect.
        """
        return StreamingResponse(
            _generate_events(price_cache, request, interval),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = HEARTBEAT_SECONDS,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Checks the cache every `interval` seconds and sends the full map whenever
    its version changed. Stops when the client disconnects.
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    idle = 0.0
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while not await request.is_disconnected():
            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                data = {ticker: u.to_dict() for ticker, u in price_cache.get_all().items()}
                yield f"data: {json.dumps(data)}\n\n"  # sent even if {} (after a remove)
                idle = 0.0
            elif idle >= heartbeat:
                yield ": ping\n\n"
                idle = 0.0

            await asyncio.sleep(interval)
            idle += interval
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
        raise
    finally:
        logger.info("SSE client disconnected: %s", client_ip)
