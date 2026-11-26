"""Tests for the SSE generator and router factory (no server needed)."""

import asyncio
import json

import pytest

from app.market import create_stream_router
from app.market.cache import PriceCache
from app.market.stream import _generate_events


class FakeRequest:
    client = None

    def __init__(self, polls_before_disconnect: int, on_poll=None):
        self.n = polls_before_disconnect
        self.on_poll = on_poll
        self.polls = 0

    async def is_disconnected(self) -> bool:
        if self.on_poll:
            self.on_poll(self.polls)
        self.polls += 1
        self.n -= 1
        return self.n < 0


def parse(event: str) -> dict:
    assert event.startswith("data: ") and event.endswith("\n\n")
    return json.loads(event.removeprefix("data: "))


async def collect(cache, request, **kw) -> list[str]:
    return [e async for e in _generate_events(cache, request, interval=0.001, **kw)]


async def test_emits_retry_then_data_without_duplicates():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    events = await collect(cache, FakeRequest(3))
    assert events[0] == "retry: 1000\n\n"
    data = parse(events[1])
    assert data["AAPL"]["price"] == 190.0
    assert data["AAPL"]["day_change_percent"] == 0.0
    assert len(events) == 2  # version unchanged -> no duplicate frames


async def test_new_event_on_each_change():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    events = await collect(
        cache, FakeRequest(3, on_poll=lambda i: i == 1 and cache.update("AAPL", 191.0))
    )
    assert [parse(e)["AAPL"]["price"] for e in events[1:]] == [190.0, 191.0]


async def test_empty_cache_sends_empty_map():
    events = await collect(PriceCache(), FakeRequest(1))
    assert parse(events[1]) == {}


async def test_removing_last_ticker_sends_empty_map():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    events = await collect(cache, FakeRequest(3, on_poll=lambda i: i == 1 and cache.remove("AAPL")))
    assert parse(events[1]) == {"AAPL": parse(events[1])["AAPL"]}
    assert parse(events[2]) == {}


async def test_heartbeat_when_idle():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    events = await collect(cache, FakeRequest(20), heartbeat=0.005)
    assert ": ping\n\n" in events
    assert sum(e.startswith("data:") for e in events) == 1


async def test_immediate_disconnect_yields_only_retry():
    events = await collect(PriceCache(), FakeRequest(0))
    assert events == ["retry: 1000\n\n"]


async def test_cancellation_is_propagated():
    cache = PriceCache()
    gen = _generate_events(cache, FakeRequest(10_000), interval=0.01)

    async def consume():
        async for _ in gen:
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_router_factory_is_reentrant():
    r1, r2 = create_stream_router(PriceCache()), create_stream_router(PriceCache())
    assert r1 is not r2
    assert [r.path for r in r1.routes] == ["/api/stream/prices"]
    assert len(r2.routes) == 1


async def test_endpoint_returns_event_stream():
    """Call the route handler directly and read the first frames from its body."""
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    router = create_stream_router(cache, interval=0.01)
    handler = router.routes[0].endpoint

    resp = await handler(FakeRequest(10_000))
    assert resp.media_type == "text/event-stream"
    assert resp.headers["cache-control"] == "no-cache"
    assert resp.headers["x-accel-buffering"] == "no"
    assert "connection" not in resp.headers

    body = resp.body_iterator
    assert await anext(body) == "retry: 1000\n\n"
    assert parse(await anext(body))["AAPL"]["price"] == 190.0
    await body.aclose()
