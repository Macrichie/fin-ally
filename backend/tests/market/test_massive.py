"""Tests for MassiveDataSource.

Snapshots are built with the real `massive` models (TickerSnapshot.from_dict) so
the tests check our field mapping against the library, not against a MagicMock
that accepts any attribute (the root cause of the old `last_trade.timestamp` bug).
"""

import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource, _extract

TS_NS = 1_732_464_000_000_000_000  # 2024-11-24T16:00:00Z in nanoseconds
TS_S = 1_732_464_000.0


def real_snap(ticker, p=None, t=None, day_c=None, prev_c=None, updated=None) -> TickerSnapshot:
    d: dict = {"ticker": ticker}
    if p is not None:
        d["lastTrade"] = {"p": p, "t": t}
    if day_c is not None:
        d["day"] = {"c": day_c}
    if prev_c is not None:
        d["prevDay"] = {"c": prev_c}
    if updated is not None:
        d["updated"] = updated
    return TickerSnapshot.from_dict(d)


def make_source(tickers=("AAPL",), interval=60.0, cache=None) -> MassiveDataSource:
    src = MassiveDataSource("test-key", cache or PriceCache(), poll_interval=interval)
    src._client = MagicMock()  # satisfy the _poll_once guard
    src._tickers = list(tickers)
    return src


class TestExtract:
    def test_uses_sip_timestamp_in_ns(self):
        assert _extract(real_snap("AAPL", 191.2, TS_NS, prev_c=189.87)) == (
            "AAPL",
            191.2,
            TS_S,
            189.87,
        )

    def test_falls_back_to_updated_timestamp(self):
        _, _, ts, _ = _extract(real_snap("AAPL", 191.2, None, updated=TS_NS))
        assert ts == TS_S

    def test_falls_back_to_now_without_timestamps(self):
        _, _, ts, _ = _extract(real_snap("AAPL", 191.2))
        assert ts > TS_S

    def test_falls_back_to_day_close_without_last_trade(self):
        _, price, _, prev = _extract(real_snap("AAPL", day_c=191.0, prev_c=189.87))
        assert price == 191.0
        assert prev == 189.87

    def test_falls_back_to_prev_close_premarket(self):
        """Pre-market the `day` aggregate is all zeros."""
        _, price, _, _ = _extract(real_snap("AAPL", day_c=0, prev_c=189.87))
        assert price == 189.87

    def test_no_prev_day_gives_none_reference(self):
        assert _extract(real_snap("AAPL", 191.2, TS_NS))[3] is None

    def test_unusable_snapshot_returns_none(self):
        assert _extract(real_snap("AAPL")) is None
        assert _extract(real_snap("AAPL", day_c=0, prev_c=0)) is None
        assert _extract(TickerSnapshot.from_dict({})) is None

    def test_real_attribute_names_exist(self):
        """Guard: the attributes _extract relies on exist on the real model."""
        snap = real_snap("AAPL", 1.0, TS_NS, day_c=1.0, prev_c=1.0, updated=TS_NS)
        assert snap.last_trade.sip_timestamp == TS_NS
        assert snap.prev_day.close == 1.0
        assert snap.day.close == 1.0
        assert not hasattr(snap.last_trade, "timestamp")


class TestPolling:
    async def test_poll_with_real_snapshots_populates_cache(self):
        src = make_source(["AAPL", "GOOGL"])
        src._fetch_snapshots = MagicMock(
            return_value=[
                real_snap("AAPL", 191.2, TS_NS, prev_c=189.87),
                real_snap("GOOGL", 175.25, TS_NS, prev_c=170.0),
            ]
        )
        await src._poll_once()

        aapl = src._cache.get("AAPL")
        assert aapl.price == 191.2
        assert aapl.timestamp == TS_S
        assert aapl.reference_price == 189.87
        assert aapl.day_change == pytest.approx(1.33)
        assert src._cache.get_price("GOOGL") == 175.25

    async def test_fetch_receives_a_copy_of_tickers(self):
        src = make_source(["AAPL"])
        src._fetch_snapshots = MagicMock(return_value=[])
        await src._poll_once()
        (passed,), _ = src._fetch_snapshots.call_args
        assert passed == ["AAPL"]
        assert passed is not src._tickers

    async def test_unusable_snapshot_skipped(self):
        src = make_source(["AAPL", "BAD"])
        src._fetch_snapshots = MagicMock(
            return_value=[real_snap("AAPL", 190.5, TS_NS), real_snap("BAD")]
        )
        await src._poll_once()
        assert src._cache.get_price("AAPL") == 190.5
        assert src._cache.get_price("BAD") is None

    async def test_untracked_ticker_in_response_ignored(self):
        src = make_source(["AAPL"])
        src._fetch_snapshots = MagicMock(
            return_value=[real_snap("AAPL", 190.5, TS_NS), real_snap("MSFT", 420.0, TS_NS)]
        )
        await src._poll_once()
        assert "MSFT" not in src._cache

    async def test_ticker_removed_mid_poll_is_not_written_back(self):
        """Regression: a remove during the in-flight HTTP call left a ghost entry."""
        src = make_source(["AAPL", "GOOGL"])
        in_flight = threading.Event()
        release = threading.Event()

        def slow_fetch(tickers):
            in_flight.set()
            release.wait(2)
            return [real_snap(t, 100.0, TS_NS) for t in tickers]

        src._fetch_snapshots = slow_fetch
        poll = asyncio.create_task(src._poll_once())
        await asyncio.to_thread(in_flight.wait, 2)
        await src.remove_ticker("GOOGL")
        release.set()
        await poll

        assert src.get_tickers() == ["AAPL"]
        assert "AAPL" in src._cache
        assert "GOOGL" not in src._cache

    async def test_empty_tickers_skips_poll(self):
        src = make_source([])
        src._fetch_snapshots = MagicMock()
        await src._poll_once()
        src._fetch_snapshots.assert_not_called()

    async def test_none_response_is_handled(self):
        src = make_source(["AAPL"])
        src._fetch_snapshots = MagicMock(return_value=None)
        await src._poll_once()  # should not raise
        assert len(src._cache) == 0


class TestBackoff:
    @pytest.mark.parametrize(
        "exc",
        [RuntimeError("network"), AuthError("bad key"), BadResponse("429")],
    )
    async def test_failure_does_not_raise_and_backs_off(self, exc):
        src = make_source(interval=15)
        src._fetch_snapshots = MagicMock(side_effect=exc)
        await src._poll_once()
        assert src._current_delay == 30
        assert src._cache.get_price("AAPL") is None

    async def test_backoff_caps_then_resets_on_success(self):
        src = make_source(interval=15)
        src._fetch_snapshots = MagicMock(side_effect=RuntimeError("boom"))
        for _ in range(10):
            await src._poll_once()
        assert src._current_delay == MassiveDataSource.MAX_BACKOFF

        src._fetch_snapshots = MagicMock(return_value=[])
        await src._poll_once()
        assert src._current_delay == 15


class TestTickers:
    async def test_add_ticker_normalizes(self):
        src = MassiveDataSource("k", PriceCache())
        await src.add_ticker("  aapl ")
        await src.add_ticker("AAPL")
        assert src.get_tickers() == ["AAPL"]

    async def test_add_ticker_sets_wake(self):
        src = MassiveDataSource("k", PriceCache())
        await src.add_ticker("AAPL")
        assert src._wake.is_set()

    async def test_remove_ticker_clears_cache(self):
        src = make_source(["AAPL", "GOOGL"])
        src._cache.update("AAPL", 190.0)
        await src.remove_ticker("aapl")
        assert src.get_tickers() == ["GOOGL"]
        assert src._cache.get("AAPL") is None

    async def test_get_tickers_returns_copy(self):
        src = make_source(["AAPL"])
        src.get_tickers().append("X")
        assert src.get_tickers() == ["AAPL"]


class TestLifecycle:
    async def test_start_does_immediate_poll(self):
        cache = PriceCache()
        src = MassiveDataSource("k", cache, poll_interval=60.0)
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(
                src, "_fetch_snapshots", return_value=[real_snap("AAPL", 190.5, TS_NS)]
            ):
                await src.start(["aapl"])
                assert cache.get_price("AAPL") == 190.5
        assert src.get_tickers() == ["AAPL"]
        await src.stop()

    async def test_start_does_not_block_on_slow_api(self):
        src = MassiveDataSource("k", PriceCache(), poll_interval=60.0)
        src.START_TIMEOUT = 0.05
        release = threading.Event()

        def hang(tickers):
            release.wait(2)
            return []

        with patch("app.market.massive_client.RESTClient"):
            src._fetch_snapshots = hang
            await asyncio.wait_for(src.start(["AAPL"]), timeout=1)
        release.set()
        await src.stop()

    async def test_add_before_start_is_remembered(self):
        src = MassiveDataSource("k", PriceCache(), poll_interval=60.0)
        await src.add_ticker("TSLA")
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(src, "_fetch_snapshots", return_value=[]):
                await src.start(["AAPL"])
        assert src.get_tickers() == ["TSLA", "AAPL"]
        await src.stop()

    async def test_start_twice_raises(self):
        src = MassiveDataSource("k", PriceCache(), poll_interval=60.0)
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(src, "_fetch_snapshots", return_value=[]):
                await src.start(["AAPL"])
                with pytest.raises(RuntimeError):
                    await src.start(["AAPL"])
        await src.stop()

    async def test_stop_cancels_task_and_is_idempotent(self):
        src = MassiveDataSource("k", PriceCache(), poll_interval=10.0)
        with patch("app.market.massive_client.RESTClient"):
            with patch.object(src, "_fetch_snapshots", return_value=[]):
                await src.start(["AAPL"])
        task = src._task
        assert task is not None and not task.done()
        await src.stop()
        await src.stop()
        assert src._task is None
        assert task.done()

    async def test_add_ticker_triggers_early_poll(self):
        src = MassiveDataSource("k", PriceCache(), poll_interval=60.0)
        src.MIN_POLL_GAP = 0.0
        src._min_gap = 0.0
        calls: list[list[str]] = []

        def fetch(tickers):
            calls.append(tickers)
            return [real_snap(t, 100.0, TS_NS) for t in tickers]

        with patch("app.market.massive_client.RESTClient"):
            src._fetch_snapshots = fetch
            await src.start(["AAPL"])
            await src.add_ticker("TSLA")
            for _ in range(100):
                if "TSLA" in src._cache:
                    break
                await asyncio.sleep(0.01)
        assert src._cache.get_price("TSLA") == 100.0
        assert calls == [["AAPL"], ["AAPL", "TSLA"]]
        await src.stop()

    async def test_early_poll_respects_min_gap(self):
        src = MassiveDataSource("k", PriceCache(), poll_interval=60.0)
        src._min_gap = 0.3
        calls: list[float] = []
        loop = asyncio.get_running_loop()

        def fetch(tickers):
            calls.append(loop.time())
            return []

        with patch("app.market.massive_client.RESTClient"):
            src._fetch_snapshots = fetch
            await src.start(["AAPL"])
            await src.add_ticker("TSLA")
            await asyncio.sleep(0.1)
            assert len(calls) == 1  # still waiting for the minimum gap
            await asyncio.sleep(0.4)
        assert len(calls) == 2
        assert calls[1] - calls[0] >= 0.25
        await src.stop()
