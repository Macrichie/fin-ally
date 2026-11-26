"""Interface conformance: the same contract tests run against both sources (PLAN §12)."""

from unittest.mock import MagicMock, patch

import pytest

from app.market import MarketDataSource, PriceCache, normalize_ticker
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


@pytest.fixture(params=["simulator", "massive"])
async def source(request):
    cache = PriceCache()
    if request.param == "simulator":
        src = SimulatorDataSource(cache, update_interval=0.01)
        yield src, cache
        await src.stop()
    else:
        src = MassiveDataSource("k", cache, poll_interval=60)
        src._fetch_snapshots = MagicMock(return_value=[])  # no network
        with patch("app.market.massive_client.RESTClient"):
            yield src, cache
            await src.stop()


async def test_is_a_market_data_source(source):
    src, _ = source
    assert isinstance(src, MarketDataSource)


async def test_start_normalizes_tickers(source):
    src, _ = source
    await src.start([" aapl", "AAPL", "msft "])
    assert src.get_tickers() == ["AAPL", "MSFT"]


async def test_add_is_normalized_and_idempotent(source):
    src, _ = source
    await src.start(["AAPL"])
    await src.add_ticker("  tsla ")
    await src.add_ticker("TSLA")
    assert src.get_tickers().count("TSLA") == 1


async def test_add_before_start_is_remembered(source):
    src, _ = source
    await src.add_ticker("nflx")
    assert "NFLX" in src.get_tickers()
    await src.start(["AAPL"])
    assert set(src.get_tickers()) == {"AAPL", "NFLX"}


async def test_remove_clears_cache(source):
    src, cache = source
    await src.start(["AAPL"])
    cache.update("AAPL", 190.0)
    await src.remove_ticker("aapl")
    assert "AAPL" not in cache
    assert "AAPL" not in src.get_tickers()


async def test_remove_untracked_still_clears_cache(source):
    src, cache = source
    await src.start(["AAPL"])
    cache.update("ZZZ", 1.0)
    await src.remove_ticker("ZZZ")
    assert "ZZZ" not in cache


async def test_remove_unknown_is_noop(source):
    src, _ = source
    await src.start(["AAPL"])
    await src.remove_ticker("NOPE")
    assert src.get_tickers() == ["AAPL"]


async def test_get_tickers_returns_copy(source):
    src, _ = source
    await src.start(["AAPL"])
    src.get_tickers().append("X")
    assert src.get_tickers() == ["AAPL"]


async def test_stop_is_idempotent(source):
    src, _ = source
    await src.start(["AAPL"])
    await src.stop()
    await src.stop()


async def test_stop_before_start_is_safe(source):
    src, _ = source
    await src.stop()


async def test_no_cache_writes_after_stop(source):
    src, cache = source
    await src.start(["AAPL"])
    await src.stop()
    v = cache.version
    import asyncio

    await asyncio.sleep(0.05)
    assert cache.version == v


async def test_start_twice_raises(source):
    src, _ = source
    await src.start(["AAPL"])
    with pytest.raises(RuntimeError):
        await src.start(["AAPL"])


@pytest.mark.parametrize("bad", ["DROP TABLE", "", "   ", "1ABC", "AAPL;", "TOOLONGTICKER"])
async def test_invalid_ticker_rejected(source, bad):
    src, _ = source
    await src.start([])
    with pytest.raises(ValueError):
        await src.add_ticker(bad)
    with pytest.raises(ValueError):
        await src.remove_ticker(bad)


async def test_invalid_ticker_rejected_on_start(source):
    src, _ = source
    with pytest.raises(ValueError):
        await src.start(["AAPL", "DROP TABLE"])


class TestNormalizeTicker:
    @pytest.mark.parametrize(
        "raw,expected",
        [("aapl", "AAPL"), ("  msft ", "MSFT"), ("BRK.B", "BRK.B"), ("brk-b", "BRK-B"), ("V", "V")],
    )
    def test_valid(self, raw, expected):
        assert normalize_ticker(raw) == expected

    @pytest.mark.parametrize("bad", ["", "DROP TABLE", "9X", "A$", "ABCDEFGHIJK", None, 123])
    def test_invalid(self, bad):
        with pytest.raises(ValueError):
            normalize_ticker(bad)
