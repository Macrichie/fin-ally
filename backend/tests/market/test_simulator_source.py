"""Integration tests for SimulatorDataSource."""

import asyncio

import pytest

from app.market.cache import PriceCache
from app.market.simulator import GBMSimulator, SimulatorDataSource


class TestSimulatorDataSource:
    """Integration tests for the SimulatorDataSource."""

    async def test_start_populates_cache(self):
        """Test that start() immediately populates the cache."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        # Cache should have seed prices immediately (before first loop tick)
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None

        await source.stop()

    async def test_prices_update_over_time(self):
        """Test that prices are updated periodically."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        await source.start(["AAPL"])

        initial_version = cache.version
        await asyncio.sleep(0.3)  # Several update cycles

        # Version should have incremented (prices updated)
        assert cache.version > initial_version

        await source.stop()

    async def test_stop_is_clean(self):
        """Test that stop() is clean and idempotent."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        # Double stop should not raise
        await source.stop()

    async def test_add_ticker(self):
        """Test adding a ticker dynamically."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()
        assert cache.get("TSLA") is not None

        await source.stop()

    async def test_remove_ticker(self):
        """Test removing a ticker."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "TSLA"])

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None

        await source.stop()

    async def test_get_tickers(self):
        """Test getting the list of active tickers."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        tickers = source.get_tickers()
        assert set(tickers) == {"AAPL", "GOOGL"}

        await source.stop()

    async def test_empty_start(self):
        """Test starting with no tickers."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start([])

        assert len(cache) == 0
        assert source.get_tickers() == []

        await source.stop()

    async def test_exception_resilience(self):
        """Test that simulator continues running after errors."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)

        # Start with a valid ticker
        await source.start(["AAPL"])

        # Wait for some updates
        await asyncio.sleep(0.15)

        # Task should still be running
        assert source._task is not None
        assert not source._task.done()

        await source.stop()

    async def test_custom_update_interval(self):
        """Test using a custom update interval."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start(["AAPL"])

        initial_version = cache.version
        await asyncio.sleep(0.05)  # Should get ~5 updates

        # Should have multiple updates with fast interval
        assert cache.version > initial_version + 2

        await source.stop()

    async def test_custom_event_probability(self):
        """Test creating source with custom event probability."""
        cache = PriceCache()
        # Very high event probability for testing
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1, event_probability=1.0)
        await source.start(["AAPL"])

        # Just verify it starts and stops cleanly
        await asyncio.sleep(0.2)
        await source.stop()

    async def test_add_before_start_is_remembered(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.add_ticker("tsla")
        assert source.get_tickers() == ["TSLA"]
        await source.start(["AAPL"])
        assert set(source.get_tickers()) == {"AAPL", "TSLA"}
        assert cache.get("TSLA") is not None
        await source.stop()

    async def test_lowercase_add_does_not_duplicate(self):
        """Regression: add_ticker('aapl') used to create a second 'aapl' ticker."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        v = cache.version
        await source.add_ticker("aapl")
        assert source.get_tickers() == ["AAPL"]
        assert "aapl" not in cache
        assert cache.version == v  # no-op add does not reset the price
        await source.stop()

    async def test_start_twice_raises(self):
        source = SimulatorDataSource(price_cache=PriceCache(), update_interval=0.1)
        await source.start(["AAPL"])
        with pytest.raises(RuntimeError):
            await source.start(["AAPL"])
        await source.stop()

    async def test_initial_prices_used_on_start_and_add(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10)
        source.set_initial_prices({"aapl": 150.0, "pypl": 112.0, "BAD TICKER": 1.0, "MSFT": 0})
        await source.start(["AAPL", "MSFT"])
        await source.add_ticker("PYPL")
        assert cache.get_price("AAPL") == 150.0
        assert cache.get_price("PYPL") == 112.0
        assert cache.get_price("MSFT") == 420.0  # zero hint ignored -> seed
        await source.stop()

    async def test_dt_derived_from_update_interval(self):
        source = SimulatorDataSource(price_cache=PriceCache(), update_interval=2.0)
        await source.start(["AAPL"])
        assert source._sim._dt == pytest.approx(2.0 / GBMSimulator.TRADING_SECONDS_PER_YEAR)
        await source.stop()

    async def test_seeded_rng_is_reproducible(self):
        import numpy as np

        async def run():
            cache = PriceCache()
            src = SimulatorDataSource(cache, update_interval=10, rng=np.random.default_rng(7))
            await src.start(["AAPL", "MSFT"])
            prices = [src._sim.step() for _ in range(5)]
            await src.stop()
            return prices

        assert await run() == await run()

    async def test_first_price_is_reference_price(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start(["AAPL"])
        await asyncio.sleep(0.05)
        await source.stop()
        assert cache.get("AAPL").reference_price == 190.0
