"""Tests for PriceCache."""

import threading

import pytest

from app.market.cache import PriceCache


class TestPriceCache:
    """Unit tests for the PriceCache."""

    def test_update_and_get(self):
        """Test updating and getting a price."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert cache.get("AAPL") == update

    def test_first_update_is_flat(self):
        """Test that the first update has flat direction."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_direction_up(self):
        """Test price update with upward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_direction_down(self):
        """Test price update with downward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 189.00)
        assert update.direction == "down"
        assert update.change == -1.00

    def test_remove(self):
        """Test removing a ticker from cache."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None

    def test_remove_nonexistent(self):
        """Test removing a ticker that doesn't exist."""
        cache = PriceCache()
        cache.remove("AAPL")  # Should not raise

    def test_get_all(self):
        """Test getting all prices."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.update("GOOGL", 175.00)
        all_prices = cache.get_all()
        assert set(all_prices.keys()) == {"AAPL", "GOOGL"}

    def test_version_increments(self):
        """Test that version counter increments."""
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_get_price_convenience(self):
        """Test the convenience get_price method."""
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("NOPE") is None

    def test_len(self):
        """Test __len__ method."""
        cache = PriceCache()
        assert len(cache) == 0
        cache.update("AAPL", 190.00)
        assert len(cache) == 1
        cache.update("GOOGL", 175.00)
        assert len(cache) == 2

    def test_contains(self):
        """Test __contains__ method."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        assert "AAPL" in cache
        assert "GOOGL" not in cache

    def test_custom_timestamp(self):
        """Test updating with a custom timestamp."""
        cache = PriceCache()
        custom_ts = 1234567890.0
        update = cache.update("AAPL", 190.50, timestamp=custom_ts)
        assert update.timestamp == custom_ts

    def test_price_rounding(self):
        """Test that prices are rounded to 2 decimal places."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.12345)
        assert update.price == 190.12


class TestPriceCacheRound2:
    """Fixes from the round-2 review (§3.3, §3.6)."""

    def test_reference_price_defaults_to_first_price_and_persists(self):
        c = PriceCache()
        c.update("AAPL", 190.0)
        c.update("AAPL", 191.0)
        u = c.update("AAPL", 192.0)
        assert u.reference_price == 190.0
        assert u.day_change_percent == pytest.approx(1.0526, abs=1e-3)

    def test_explicit_reference_price_overrides(self):
        c = PriceCache()
        c.update("AAPL", 190.0)
        u = c.update("AAPL", 191.2, reference_price=189.87)
        assert u.reference_price == 189.87
        assert u.day_change == pytest.approx(1.33)

    def test_reference_price_kept_when_omitted_after_explicit(self):
        c = PriceCache()
        c.update("AAPL", 191.2, reference_price=189.87)
        assert c.update("AAPL", 191.5).reference_price == 189.87

    def test_remove_bumps_version(self):
        c = PriceCache()
        c.update("AAPL", 190.0)
        v = c.version
        c.remove("AAPL")
        assert c.version == v + 1

    def test_remove_missing_does_not_bump_version(self):
        c = PriceCache()
        v = c.version
        c.remove("NOPE")
        assert c.version == v

    def test_timestamp_zero_is_respected(self):
        assert PriceCache().update("X", 1.0, timestamp=0.0).timestamp == 0.0

    def test_sub_dollar_prices_keep_precision(self):
        c = PriceCache()
        u = c.update("PENNY", 0.012345)
        assert u.price == 0.0123
        assert u.reference_price == 0.0123

    def test_concurrent_writers_and_readers(self):
        c = PriceCache()
        tickers = [f"T{i}" for i in range(20)]
        n_iter = 500

        def writer(offset):
            for i in range(n_iter):
                for t in tickers:
                    c.update(t, 100.0 + offset + i * 0.01)

        def reader():
            for _ in range(n_iter):
                snap = c.get_all()
                assert all(u.price > 0 for u in snap.values())

        threads = [threading.Thread(target=writer, args=(k,)) for k in range(4)]
        threads += [threading.Thread(target=reader) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert len(c) == len(tickers)
        assert c.version == 4 * n_iter * len(tickers)
