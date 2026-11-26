"""Tests for PriceUpdate dataclass."""

import pytest

from app.market.models import PriceUpdate


class TestPriceUpdate:
    """Unit tests for the PriceUpdate model."""

    def test_price_update_creation(self):
        """Test basic PriceUpdate creation."""
        update = PriceUpdate(
            ticker="AAPL", price=190.50, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert update.previous_price == 190.00
        assert update.timestamp == 1234567890.0

    def test_change_calculation(self):
        """Test price change calculation."""
        update = PriceUpdate(
            ticker="AAPL", price=190.50, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.change == 0.50

    def test_change_negative(self):
        """Test negative price change."""
        update = PriceUpdate(
            ticker="AAPL", price=189.50, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.change == -0.50

    def test_change_percent_up(self):
        """Test percentage change calculation (up)."""
        update = PriceUpdate(
            ticker="AAPL", price=190.00, previous_price=100.00, timestamp=1234567890.0
        )
        assert update.change_percent == 90.0

    def test_change_percent_down(self):
        """Test percentage change calculation (down)."""
        update = PriceUpdate(
            ticker="AAPL", price=100.00, previous_price=200.00, timestamp=1234567890.0
        )
        assert update.change_percent == -50.0

    def test_change_percent_zero_previous(self):
        """Test percentage change with zero previous price."""
        update = PriceUpdate(
            ticker="AAPL", price=100.00, previous_price=0.00, timestamp=1234567890.0
        )
        assert update.change_percent == 0.0

    def test_direction_up(self):
        """Test direction calculation (up)."""
        update = PriceUpdate(
            ticker="AAPL", price=191.00, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.direction == "up"

    def test_direction_down(self):
        """Test direction calculation (down)."""
        update = PriceUpdate(
            ticker="AAPL", price=189.00, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.direction == "down"

    def test_direction_flat(self):
        """Test direction calculation (flat)."""
        update = PriceUpdate(
            ticker="AAPL", price=190.00, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.direction == "flat"

    def test_to_dict(self):
        """Test serialization to dictionary."""
        update = PriceUpdate(
            ticker="AAPL", price=190.50, previous_price=190.00, timestamp=1234567890.0
        )
        result = update.to_dict()

        assert result["ticker"] == "AAPL"
        assert result["price"] == 190.50
        assert result["previous_price"] == 190.00
        assert result["timestamp"] == 1234567890.0
        assert result["change"] == 0.50
        assert result["change_percent"] == 0.2632  # (0.50 / 190.00) * 100
        assert result["direction"] == "up"

    def test_immutability(self):
        """Test that PriceUpdate is immutable."""
        update = PriceUpdate(
            ticker="AAPL", price=190.50, previous_price=190.00, timestamp=1234567890.0
        )

        with pytest.raises(AttributeError):
            update.price = 200.00  # Should raise error


class TestDayChange:
    """reference_price drives the watchlist 'daily change %' (PLAN §10)."""

    def test_day_change(self):
        u = PriceUpdate(
            "AAPL", price=191.20, previous_price=191.05, timestamp=1.0, reference_price=190.00
        )
        assert u.day_change == 1.2
        assert u.day_change_percent == 0.6316

    def test_day_change_without_reference_is_zero(self):
        u = PriceUpdate("AAPL", price=191.20, previous_price=191.05, timestamp=1.0)
        assert u.reference_price is None
        assert u.day_change == 0.0
        assert u.day_change_percent == 0.0

    def test_zero_reference_is_zero(self):
        u = PriceUpdate("AAPL", price=1.0, previous_price=1.0, reference_price=0.0)
        assert u.day_change_percent == 0.0

    def test_to_dict_includes_day_fields(self):
        u = PriceUpdate(
            "AAPL", price=189.0, previous_price=190.0, timestamp=1.0, reference_price=200.0
        )
        d = u.to_dict()
        assert d["reference_price"] == 200.0
        assert d["day_change"] == -11.0
        assert d["day_change_percent"] == -5.5
        assert set(d) == {
            "ticker",
            "price",
            "previous_price",
            "timestamp",
            "change",
            "change_percent",
            "direction",
            "reference_price",
            "day_change",
            "day_change_percent",
        }
