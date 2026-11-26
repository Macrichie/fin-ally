"""Tests for market data source factory."""

import os
from pathlib import Path
from unittest.mock import patch

from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


class TestFactory:
    """Tests for create_market_data_source factory."""

    def test_creates_simulator_when_no_api_key(self):
        """Test that simulator is created when MASSIVE_API_KEY is not set."""
        cache = PriceCache()

        with patch.dict(os.environ, {}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_simulator_when_api_key_empty(self):
        """Test that simulator is created when MASSIVE_API_KEY is empty."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_simulator_when_api_key_whitespace(self):
        """Test that simulator is created when MASSIVE_API_KEY is whitespace."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_massive_when_api_key_set(self):
        """Test that Massive client is created when MASSIVE_API_KEY is set."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)

    def test_massive_receives_api_key(self):
        """Test that Massive client receives the API key."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key-123"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)
        assert source._api_key == "test-key-123"

    def test_simulator_receives_cache(self):
        """Test that simulator receives the cache reference."""
        cache = PriceCache()

        with patch.dict(os.environ, {}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)
        assert source._cache is cache

    def test_massive_receives_cache(self):
        """Test that Massive client receives the cache reference."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)
        assert source._cache is cache

    def test_poll_interval_default(self):
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "k"}, clear=True):
            source = create_market_data_source(PriceCache())
        assert source._interval == 15.0

    def test_poll_interval_from_env(self):
        with patch.dict(
            os.environ, {"MASSIVE_API_KEY": "k", "MASSIVE_POLL_INTERVAL": "3"}, clear=True
        ):
            source = create_market_data_source(PriceCache())
        assert source._interval == 3.0

    def test_poll_interval_invalid_falls_back(self):
        with patch.dict(
            os.environ, {"MASSIVE_API_KEY": "k", "MASSIVE_POLL_INTERVAL": "fast"}, clear=True
        ):
            source = create_market_data_source(PriceCache())
        assert source._interval == 15.0

    def test_poll_interval_clamped(self):
        with patch.dict(
            os.environ, {"MASSIVE_API_KEY": "k", "MASSIVE_POLL_INTERVAL": "0"}, clear=True
        ):
            source = create_market_data_source(PriceCache())
        assert source._interval == 1.0

    def test_simulator_mode_does_not_import_massive(self):
        """The `massive` package is only loaded when MASSIVE_API_KEY is set."""
        import subprocess
        import sys

        code = (
            "import os, sys; os.environ.pop('MASSIVE_API_KEY', None);"
            "from app.market import PriceCache, create_market_data_source;"
            "create_market_data_source(PriceCache());"
            "print('massive' in sys.modules)"
        )
        out = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path(__file__).resolve().parents[2],
        )
        assert out.stdout.strip() == "False"
