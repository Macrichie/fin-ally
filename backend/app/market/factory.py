"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 15.0
MIN_POLL_INTERVAL = 1.0


def _poll_interval_from_env() -> float:
    raw = os.environ.get("MASSIVE_POLL_INTERVAL", "").strip()
    if not raw:
        return DEFAULT_POLL_INTERVAL
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Invalid MASSIVE_POLL_INTERVAL=%r; using %.0fs", raw, DEFAULT_POLL_INTERVAL)
        return DEFAULT_POLL_INTERVAL
    if value < MIN_POLL_INTERVAL:
        logger.warning("MASSIVE_POLL_INTERVAL=%s too small; using %.0fs", raw, MIN_POLL_INTERVAL)
        return MIN_POLL_INTERVAL
    return value


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-blank -> MassiveDataSource (real market data),
      polling every MASSIVE_POLL_INTERVAL seconds (default 15)
    - Otherwise -> SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    Each implementation is imported lazily so simulator mode never loads `massive`.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        from .massive_client import MassiveDataSource

        interval = _poll_interval_from_env()
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=interval)

    from .simulator import SimulatorDataSource

    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
