"""Abstract interface for market data sources."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def normalize_ticker(raw: str) -> str:
    """'  aapl ' -> 'AAPL'. Raises ValueError for anything that isn't ticker-shaped.

    Shared by both data sources and the watchlist API, so every layer agrees on
    what a ticker looks like.
    """
    if not isinstance(raw, str):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    ticker = raw.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule. Downstream code reads the cache, never the source.

        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL"])   # exactly once
        await source.add_ticker("PYPL")          # any time after construction
        await source.remove_ticker("GOOGL")
        await source.stop()                      # idempotent

    Contract rules (checked by tests/market/test_conformance.py):
      1. Every method accepts any case/whitespace and normalizes via
         normalize_ticker(); invalid symbols raise ValueError.
      2. add_ticker() before start() is remembered.
      3. remove_ticker() always clears the cache entry.
      4. stop() is idempotent; no cache writes happen after it returns.
      5. get_tickers() returns a copy.
      6. start() twice raises RuntimeError.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing prices. Leaves the cache populated (or a first poll
        attempted) before returning, so the first SSE event has data."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Safe to call repeatedly."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker AND remove it from the PriceCache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers (a copy)."""

    def set_initial_prices(self, prices: dict[str, float]) -> None:
        """Hint last-known prices (e.g. from the DB) before start().

        Sources that generate prices (the simulator) use these as starting
        points so P&L stays continuous across restarts. Sources backed by real
        data ignore them. Default: no-op.
        """
