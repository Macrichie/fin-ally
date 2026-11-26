"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time

from massive import RESTClient
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker

logger = logging.getLogger(__name__)

NS_PER_S = 1_000_000_000


def _positive(value) -> float | None:
    """Return value as float if it is a positive number, else None."""
    if isinstance(value, (int, float)) and value > 0:
        return float(value)
    return None


def _extract(snap) -> tuple[str, float, float, float | None] | None:
    """Map a TickerSnapshot -> (ticker, price, timestamp_s, prev_close), or None if unusable.

    Field mapping (verified against massive 2.x models):
      price      last_trade.price -> day.close -> prev_day.close
                 (day is all zeros pre-market; last_trade may be None)
      timestamp  last_trade.sip_timestamp (ns) -> snap.updated (ns) -> now
      prev_close prev_day.close (daily-change baseline)
    """
    ticker = getattr(snap, "ticker", None)
    if not ticker or not isinstance(ticker, str):
        return None

    last_trade = getattr(snap, "last_trade", None)
    day = getattr(snap, "day", None)
    prev_day = getattr(snap, "prev_day", None)

    prev_close = _positive(getattr(prev_day, "close", None))
    price = (
        _positive(getattr(last_trade, "price", None))
        or _positive(getattr(day, "close", None))
        or prev_close
    )
    if price is None:
        return None

    ts_ns = _positive(getattr(last_trade, "sip_timestamp", None)) or _positive(
        getattr(snap, "updated", None)
    )
    ts = ts_ns / NS_PER_S if ts_ns else time.time()

    return ticker.upper(), price, ts, prev_close


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min -> poll every 15s (default)
      - Paid tiers: higher limits -> poll every 2-5s (MASSIVE_POLL_INTERVAL)

    Failures back off exponentially (interval -> ... -> MAX_BACKOFF) and reset
    on the first success. add_ticker() triggers an early poll, but polls are
    never closer together than min(interval, MIN_POLL_GAP), which keeps the
    free tier under 5 calls/min.
    """

    MAX_BACKOFF = 120.0
    MIN_POLL_GAP = 12.0  # 60s / 5 calls: free-tier safe spacing for early polls
    START_TIMEOUT = 10.0  # max time start() waits for the first poll

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._min_gap = min(poll_interval, self.MIN_POLL_GAP)
        self._current_delay = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._wake = asyncio.Event()  # set by add_ticker to poll early
        self._first_poll_done = asyncio.Event()
        self._last_poll = float("-inf")

    # --- Lifecycle ---

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("MassiveDataSource.start() called twice")
        for t in tickers:
            t = normalize_ticker(t)
            if t not in self._tickers:
                self._tickers.append(t)
        self._client = RESTClient(api_key=self._api_key)
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

        # Wait briefly for the first poll so the cache is warm for the first SSE
        # event, but never let a slow/failing API block app start-up.
        try:
            await asyncio.wait_for(self._first_poll_done.wait(), timeout=self.START_TIMEOUT)
        except TimeoutError:
            logger.warning(
                "Massive first poll still running after %.0fs; continuing start-up",
                self.START_TIMEOUT,
            )
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(self._tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            self._wake.set()  # don't make the user wait a full interval
            logger.info("Massive: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Polling ---

    async def _poll_loop(self) -> None:
        """Poll immediately, then on interval (or early after add_ticker)."""
        self._wake.clear()  # the first poll already covers any pre-start adds
        try:
            await self._poll_once()
        finally:
            self._first_poll_done.set()

        while True:
            remaining = self._current_delay - (time.monotonic() - self._last_poll)
            if remaining > 0:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=remaining)
                except TimeoutError:
                    pass
            if self._wake.is_set():
                self._wake.clear()
                # Early wake: still honour the rate limit. While backing off,
                # wait out the full backoff rather than hammering a failing API.
                backing_off = self._current_delay > self._interval
                min_gap = self._current_delay if backing_off else self._min_gap
                gap = min_gap - (time.monotonic() - self._last_poll)
                if gap > 0:
                    await asyncio.sleep(gap)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        # Stamp first, even when skipping, so the loop always waits between cycles.
        self._last_poll = time.monotonic()
        if not self._tickers or not self._client:
            return

        tickers = list(self._tickers)  # snapshot: the list may change mid-request
        try:
            # The Massive RESTClient is synchronous: run it in a worker thread.
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except (AuthError, BadResponse) as e:  # bad key / 403 / 429 after client retries
            self._back_off(f"HTTP error: {e}")
            return
        except Exception as e:  # network, DNS, JSON
            self._back_off(f"{type(e).__name__}: {e}")
            return

        self._current_delay = self._interval  # success -> reset backoff
        tracked = set(self._tickers)
        seen: set[str] = set()
        updated = 0
        for snap in snapshots or []:
            row = _extract(snap)
            if row is None:
                logger.warning("Skipping unusable snapshot for %s", getattr(snap, "ticker", "???"))
                continue
            ticker, price, ts, prev_close = row
            seen.add(ticker)
            if ticker not in tracked:  # removed while the request was in flight
                continue
            self._cache.update(ticker, price, timestamp=ts, reference_price=prev_close)
            updated += 1

        missing = set(tickers) - seen
        if missing:
            logger.warning("Massive returned no data for: %s", ", ".join(sorted(missing)))
        logger.debug("Massive poll: updated %d/%d tickers", updated, len(tickers))

    def _back_off(self, reason: str) -> None:
        self._current_delay = min(self._current_delay * 2, self.MAX_BACKOFF)
        logger.error("Massive poll failed (%s); next attempt in %.0fs", reason, self._current_delay)

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking HTTP call to the Massive REST API. Runs in a worker thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
