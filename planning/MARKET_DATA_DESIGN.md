# Market Data Backend — Design

**Scope:** everything under `backend/app/market/`: the unified data-source interface, the in-memory price cache, the GBM simulator, the Massive (Polygon.io) poller, the SSE endpoint, and how the rest of the backend uses them.

**Relationship to other docs:**
- `planning/PLAN.md` §6 is the requirement. This document is the implementation contract.
- `planning/archive/MARKET_DATA_DESIGN.md` was the original design. The code in `backend/app/market/` was built from it and passes 73 tests.
- This document **describes the code as it exists** and **specifies the changes still needed** (§15). Sections marked **🔧 Change** show code that differs from what is in the repo today; everything else matches the repo.

---

## Table of Contents

1. [Requirements](#1-requirements)
2. [Architecture](#2-architecture)
3. [File Layout & Public API](#3-file-layout--public-api)
4. [Data Model — `models.py`](#4-data-model--modelspy)
5. [Price Cache — `cache.py`](#5-price-cache--cachepy)
6. [Unified Interface — `interface.py`](#6-unified-interface--interfacepy)
7. [Seed Data — `seed_prices.py`](#7-seed-data--seed_pricespy)
8. [GBM Simulator — `simulator.py`](#8-gbm-simulator--simulatorpy)
9. [Massive API Client — `massive_client.py`](#9-massive-api-client--massive_clientpy)
10. [Factory — `factory.py`](#10-factory--factorypy)
11. [SSE Endpoint — `stream.py`](#11-sse-endpoint--streampy)
12. [FastAPI Integration](#12-fastapi-integration)
13. [Frontend Contract](#13-frontend-contract)
14. [Testing](#14-testing)
15. [Change List (Gaps in Current Code)](#15-change-list-gaps-in-current-code)
16. [Configuration Reference](#16-configuration-reference)

---

## 1. Requirements

Taken from `PLAN.md`:

| # | Requirement | Where satisfied |
|---|---|---|
| R1 | Two implementations (simulator, Massive) behind one interface | `MarketDataSource` ABC (§6) |
| R2 | Source chosen by `MASSIVE_API_KEY` (set and non-empty → Massive) | `create_market_data_source()` (§10) |
| R3 | Simulator: GBM, per-ticker drift/vol, ~500 ms updates | `GBMSimulator` (§8) |
| R4 | Simulator: correlated moves across tickers | Cholesky-correlated normals (§8.3) |
| R5 | Simulator: occasional 2–5% "events" | Shock events (§8.4) |
| R6 | Simulator: realistic seed prices | `SEED_PRICES` (§7) |
| R7 | Massive: REST polling of union of watched tickers; 15 s free tier, 2–15 s paid | `MassiveDataSource` (§9) |
| R8 | Shared in-memory cache: latest price, previous price, timestamp per ticker | `PriceCache` (§5) |
| R9 | `GET /api/stream/prices` SSE, ~500 ms cadence, each event has ticker, price, previous price, timestamp, direction | `stream.py` (§11) |
| R10 | EventSource auto-reconnect | `retry:` directive + heartbeat (§11) |
| R11 | Watchlist shows **daily change %** | `reference_price` / `day_change_percent` (§4, **🔧 new**) |
| R12 | Watchlist add/remove (manual or via AI) changes which tickers are priced | `add_ticker` / `remove_ticker` (§6, §12.3) |

---

## 2. Architecture

```
                 ┌──────────────────────────────┐
                 │   create_market_data_source  │  reads MASSIVE_API_KEY
                 └──────────────┬───────────────┘
                                │ returns one of
            ┌───────────────────┴────────────────────┐
            ▼                                        ▼
┌───────────────────────┐              ┌──────────────────────────┐
│  SimulatorDataSource  │              │    MassiveDataSource     │
│  asyncio task, 0.5 s  │              │  asyncio task, 15 s      │
│  GBMSimulator.step()  │              │  REST call in thread     │
└───────────┬───────────┘              └────────────┬─────────────┘
            │  cache.update(ticker, price, ...)     │
            └──────────────────┬────────────────────┘
                               ▼
                 ┌──────────────────────────────┐
                 │  PriceCache (Lock-guarded)   │  latest PriceUpdate per ticker
                 │  + version counter           │
                 └───┬──────────┬───────────┬───┘
                     │          │           │   reads only
                     ▼          ▼           ▼
             SSE stream    Portfolio    Trade execution / LLM context
          /api/stream/prices  valuation   (/api/portfolio/trade, /api/chat)
```

**Principles**

- **Producers push, consumers pull.** A data source never returns prices to callers; it writes them into `PriceCache`. Every consumer reads from the cache. Swapping the source changes nothing downstream.
- **One writer at a time.** Exactly one `MarketDataSource` runs per process.
- **Latest value only.** The cache holds one `PriceUpdate` per ticker. History (sparklines, the main chart) is accumulated **on the frontend** from the SSE stream. Portfolio value history lives in the `portfolio_snapshots` table, not here.
- **Never crash the loop.** Background tasks catch and log every exception, then continue on the next tick.

**Concurrency model**

| Actor | Runs on | Touches cache via |
|---|---|---|
| Simulator loop | asyncio event loop | `update()` |
| Massive poll | Worker thread (`asyncio.to_thread`) for the HTTP call; cache writes happen back on the event loop | `update()` |
| SSE generators (one per client) | asyncio event loop | `version`, `get_all()` |
| REST routes (trade, portfolio, chat) | asyncio event loop | `get()`, `get_price()` |

Today every cache access happens on the event loop. The `threading.Lock` costs almost nothing, and it keeps the cache safe if a source ever writes from a thread, or on free-threaded Python 3.13t+.

---

## 3. File Layout & Public API

```
backend/app/market/
  __init__.py         # Public re-exports (below)
  models.py           # PriceUpdate (frozen dataclass)
  cache.py            # PriceCache
  interface.py        # MarketDataSource (ABC)
  seed_prices.py      # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, correlation constants
  simulator.py        # GBMSimulator + SimulatorDataSource
  massive_client.py   # MassiveDataSource
  factory.py          # create_market_data_source()
  stream.py           # create_stream_router() → GET /api/stream/prices
backend/tests/market/ # pytest suite (§14)
backend/market_data_demo.py  # Rich terminal demo: `uv run market_data_demo.py`
```

The rest of the backend imports **only** from the package root:

```python
from app.market import (
    PriceUpdate,
    PriceCache,
    MarketDataSource,
    create_market_data_source,
    create_stream_router,
    normalize_ticker,          # 🔧 new (§6.1)
)
```

Dependencies (already in `backend/pyproject.toml`): `fastapi`, `uvicorn[standard]`, `numpy>=2`, `massive>=1.0.0`. Dev: `pytest`, `pytest-asyncio` (`asyncio_mode = "auto"`), `pytest-cov`, `ruff`.

---

## 4. Data Model — `models.py`

`PriceUpdate` is the only type that leaves the market-data layer.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of one ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    # 🔧 new: baseline for "daily change". Massive: previous session close.
    # Simulator: the first price seen this process lifetime ("session open").
    reference_price: float | None = None

    @property
    def change(self) -> float:
        """Tick-to-tick absolute change (drives the flash animation)."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Tick-to-tick percent change."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up' | 'down' | 'flat' relative to the previous tick."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    # 🔧 new ---------------------------------------------------------------
    @property
    def day_change(self) -> float:
        """Absolute change versus reference_price (0.0 if unknown)."""
        if not self.reference_price:
            return 0.0
        return round(self.price - self.reference_price, 4)

    @property
    def day_change_percent(self) -> float:
        """Percent change versus reference_price — the watchlist 'daily change %'."""
        if not self.reference_price:
            return 0.0
        return round((self.price - self.reference_price) / self.reference_price * 100, 4)
    # ----------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Single serialization point for SSE and REST."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "reference_price": self.reference_price,        # 🔧 new
            "day_change": self.day_change,                  # 🔧 new
            "day_change_percent": self.day_change_percent,  # 🔧 new
        }
```

**Design notes**
- `frozen=True`: safe to share between the producer and many SSE readers with no copying.
- `slots=True`: about 20 of these are created per second, so the memory saving is worth having.
- Derived values are **properties**, so `direction` can never disagree with `price` and `previous_price`.
- **Why `reference_price`:** `PLAN.md` §10 asks for "daily change %" in the watchlist. `change_percent` is tick-to-tick (around 0.01%), which is right for the flash animation and useless as a daily number. The new field is optional with a default, so existing callers and tests keep working.

Example:

```python
>>> u = PriceUpdate("AAPL", price=191.20, previous_price=191.05,
...                 timestamp=1_732_000_000.0, reference_price=190.00)
>>> u.direction, u.change, u.day_change_percent
('up', 0.15, 0.6316)
```

---

## 5. Price Cache — `cache.py`

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest PriceUpdate per ticker.

    Writers: exactly one MarketDataSource.
    Readers: SSE stream, portfolio valuation, trade execution, LLM context.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0  # bumped on every write; lets SSE skip unchanged ticks

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,   # 🔧 new
    ) -> PriceUpdate:
        """Record a new price. First update for a ticker is 'flat' (previous == price).

        reference_price: if given, it replaces the stored baseline (Massive passes
        the previous close on every poll). If omitted, the existing baseline is
        kept; for a brand-new ticker the first price becomes the baseline
        (simulator 'session open').
        """
        with self._lock:
            ts = time.time() if timestamp is None else timestamp   # 🔧 was `timestamp or time.time()`
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if reference_price is None:
                reference_price = prev.reference_price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
                reference_price=round(reference_price, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy — safe to iterate while writers continue."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1   # 🔧 new: removal is a change the SSE must push

    @property
    def version(self) -> int:
        with self._lock:          # 🔧 new: consistent with the rest of the class
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Why bump `version` on `remove`:** each SSE event is the full price map, so when a ticker is removed the next event simply leaves it out. With the simulator, a tick follows within 500 ms anyway. With Massive, the stream could keep showing the removed ticker for up to 15 s unless the removal itself counts as a change.

**Why round to 2 dp in the cache rather than in the source:** every consumer (UI, trade fills, P&L) sees the same price the user saw. The simulator keeps full precision internally, so rounding never compounds.

---

## 6. Unified Interface — `interface.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule. Downstream code reads the cache, never the source.

        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL"])   # exactly once
        await source.add_ticker("PYPL")          # any time after construction
        await source.remove_ticker("GOOGL")
        await source.stop()                      # idempotent
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing prices. Should leave the cache populated (or a first
        poll attempted) before returning, so the first SSE event has data."""

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
```

### 6.1 Contract rules (🔧 tightened)

These rules apply to **both** implementations. The shared conformance tests in §14.1 check them.

1. **Ticker normalization.** Every method accepts any case or whitespace and normalizes with one shared helper. Today Massive upper-cases tickers and the simulator does not. Add to `interface.py` and export:

   ```python
   import re

   _TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")

   def normalize_ticker(raw: str) -> str:
       """'  aapl ' -> 'AAPL'. Raises ValueError for anything that isn't ticker-shaped."""
       t = raw.strip().upper()
       if not _TICKER_RE.match(t):
           raise ValueError(f"Invalid ticker symbol: {raw!r}")
       return t
   ```

   The watchlist API calls it as well and turns `ValueError` into a 400. That keeps garbage like `"DROP TABLE"` out of both the DB and the data source.
2. **`add_ticker` before `start`** must be remembered. Today `SimulatorDataSource` silently ignores it. The fix is in §8.6.
3. **`remove_ticker` always clears the cache entry**, even if the source never tracked the ticker.
4. **`stop()` is idempotent**, and no cache writes happen after it returns.
5. **`get_tickers()` returns a copy.**

---

## 7. Seed Data — `seed_prices.py`

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00, "TSLA": 250.00,
    "NVDA": 800.00, "META": 500.00, "JPM": 195.00, "V": 280.00, "NFLX": 600.00,
}

# sigma = annualized volatility, mu = annualized drift
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},   # high vol
    "NVDA": {"sigma": 0.40, "mu": 0.08},   # high vol, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM":  {"sigma": 0.18, "mu": 0.04},   # bank, low vol
    "V":    {"sigma": 0.17, "mu": 0.04},   # payments, low vol
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR = 0.3   # cross-sector and unknown tickers
TSLA_CORR = 0.3          # TSLA does its own thing
```

**Unknown tickers** (for example, a user adds `PYPL`) start at a random price between $50 and $300 with `DEFAULT_PARAMS`. **🔧 Change:** seed the random price from the ticker name, so the same symbol always starts at the same price across restarts and in tests:

```python
def seed_price_for(ticker: str) -> float:
    if ticker in SEED_PRICES:
        return SEED_PRICES[ticker]
    return round(random.Random(ticker).uniform(50.0, 300.0), 2)
```

---

## 8. GBM Simulator — `simulator.py`

The file has two classes:
- `GBMSimulator` is pure math: synchronous, no I/O, and easy to unit test.
- `SimulatorDataSource` is the async `MarketDataSource` adapter that runs the simulator in a loop.

### 8.1 The math

Geometric Brownian Motion, solved exactly over a step `dt`:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )        Z ~ N(0, 1), correlated across tickers
```

- `dt` is the tick length as a fraction of a **trading year**: `0.5 s / (252 days × 6.5 h × 3600 s) ≈ 8.48e-8`.
- Per-tick standard deviation of the log return is `σ·√dt`. For AAPL that's `0.22 × 2.9e-4 ≈ 0.0064%`, about 1.2¢ on $190. Moves are small and smooth and add up to a realistic intraday range.
- `exp()` is always positive, so **prices can never go negative or hit zero**.
- The `−σ²/2` term is the Itô correction. Without it the *median* path would drift upward faster than `μ`.

Worked example (one AAPL tick, Z = +1.0):

```python
import math
S, mu, sigma, dt = 190.0, 0.05, 0.22, 0.5 / (252 * 6.5 * 3600)
S * math.exp((mu - 0.5 * sigma**2) * dt + sigma * math.sqrt(dt) * 1.0)
# → 190.01217...  (+1.2¢)
```

### 8.2 `GBMSimulator`

```python
from __future__ import annotations

import logging
import math
import random

import numpy as np

from .seed_prices import (
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS, INTRA_FINANCE_CORR,
    INTRA_TECH_CORR, TICKER_PARAMS, TSLA_CORR, seed_price_for,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600      # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR      # ≈ 8.48e-8

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT,
                 event_probability: float = 0.001) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}           # full precision
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for t in tickers:
            self._add_ticker_internal(t)
        self._rebuild_cholesky()                      # once, after the batch

    def step(self) -> dict[str, float]:
        """Advance every ticker one tick. Hot path: called every 500 ms."""
        n = len(self._tickers)
        if n == 0:
            return {}
        z = np.random.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z                    # correlate

        out: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:    # §8.4
                shock = random.uniform(0.02, 0.05) * random.choice((-1, 1))
                self._prices[ticker] *= 1 + shock
                logger.debug("Event on %s: %+.1f%%", ticker, shock * 100)

            out[ticker] = round(self._prices[ticker], 2)
        return out

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker], self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = seed_price_for(ticker)
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))
```

### 8.3 Correlated moves (Cholesky)

To make tickers move together, build an n×n correlation matrix `C` from the sector rules, factor it as `C = L·Lᵀ`, and turn independent normals `z` into correlated ones with `L·z`. The result has covariance `L·I·Lᵀ = C`.

```python
    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech, finance = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if "TSLA" in (t1, t2):
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

Resulting matrix for a subset (rows and columns in order AAPL, MSFT, TSLA, JPM, V):

```
        AAPL  MSFT  TSLA  JPM   V
AAPL    1.0   0.6   0.3   0.3   0.3
MSFT    0.6   1.0   0.3   0.3   0.3
TSLA    0.3   0.3   1.0   0.3   0.3
JPM     0.3   0.3   0.3   1.0   0.5
V       0.3   0.3   0.3   0.5   1.0
```

**Positive-definiteness:** the matrix can be written as `C = 0.3·J + 0.3·J_tech + 0.2·J_fin + D`. Here `J` is the all-ones matrix, `J_tech` and `J_fin` are all-ones on their sector's block (TSLA is excluded from tech by the override), and `D` is diagonal with entries 0.4 for tech, 0.5 for finance and 0.7 for everything else. The all-ones terms are positive semidefinite and `D` is positive definite, so `C` is positive definite for any set of tickers, and `np.linalg.cholesky` can't fail. (Verified numerically: for 46 tickers the smallest eigenvalue is 0.4.) Rebuilding is O(n³), which is trivial for n < 50, and only happens on add or remove.

### 8.4 Random events

Each ticker has a `0.001` chance per tick of a ±2–5% jump. At 2 ticks/s across 10 tickers, that's about one event somewhere every 50 s, enough to make the heatmap and the flash animations interesting. The shock multiplies the price, so the price stays positive, and the GBM carries on from the new level (a "gap" with no mean reversion).

### 8.5 Per-tick cost

With n = 10: one 10-sample `standard_normal`, one 10×10 mat-vec, and 10 `exp` calls. That's well under 50 µs, so the event loop isn't blocked.

### 8.6 `SimulatorDataSource`

```python
import asyncio

from .cache import PriceCache
from .interface import MarketDataSource, normalize_ticker


class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5,
                 event_probability: float = 0.001) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._pending: list[str] = []                # 🔧 new: adds before start()
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        initial = list(dict.fromkeys(normalize_ticker(t) for t in [*tickers, *self._pending]))
        self._sim = GBMSimulator(initial, event_probability=self._event_prob)
        for t in initial:                            # seed cache → first SSE event has data
            self._cache.update(t, self._sim.get_price(t))
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(initial))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is None:
            if ticker not in self._pending:
                self._pending.append(ticker)
            return
        self._sim.add_ticker(ticker)
        self._cache.update(ticker, self._sim.get_price(ticker))   # visible immediately

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        if ticker in self._pending:
            self._pending.remove(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else list(self._pending)

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker, price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Because the loop is single-threaded asyncio, `add_ticker` and `remove_ticker` can't interleave with `step()`. No locking is needed around the simulator.

---

## 9. Massive API Client — `massive_client.py`

### 9.1 Endpoint

One call fetches every watched ticker, which is what keeps the free tier (5 requests/min) workable:

```
GET https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...
Authorization: Bearer $MASSIVE_API_KEY
```

Python client (`massive` package, synchronous):

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key=key)
snaps = client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS,
                                tickers=["AAPL", "MSFT"])
```

Raw JSON for one ticker (abbreviated):

```json
{
  "ticker": "AAPL",
  "todaysChange": 1.33, "todaysChangePerc": 0.70,
  "updated": 1732464000123456789,
  "day":     {"o": 189.1, "h": 191.8, "l": 188.7, "c": 191.2, "v": 41234567},
  "prevDay": {"o": 187.0, "h": 190.3, "l": 186.5, "c": 189.87, "v": 52000000},
  "lastTrade": {"p": 191.20, "s": 100, "x": 4, "t": 1732464000123000000},
  "lastQuote": {"p": 191.19, "P": 191.21, "t": 1732464000120000000}
}
```

### 9.2 Field mapping (verified against the installed `massive` models)

The client parses JSON into `TickerSnapshot` dataclasses. These attribute names were checked against the installed package (`massive/rest/models/snapshot.py`, `trades.py`, `aggs.py`):

| We need | `TickerSnapshot` attribute | Notes |
|---|---|---|
| Ticker | `snap.ticker` | |
| Current price | `snap.last_trade.price` | `last_trade` may be `None` (thin or halted tickers) |
| Fallback price | `snap.day.close`, then `snap.prev_day.close` | `day` values are `0` pre-market (snapshot resets at midnight ET) |
| Timestamp | `snap.last_trade.sip_timestamp` | **Nanoseconds** (Unix). Fallback: `snap.updated` (also ns) |
| Daily baseline | `snap.prev_day.close` | → `reference_price` |

> ⚠️ **Bug in the current code:** `massive_client.py` reads `snap.last_trade.timestamp / 1000.0`. `LastTrade` has **no** `timestamp` attribute; the fields are `sip_timestamp`, `participant_timestamp` and `trf_timestamp`, all in nanoseconds. Every real snapshot raises `AttributeError`, which the per-snapshot `try` catches and logs as "Skipping snapshot", so **with a real API key the cache never gets a price**. The tests don't catch it because they build snapshots from `MagicMock`, which invents any attribute you ask for. This was verified in the repo environment:
> ```python
> >>> s = TickerSnapshot.from_dict({"ticker": "AAPL", "lastTrade": {"p": 190.5, "t": 1707580800000000000}})
> >>> hasattr(s.last_trade, "timestamp")
> False
> >>> s.last_trade.sip_timestamp
> 1707580800000000000
> ```
> The fix is the `_extract()` function in §9.3, and the regression test in §14.4 uses real `TickerSnapshot.from_dict` objects.

### 9.3 Implementation (🔧 revised)

```python
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


def _extract(snap) -> tuple[str, float, float, float | None] | None:
    """Map a TickerSnapshot → (ticker, price, timestamp_s, prev_close) or None if unusable."""
    ticker = getattr(snap, "ticker", None)
    if not ticker:
        return None

    lt, day, prev = snap.last_trade, snap.day, snap.prev_day
    price = (lt.price if lt and lt.price else None) \
        or (day.close if day and day.close else None) \
        or (prev.close if prev and prev.close else None)
    if not price or price <= 0:
        return None

    ts_ns = (lt.sip_timestamp if lt else None) or snap.updated
    ts = ts_ns / NS_PER_S if ts_ns else time.time()

    prev_close = prev.close if prev and prev.close else None
    return ticker, float(price), ts, prev_close


class MassiveDataSource(MarketDataSource):
    """Polls the Massive snapshot endpoint for all watched tickers in one call."""

    MAX_BACKOFF = 120.0

    def __init__(self, api_key: str, price_cache: PriceCache,
                 poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._current_delay = poll_interval
        self._tickers: list[str] = []
        self._client: RESTClient | None = None
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()      # set by add_ticker to poll early
        self._last_poll = 0.0

    # ---- lifecycle -------------------------------------------------------
    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        for t in tickers:
            t = normalize_ticker(t)
            if t not in self._tickers:
                self._tickers.append(t)
        await self._poll_once()           # cache warm before the first SSE event
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller: %d tickers every %.1fs", len(self._tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            self._wake.set()              # don't make the user wait a full interval

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # ---- polling ---------------------------------------------------------
    async def _poll_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._current_delay)
            except TimeoutError:
                pass
            self._wake.clear()
            # Never poll faster than the configured interval (rate limit) —
            # an early wake just shortens the remaining wait to the minimum.
            gap = self._interval - (time.monotonic() - self._last_poll)
            if gap > 0:
                await asyncio.sleep(gap)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        tickers = list(self._tickers)     # snapshot: the list may change mid-request
        self._last_poll = time.monotonic()
        try:
            snaps = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except (AuthError, BadResponse) as e:   # bad key / 403 / 429 (after client retries)
            self._back_off(f"HTTP error: {e}")
            return
        except Exception as e:            # network, DNS, JSON
            self._back_off(f"{type(e).__name__}: {e}")
            return

        self._current_delay = self._interval          # success → reset backoff
        updated = 0
        for snap in snaps:
            row = _extract(snap)
            if row is None:
                logger.warning("Skipping unusable snapshot for %s", getattr(snap, "ticker", "?"))
                continue
            ticker, price, ts, prev_close = row
            if ticker not in self._tickers:            # removed while request was in flight
                continue
            self._cache.update(ticker, price, timestamp=ts, reference_price=prev_close)
            updated += 1

        missing = set(tickers) - {getattr(s, "ticker", None) for s in snaps}
        if missing:
            logger.warning("Massive returned no data for: %s", ", ".join(sorted(missing)))
        logger.debug("Massive poll: %d/%d updated", updated, len(tickers))

    def _back_off(self, reason: str) -> None:
        self._current_delay = min(self._current_delay * 2, self.MAX_BACKOFF)
        logger.error("Massive poll failed (%s); next attempt in %.0fs", reason, self._current_delay)

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking HTTP call — runs in a worker thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS, tickers=tickers,
        )
```

> `massive.exceptions` exports `AuthError` (missing or invalid key) and `BadResponse` (non-2xx after the client's built-in retries). Both are verified in the installed package. Catch `(AuthError, BadResponse)` in the first `except`. The separate branch exists only to give HTTP failures and network failures distinct log messages; both back off.

### 9.4 Behaviour notes

| Situation | Behaviour |
|---|---|
| Market closed / weekend | `last_trade.price` is the last trade (it may include after-hours). Prices are static, so each poll writes the same price, `direction` is `"flat"`, and nothing flashes. This is expected; the UI shows a stable "last" price. |
| Pre-market (00:00–04:00 ET) | `day` is all zeros; `_extract` falls back to `prev_day.close`. |
| Unknown symbol (user adds `ZZZZ`) | Missing from the response, so it's logged once per poll and has no cache entry. The watchlist API shows it with `price: null` (§12.3). |
| 401 bad key | Backs off to 120 s and logs an error. The app keeps running with an empty cache. Falling back to the simulator is deliberately **not** automatic: silently showing fake prices to someone who configured real data would be misleading. |
| 429 rate limit | Exponential backoff 15 → 30 → 60 → 120 s, reset on the first success. |
| Polling cadence | `poll_interval` from `MASSIVE_POLL_INTERVAL` (§16). Free: 15 s (4 calls/min, under the 5/min limit, which leaves headroom for the immediate poll on `start` and for `add_ticker` wakes). Paid: 2–5 s. |
| `previous_price` semantics | The price at the previous **poll**, so `direction` and the flash still work, just every 15 s instead of every 0.5 s. |

---

## 10. Factory — `factory.py`

```python
import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set & non-blank → Massive; otherwise the GBM simulator.
    Returns an UNSTARTED source; caller awaits source.start(tickers)."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval = float(os.environ.get("MASSIVE_POLL_INTERVAL", "15"))   # 🔧 new
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key, price_cache, poll_interval=interval)
    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache)
```

Examples:

```bash
MASSIVE_API_KEY=            # → simulator
MASSIVE_API_KEY="   "       # → simulator (whitespace stripped)
MASSIVE_API_KEY=abc123      # → Massive, 15 s
MASSIVE_API_KEY=abc123 MASSIVE_POLL_INTERVAL=3   # → Massive, 3 s (paid tier)
```

---

## 11. SSE Endpoint — `stream.py`

### 11.1 Wire format

`GET /api/stream/prices` → `Content-Type: text/event-stream`

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.12,"previous_price":190.05,"timestamp":1732464000.51,"change":0.07,"change_percent":0.0368,"direction":"up","reference_price":190.0,"day_change":0.12,"day_change_percent":0.0632},"MSFT":{...}}

data: {"AAPL":{...},"MSFT":{...}}

: ping

```

- **One event = the full price map** for every tracked ticker, keyed by ticker. This is deliberate. It's idempotent, so a client that reconnects or misses events is fully caught up on the next one, and a removed ticker simply disappears. Ten tickers come to about 2.5 KB per 500 ms, which is negligible on localhost.
- `retry: 1000` tells `EventSource` to reconnect 1 s after a drop.
- `: ping` (🔧 new) is an SSE comment that clients ignore. It's sent after 15 s without data so that proxies, load balancers (App Runner, Render) and browsers don't close an idle stream. This matters in Massive mode, and in simulator mode when the watchlist is empty.

### 11.2 Implementation (🔧 revised)

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 15.0


def create_stream_router(price_cache: PriceCache, interval: float = 0.5) -> APIRouter:
    """Build the SSE router bound to a PriceCache.

    🔧 The router is created INSIDE the factory. The current code uses a
    module-level `router`, so calling the factory twice (e.g. two test apps)
    registers /prices twice on the same object.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request, interval),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache, request: Request, interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"
    last_version = -1
    idle = 0.0
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                payload = {t: u.to_dict() for t, u in price_cache.get_all().items()}
                yield f"data: {json.dumps(payload)}\n\n"   # sent even if {} (after a remove)
                idle = 0.0
            elif idle >= HEARTBEAT_SECONDS:
                yield ": ping\n\n"
                idle = 0.0
            await asyncio.sleep(interval)
            idle += interval
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
```

**Why poll the version rather than use an `asyncio.Condition`:** at a 500 ms cadence, polling an int costs nothing. It also caps every client at 2 events/s whatever the producer rate, and it needs no subscriber bookkeeping or cleanup. If the source ever ticks faster than the stream, the extra ticks are naturally coalesced.

**Behaviour change:** the current code skips the send when `prices` is empty. The revised code sends `{}`, so a client whose last ticker was removed actually sees it disappear.

---

## 12. FastAPI Integration

### 12.1 Lifespan wiring (`backend/app/main.py`, not yet written)

```python
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from app.db import get_watchlist_tickers, init_db          # owned by the DB/backend work
from app.market import PriceCache, create_market_data_source, create_stream_router

price_cache = PriceCache()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()                                               # lazy create + seed
    source = create_market_data_source(price_cache)
    await source.start(get_watchlist_tickers(user_id="default"))
    app.state.price_cache = price_cache
    app.state.market_source = source
    # (portfolio snapshot task — every 30s — is started here too; not market data)
    try:
        yield
    finally:
        await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
# ... include portfolio / watchlist / chat / health routers ...

static_dir = Path(__file__).parent.parent / "static"        # Next.js export copied here
if static_dir.exists():
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")  # mount LAST
```

Run locally:

```bash
cd backend && uv run uvicorn app.main:app --reload --port 8000
```

```bash
curl -N http://localhost:8000/api/stream/prices
```

### 12.2 Dependency helpers for routes

```python
# app/deps.py
from fastapi import Request

from app.market import MarketDataSource, PriceCache


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

Tests can then override these with `app.dependency_overrides[get_price_cache] = lambda: test_cache`.

### 12.3 Watchlist coordination

The **DB is the source of truth** for which tickers the user watches. The data source mirrors it.

```python
# app/routes/watchlist.py (sketch)
@router.post("", status_code=201)
async def add_to_watchlist(body: AddTicker,
                           cache: PriceCache = Depends(get_price_cache),
                           source: MarketDataSource = Depends(get_market_source)):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(400, str(e))
    db.add_watchlist(ticker)                   # INSERT OR IGNORE (unique user_id, ticker)
    await source.add_ticker(ticker)            # idempotent
    u = cache.get(ticker)
    return {"ticker": ticker, "price": u.price if u else None}


@router.delete("/{ticker}", status_code=204)
async def remove_from_watchlist(ticker: str,
                                source: MarketDataSource = Depends(get_market_source)):
    ticker = normalize_ticker(ticker)
    db.remove_watchlist(ticker)
    if not db.has_position(ticker):            # keep pricing held positions!
        await source.remove_ticker(ticker)


@router.get("")
async def get_watchlist(cache: PriceCache = Depends(get_price_cache)):
    return [
        {"ticker": t, **(cache.get(t).to_dict() if cache.get(t) else {"price": None})}
        for t in db.get_watchlist_tickers()
    ]
```

**Rule: tracked tickers = watchlist ∪ open positions.**
- If a user removes `NVDA` from the watchlist while holding NVDA, the source keeps pricing it. Otherwise portfolio valuation and selling would break.
- On startup, `lifespan` should call `source.start(watchlist ∪ position_tickers)`.
- After a sell closes a position, if the ticker isn't on the watchlist, call `source.remove_ticker()`.
- The AI chat's `watchlist_changes` go through **the same functions** as these routes; don't duplicate the logic.

### 12.4 Other consumers

```python
# Trade execution: fill at the cached price; refuse if we have none.
price = cache.get_price(ticker)
if price is None:
    raise HTTPException(400, f"No price available for {ticker}; add it to the watchlist and retry.")

# Portfolio valuation
value = cash + sum(p.quantity * (cache.get_price(p.ticker) or p.avg_cost) for p in positions)
#  (falls back to avg_cost if a price is momentarily missing → P&L shows 0 rather than crashing)

# LLM context
watch = {t: u.to_dict() for t, u in cache.get_all().items()}
```

Buying a ticker that isn't tracked: the trade route should `await source.add_ticker(ticker)` first. In simulator mode the price is available immediately. In Massive mode it isn't until the next poll, so return the 400 above and let the user retry. This is the documented behaviour, not a bug.

---

## 13. Frontend Contract

The TypeScript type mirrors `PriceUpdate.to_dict()`:

```ts
export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;            // Unix seconds (float)
  change: number;               // tick-to-tick
  change_percent: number;       // tick-to-tick
  direction: "up" | "down" | "flat";
  reference_price: number | null;
  day_change: number;
  day_change_percent: number;   // ← watchlist "daily change %"
}
export type PriceMap = Record<string, PriceUpdate>;
```

Minimal hook with connection status and sparkline accumulation:

```ts
type Status = "connected" | "reconnecting" | "disconnected";
const MAX_POINTS = 600;   // ~5 min at 2/s

export function usePriceStream() {
  const [prices, setPrices] = useState<PriceMap>({});
  const [status, setStatus] = useState<Status>("reconnecting");
  const history = useRef<Record<string, { time: number; value: number }[]>>({});

  useEffect(() => {
    const es = new EventSource("/api/stream/prices");
    es.onopen = () => setStatus("connected");
    es.onerror = () =>
      setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
    es.onmessage = (e) => {
      const map: PriceMap = JSON.parse(e.data);
      for (const u of Object.values(map)) {
        const h = (history.current[u.ticker] ??= []);
        if (h.at(-1)?.time !== u.timestamp) {
          h.push({ time: u.timestamp, value: u.price });
          if (h.length > MAX_POINTS) h.shift();
        }
      }
      setPrices(map);
    };
    return () => es.close();
  }, []);

  return { prices, status, history: history.current };
}
```

- Flash: when `direction !== "flat"` **and** `timestamp` changed since the last render, add the `flash-up` or `flash-down` class and remove it after ~500 ms.
- Dedup by `timestamp`. In Massive mode, a cache write from an unchanged poll has a new version but the same trade timestamp, so it doesn't add a duplicate sparkline point.

---

## 14. Testing

Run from `backend/`:

```bash
uv run --extra dev pytest -v
```

```bash
uv run --extra dev pytest --cov=app --cov-report=term-missing
```

```bash
uv run --extra dev ruff check app/ tests/
```

Current state: **185 tests, all passing, 99% coverage.** Every addition below has been implemented (see `MARKET_DATA_SUMMARY.md`).

### 14.1 Interface conformance (new: `tests/market/test_conformance.py`)

The same tests run against both sources. This is how R1 ("both implementations conform to the abstract interface") actually gets checked.

```python
import pytest
from unittest.mock import MagicMock

from app.market import PriceCache
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


@pytest.fixture(params=["simulator", "massive"])
async def source(request):
    cache = PriceCache()
    if request.param == "simulator":
        src = SimulatorDataSource(cache, update_interval=0.01)
    else:
        src = MassiveDataSource("k", cache, poll_interval=60)
        src._fetch_snapshots = MagicMock(return_value=[])   # no network
    yield src, cache
    await src.stop()


async def test_add_is_normalized_and_idempotent(source):
    src, _ = source
    await src.start(["AAPL"])
    await src.add_ticker("  tsla ")
    await src.add_ticker("TSLA")
    assert src.get_tickers().count("TSLA") == 1


async def test_remove_clears_cache(source):
    src, cache = source
    await src.start(["AAPL"])
    cache.update("AAPL", 190.0)
    await src.remove_ticker("aapl")
    assert "AAPL" not in cache and "AAPL" not in src.get_tickers()


async def test_stop_is_idempotent(source):
    src, _ = source
    await src.start(["AAPL"])
    await src.stop()
    await src.stop()


async def test_invalid_ticker_rejected(source):
    src, _ = source
    await src.start([])
    with pytest.raises(ValueError):
        await src.add_ticker("DROP TABLE")
```

### 14.2 GBM statistics

```python
import numpy as np
from app.market.simulator import GBMSimulator


def test_log_returns_have_expected_volatility():
    np.random.seed(0)
    sim = GBMSimulator(["AAPL"], event_probability=0.0)
    prices = [sim.get_price("AAPL")]
    for _ in range(20_000):
        sim.step()
        prices.append(sim.get_price("AAPL"))       # unrounded internal price
    r = np.diff(np.log(prices))
    expected = 0.22 * np.sqrt(GBMSimulator.DEFAULT_DT)
    assert abs(r.std() / expected - 1) < 0.05      # within 5%


def test_tech_correlation_is_realised():
    np.random.seed(1)
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"], event_probability=0.0)
    series = {t: [sim.get_price(t)] for t in sim.get_tickers()}
    for _ in range(20_000):
        sim.step()
        for t in series:
            series[t].append(sim.get_price(t))
    r = {t: np.diff(np.log(v)) for t, v in series.items()}
    assert abs(np.corrcoef(r["AAPL"], r["MSFT"])[0, 1] - 0.6) < 0.05
    assert abs(np.corrcoef(r["AAPL"], r["JPM"])[0, 1] - 0.3) < 0.05


def test_cholesky_ok_for_many_unknown_tickers():
    sim = GBMSimulator([f"T{i}" for i in range(40)] + ["AAPL", "TSLA", "JPM", "V"])
    assert len(sim.step()) == 44
```

### 14.3 Cache additions

```python
def test_reference_price_defaults_to_first_price_and_persists():
    c = PriceCache()
    c.update("AAPL", 190.0)
    u = c.update("AAPL", 192.0)
    assert u.reference_price == 190.0 and u.day_change_percent == pytest.approx(1.0526, abs=1e-3)


def test_explicit_reference_price_overrides():
    c = PriceCache()
    u = c.update("AAPL", 191.2, reference_price=189.87)
    assert u.day_change == pytest.approx(1.33)


def test_remove_bumps_version():
    c = PriceCache()
    c.update("AAPL", 190.0)
    v = c.version
    c.remove("AAPL")
    assert c.version == v + 1


def test_timestamp_zero_is_respected():
    assert PriceCache().update("X", 1.0, timestamp=0.0).timestamp == 0.0
```

### 14.4 Massive with **real** model objects (regression for the timestamp bug)

```python
from unittest.mock import MagicMock

from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource, _extract


def real_snap(ticker, p=None, t=None, day_c=None, prev_c=None):
    d = {"ticker": ticker}
    if p is not None:
        d["lastTrade"] = {"p": p, "t": t}
    if day_c is not None:
        d["day"] = {"c": day_c}
    if prev_c is not None:
        d["prevDay"] = {"c": prev_c}
    return TickerSnapshot.from_dict(d)


def test_extract_uses_sip_timestamp_in_ns():
    assert _extract(real_snap("AAPL", 191.2, 1_732_464_000_000_000_000, prev_c=189.87)) == (
        "AAPL", 191.2, 1_732_464_000.0, 189.87,
    )


def test_extract_falls_back_to_prev_close_premarket():
    ticker, price, _, _ = _extract(real_snap("AAPL", day_c=0, prev_c=189.87))
    assert price == 189.87


async def test_poll_with_real_snapshots_populates_cache():
    cache = PriceCache()
    src = MassiveDataSource("k", cache, poll_interval=60)
    src._client = MagicMock()
    src._tickers = ["AAPL"]
    src._fetch_snapshots = MagicMock(
        return_value=[real_snap("AAPL", 191.2, 1_732_464_000_000_000_000, prev_c=189.87)]
    )
    await src._poll_once()
    assert cache.get_price("AAPL") == 191.2      # FAILS on current code


async def test_failure_backs_off_then_resets():
    src = MassiveDataSource("k", PriceCache(), poll_interval=15)
    src._client = MagicMock()
    src._tickers = ["AAPL"]
    src._fetch_snapshots = MagicMock(side_effect=RuntimeError("boom"))
    await src._poll_once()
    assert src._current_delay == 30
    src._fetch_snapshots = MagicMock(return_value=[])
    await src._poll_once()
    assert src._current_delay == 15
```

### 14.5 SSE generator (no server needed)

```python
import json

from app.market.cache import PriceCache
from app.market.stream import _generate_events


class FakeRequest:
    client = None

    def __init__(self, polls_before_disconnect):
        self.n = polls_before_disconnect

    async def is_disconnected(self):
        self.n -= 1
        return self.n < 0


async def test_stream_emits_retry_then_data():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    events = [e async for e in _generate_events(cache, FakeRequest(2), interval=0.001)]
    assert events[0] == "retry: 1000\n\n"
    data = json.loads(events[1].removeprefix("data: ").strip())
    assert data["AAPL"]["price"] == 190.0
    assert len(events) == 2                       # version unchanged → no duplicate


async def test_router_factory_is_reentrant():
    from app.market import create_stream_router
    r1, r2 = create_stream_router(PriceCache()), create_stream_router(PriceCache())
    assert r1 is not r2 and len(r1.routes) == 1
```

### 14.6 E2E (owned by `test/`)

With `MASSIVE_API_KEY` unset (simulator), Playwright checks that within 2 s of load all 10 default tickers show a price, that at least one cell flashes within 5 s, and that the status dot is green. The SSE resilience test blocks `/api/stream/prices` via `page.route`, expects yellow or red, unblocks it, and expects green again.

---

## 15. Change List (Gaps in Current Code)

> ✅ Items 1–8 were implemented on 2026-11-26. Item 9 (app integration) is the next phase.

Prioritized. Each item points at the section that has the target code.

| # | Severity | File | Change | § |
|---|---|---|---|---|
| 1 | **Critical** | `massive_client.py` | `last_trade.timestamp` doesn't exist → every real snapshot is skipped. Use `sip_timestamp` (ns) via `_extract()`, with price fallbacks. Add tests that use real `TickerSnapshot.from_dict`. | 9.2, 9.3, 14.4 |
| 2 | High | `models.py`, `cache.py`, `massive_client.py` | Add `reference_price` + `day_change(_percent)` so the watchlist can show daily change % (PLAN §10). Massive passes `prev_day.close`; the simulator uses the first price. | 4, 5 |
| 3 | High | `stream.py` | Create the `APIRouter` inside `create_stream_router` (re-entrancy). Add the 15 s `: ping` heartbeat. Send `{}` instead of skipping. | 11.2 |
| 4 | Medium | `massive_client.py` | Exponential backoff on errors; `MASSIVE_POLL_INTERVAL`; early poll on `add_ticker` (still rate-limited); pass a **copy** of the ticker list to the worker thread; drop results for tickers removed mid-flight. | 9.3, 10 |
| 5 | Medium | `interface.py`, both sources | Shared `normalize_ticker()` with validation; the simulator honours `add_ticker` before `start`. | 6.1, 8.6 |
| 6 | Medium | `cache.py` | `remove()` bumps `version`; `version` read under the lock; `timestamp=0.0` no longer replaced by `now`. | 5 |
| 7 | Low | `seed_prices.py`, `simulator.py` | Deterministic seed price for unknown tickers (`seed_price_for`). | 7 |
| 8 | Low | tests | Conformance suite, GBM statistics, SSE generator tests. | 14 |
| 9 | Integration | `app/main.py` (new), watchlist/trade routes | Lifespan wiring; tracked tickers = watchlist ∪ positions. | 12 |

None of these change the public import surface, except for adding `normalize_ticker`. Existing tests should keep passing; the one exception is any assertion that `remove()` leaves `version` unchanged (none exists today).

---

## 16. Configuration Reference

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | *(empty)* | Non-blank → Massive poller; blank or absent → simulator |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between Massive polls (🔧 new). Free tier ≥ 12; paid 2–5 |

| Constant | Value | Location |
|---|---|---|
| Simulator tick | 0.5 s | `SimulatorDataSource(update_interval=)` |
| Shock probability | 0.001 per ticker per tick | `SimulatorDataSource(event_probability=)` |
| Shock size | ±2–5% | `GBMSimulator.step` |
| `dt` | 0.5 / 5,896,800 ≈ 8.48e-8 | `GBMSimulator.DEFAULT_DT` |
| SSE check interval | 0.5 s | `create_stream_router(interval=)` |
| SSE heartbeat | 15 s | `stream.HEARTBEAT_SECONDS` |
| SSE client retry | 1000 ms | `retry:` line |
| Massive max backoff | 120 s | `MassiveDataSource.MAX_BACKOFF` |
| Price precision | 2 dp (cache); full precision inside the simulator | `PriceCache.update` |
