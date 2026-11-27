# Market Data Interface Design

Unified Python interface for market data in FinAlly. Two implementations (simulator and Massive API) behind one abstract interface. All downstream code — SSE streaming, price cache, portfolio valuation, trade execution — is source-agnostic.

## Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Massive REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution (read current price)
```

The key design principle: **producers** (data sources) write prices into the cache; **consumers** (SSE, portfolio, trading) read from the cache. Neither side knows about the other.

## Core Data Model

**File: `backend/app/market/models.py`**

```python
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

This is the **only data structure** that leaves the market data layer. Everything downstream works with `PriceUpdate` objects or their `to_dict()` serialization.

Design choices:
- **Frozen dataclass** — immutable after creation, safe to share across threads
- **`slots=True`** — memory-efficient, faster attribute access
- **Computed properties** — `change`, `change_percent`, `direction` derived from price/previous_price (no stale data)
- **`to_dict()`** — single serialization point for SSE and API responses

## Abstract Interface

**File: `backend/app/market/interface.py`**

```python
from abc import ABC, abstractmethod

class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.
        Starts a background asyncio task. Call exactly once.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. Also removes from PriceCache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

**Why this shape:**
- **Push, not pull** — data sources write to the cache proactively. Consumers never need to know about the source.
- **Async lifecycle** — `start()`/`stop()` manage background tasks; async so they can do I/O on startup (Massive does an immediate first poll).
- **Dynamic watchlist** — `add_ticker()`/`remove_ticker()` allow the watchlist to change at runtime without restarting.
- **No return values** — prices flow through the cache, not through method return values. This decouples producers from consumers.

## Price Cache

**File: `backend/app/market/cache.py`**

The shared in-memory store that bridges producers (data sources) and consumers (SSE, portfolio, trades).

```python
class PriceCache:
    """Thread-safe cache of latest prices per ticker."""

    def __init__(self):
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing, bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price. Computes direction from previous price. Returns PriceUpdate."""

    def get(self, ticker: str) -> PriceUpdate | None:
        """Latest update for a single ticker."""

    def get_price(self, ticker: str) -> float | None:
        """Convenience: just the price float."""

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices (shallow copy)."""

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache."""

    @property
    def version(self) -> int:
        """Monotonic counter — increments on every update. Used by SSE for change detection."""
```

**Key design decisions:**
- **Thread-safe with `Lock`** — the simulator runs in an asyncio task but numpy operations release the GIL, so we use a threading lock for safety
- **Version counter** — the SSE endpoint uses this to detect changes. If `version` hasn't changed since the last check, no event is sent. This avoids sending duplicate data.
- **Automatic direction computation** — `update()` compares against the previous price to set direction, so callers just pass the raw price
- **Round to 2 decimal places** — prices are rounded in `update()` to avoid floating-point noise in the UI

## Factory Function

**File: `backend/app/market/factory.py`**

Selects the data source implementation at startup:

```python
import os

def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        return SimulatorDataSource(price_cache=price_cache)
```

The factory uses **eager imports** (not lazy) since both `massive` and `numpy` are declared dependencies. The env var check is a simple string test — empty string or missing both resolve to the simulator.

## Massive Implementation

**File: `backend/app/market/massive_client.py`**

```python
class MassiveDataSource(MarketDataSource):
    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0):
        self._client: RESTClient | None = None
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()                    # Immediate first poll
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # Synchronous client → run in thread pool
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            for snap in snapshots:
                self._cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.timestamp / 1000,  # ms → sec
                )
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop retries on the next interval
```

**Implementation notes:**
- **`asyncio.to_thread()`** wraps the synchronous Massive client so it doesn't block the event loop
- **Immediate first poll** in `start()` ensures the cache has data before the first SSE event
- **Error handling**: individual snapshot parse errors are logged and skipped; HTTP errors are caught at the loop level
- **Ticker normalization**: `upper().strip()` ensures consistent matching

## Simulator Implementation

**File: `backend/app/market/simulator.py`**

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5, event_probability: float = 0.001):
        self._cache = price_cache
        self._interval = update_interval
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed cache with initial prices immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            if self._sim:
                prices = self._sim.step()       # {ticker: new_price}
                for ticker, price in prices.items():
                    self._cache.update(ticker=ticker, price=price)
            await asyncio.sleep(self._interval)
```

**Implementation notes:**
- **Cache seeded in `start()`** — initial prices are written before the loop starts so SSE has data immediately
- **Cache seeded in `add_ticker()`** — dynamically added tickers get a price right away
- **`GBMSimulator` is stateful** — it holds current prices and the Cholesky matrix; the `SimulatorDataSource` is a thin async wrapper

## SSE Streaming

**File: `backend/app/market/stream.py`**

The SSE endpoint reads from `PriceCache` and pushes to connected browsers:

```python
def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Factory: creates the SSE router with an injected PriceCache."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )
    return router

async def _generate_events(price_cache, request, interval=0.5):
    yield "retry: 1000\n\n"           # Auto-reconnect after 1s
    last_version = -1
    while True:
        if await request.is_disconnected():
            break
        current_version = price_cache.version
        if current_version != last_version:    # Only send on change
            last_version = current_version
            prices = price_cache.get_all()
            if prices:
                data = {t: u.to_dict() for t, u in prices.items()}
                yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(interval)
```

**Version-based change detection** means:
- If the data source hasn't updated, no SSE event is emitted (saves bandwidth)
- If the data source updates multiple times between SSE checks, the client gets the latest state (no missed updates matter — it's always the current snapshot)

## File Structure

```
backend/app/market/
├── __init__.py           # Re-exports: PriceCache, PriceUpdate, MarketDataSource, create_market_data_source
├── models.py             # PriceUpdate frozen dataclass
├── interface.py          # MarketDataSource ABC
├── cache.py              # PriceCache (thread-safe in-memory store)
├── factory.py            # create_market_data_source() — env-based selection
├── massive_client.py     # MassiveDataSource — REST polling implementation
├── simulator.py          # GBMSimulator + SimulatorDataSource
├── seed_prices.py        # SEED_PRICES, TICKER_PARAMS, correlation constants
└── stream.py             # SSE endpoint factory (create_stream_router)
```

## Lifecycle — How It All Fits Together

```
App Startup
│
├─ 1. cache = PriceCache()
├─ 2. source = create_market_data_source(cache)   # Reads MASSIVE_API_KEY
├─ 3. await source.start(initial_tickers)          # Background task begins
│      ├─ Simulator: seeds cache, starts GBM loop
│      └─ Massive: does first poll, starts poll loop
├─ 4. stream_router = create_stream_router(cache)  # Wire up SSE endpoint
│
│  Running
│  ├─ Data source writes to cache every 500ms (sim) or 15s (Massive)
│  ├─ SSE endpoint reads from cache every 500ms, pushes to browsers
│  ├─ Trade execution reads current price from cache.get_price()
│  ├─ Portfolio valuation reads from cache.get_all()
│  └─ Watchlist changes → source.add_ticker() / source.remove_ticker()
│
App Shutdown
└─ await source.stop()
```

## Usage from Downstream Code

```python
from app.market import PriceCache, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)
await source.start(["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"])

# Read prices (any consumer)
update = cache.get("AAPL")          # PriceUpdate or None
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist
await source.add_ticker("PYPL")
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```
