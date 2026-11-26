# Backend — Developer Guide

## Project Setup

```bash
cd backend
uv sync --extra dev   # Install all dependencies including test/lint tools
```

## Market Data API

The market data subsystem lives in `app/market/`. Use these imports:

```python
from app.market import (
    PriceCache, PriceUpdate, MarketDataSource, normalize_ticker, create_market_data_source,
)
```

### Core Types

- **`PriceUpdate`** — Immutable dataclass: `ticker`, `price`, `previous_price`, `timestamp`, `reference_price` (daily baseline: previous close for Massive, first price for the simulator), plus properties `change`, `change_percent`, `direction` ("up"/"down"/"flat"), `day_change`, `day_change_percent`, and `to_dict()` for JSON serialization.

- **`PriceCache`** — Thread-safe in-memory store. Key methods:
  - `update(ticker, price, timestamp=None, reference_price=None) -> PriceUpdate` (2 dp, or 4 dp below $1)
  - `get(ticker) -> PriceUpdate | None`
  - `get_price(ticker) -> float | None`
  - `get_all() -> dict[str, PriceUpdate]`
  - `remove(ticker)`
  - `version` property — monotonic counter, increments on every update and removal (for SSE change detection)

- **`MarketDataSource`** — Abstract interface implemented by `SimulatorDataSource` and `MassiveDataSource`. Lifecycle: optional `set_initial_prices({ticker: price})` -> `start(tickers)` (once) -> `add_ticker()` / `remove_ticker()` -> `stop()` (idempotent). All ticker arguments are normalized; invalid symbols raise `ValueError`. `add_ticker` before `start` is remembered.

- **`normalize_ticker(raw)`** — `" aapl "` -> `"AAPL"`; raises `ValueError` for anything not ticker-shaped. Use it in API routes and map `ValueError` to HTTP 400.

- **`create_market_data_source(cache)`** — Factory. Returns `MassiveDataSource` if `MASSIVE_API_KEY` is set (polling every `MASSIVE_POLL_INTERVAL` seconds, default 15), otherwise `SimulatorDataSource`.

### SSE Streaming

```python
from app.market import create_stream_router

router = create_stream_router(price_cache)  # Returns a NEW FastAPI APIRouter per call
# Endpoint: GET /api/stream/prices (text/event-stream)
# Each event is the full {ticker: PriceUpdate.to_dict()} map ({} when empty);
# ": ping" comment after 15s idle.
```

### Seed Data

Default tickers: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX. Seed prices and per-ticker volatility/drift params are in `app/market/seed_prices.py`.

## Running Tests

```bash
uv run --extra dev pytest -v              # All tests
uv run --extra dev pytest --cov=app       # With coverage
uv run --extra dev ruff check app/ tests/ # Lint
uv run --extra dev ruff format app/ tests/ # Format
```

## Demo

```bash
uv run market_data_demo.py   # Live terminal dashboard with simulated prices
```
