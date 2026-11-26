# Market Data Backend — Summary

**Status:** Complete and ready for integration. Both review rounds are resolved: round 1 (`archive/MARKET_DATA_REVIEW.md`) and round 2 (`MARKET_DATA_REVIEW.md`, all items in §7 done on 2026-11-26).

## What Was Built

A complete market data subsystem in `backend/app/market/` (9 modules, ~490 statements) providing live price simulation and real market data via a unified interface.

### Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution
```

### Modules

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate` — immutable frozen dataclass (ticker, price, previous_price, timestamp, reference_price; derived change, direction, day_change, day_change_percent) |
| `interface.py` | `MarketDataSource` ABC (`start/stop/add_ticker/remove_ticker/get_tickers`, optional `set_initial_prices`) + shared `normalize_ticker()` |
| `cache.py` | `PriceCache` — thread-safe price store; version bumps on update *and* remove; keeps a per-ticker `reference_price` |
| `seed_prices.py` | Realistic seed prices, per-ticker GBM params (drift/volatility), correlation groups |
| `simulator.py` | `GBMSimulator` (Geometric Brownian Motion with Cholesky-correlated moves) + `SimulatorDataSource` |
| `massive_client.py` | `MassiveDataSource` — REST poller (`sip_timestamp` ns, price fallbacks, prev close as reference, backoff, early poll on add) |
| `factory.py` | `create_market_data_source()` — `MASSIVE_API_KEY` selects Massive (`MASSIVE_POLL_INTERVAL`, default 15s), else simulator; lazy imports |
| `stream.py` | `create_stream_router()` — fresh router per call; full price map per event (`{}` when empty); 15s `: ping` heartbeat |

### Key Design Decisions

- **Strategy pattern** — both data sources implement the same ABC; downstream code is source-agnostic
- **PriceCache as single point of truth** — producers write, consumers read; no direct coupling
- **GBM with correlated moves** — Cholesky decomposition of sector-based correlation matrix; tech stocks correlate at 0.6, finance at 0.5, cross-sector at 0.3
- **Random shock events** — ~0.1% chance per tick per ticker of a 2-5% move for visual drama
- **SSE over WebSockets** — simpler, one-way push, universal browser support

## Test Suite

**185 tests, all passing (~5 s).** `ruff check` and `ruff format --check` are clean. Overall coverage is 99%.

| Module | Covers |
|--------|--------|
| test_models.py | `PriceUpdate` maths, day change, serialization |
| test_cache.py | cache semantics, reference price, version on remove, `timestamp=0`, sub-dollar precision, thread safety |
| test_simulator.py | GBM unit behaviour, deterministic seeds, initial prices |
| test_gbm_stats.py | realised volatility and correlation match parameters, seeded reproducibility, `dt` scaling, shock events |
| test_simulator_source.py | async lifecycle, pre-start adds, normalization, `dt` derived from interval |
| test_massive.py | field mapping on **real** `TickerSnapshot.from_dict` objects, fallbacks, backoff, remove-during-poll race, early poll + rate-limit gap, start timeout |
| test_conformance.py | the shared contract, parametrized over both sources (PLAN §12) |
| test_stream.py | SSE generator: retry, no duplicates, `{}` on empty, heartbeat, cancellation, re-entrant router |
| test_factory.py | source selection, `MASSIVE_POLL_INTERVAL` parsing, simulator mode doesn't import `massive` |

## Review Fixes

**Round 1** (7 issues): build config, return types, public `get_tickers()`, constant cleanup, and test hygiene.

**Round 2** (see `MARKET_DATA_REVIEW.md` §7):
1. **P0** Massive reads `last_trade.sip_timestamp` (ns) via `_extract()`, with price fallbacks `last_trade` → `day.close` → `prev_day.close`. Tests use real models.
2. **P0** SSE router is created per call, `{}` is sent on an empty map, and `remove()` bumps the version.
3. **P1** `reference_price` / `day_change` / `day_change_percent` for the watchlist's daily change %.
4. **P1** `normalize_ticker()` is shared by both sources (raises `ValueError`). The simulator honours pre-start adds. Conformance suite added.
5. **P1** Massive snapshots the ticker list for the worker thread, drops results for tickers removed mid-poll, backs off exponentially (max 120 s), reads `MASSIVE_POLL_INTERVAL`, and polls early on `add_ticker` (never closer than `min(interval, 12s)`). `start()` waits at most 10 s for the first poll.
6. **P2** `timestamp=0.0` is respected, `version` is read under the lock, a `: ping` heartbeat was added, `CancelledError` is re-raised, and the `Connection` header was dropped.
7. **P2** SSE generator tests and GBM statistics tests added; `ruff format` applied.
8. **P3** `set_initial_prices()` keeps P&L continuous across restarts. Unknown tickers get deterministic seed prices. Injectable `np.random.Generator`. `dt` is derived from `update_interval`. `start()` twice raises `RuntimeError`.
9. Sub-dollar prices keep 4 dp (prices ≥ $1 keep 2 dp). The deprecated `event_loop_policy` fixture was removed.

## Demo

A Rich terminal demo is available at `backend/market_data_demo.py`:

```bash
cd backend
uv run market_data_demo.py
```

Displays a live-updating dashboard with all 10 tickers, sparklines, color-coded direction arrows, and an event log for notable price moves. Runs 60 seconds or until Ctrl+C.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)  # Reads MASSIVE_API_KEY / MASSIVE_POLL_INTERVAL
source.set_initial_prices(last_known)       # optional {ticker: price} from DB (simulator only)
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update = cache.get("AAPL")          # PriceUpdate or None
update.day_change_percent           # watchlist "daily change %"
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist (validate user input first; ValueError -> HTTP 400)
ticker = normalize_ticker(raw)      # from app.market import normalize_ticker
await source.add_ticker(ticker)
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```

## Integration Notes (for `app/main.py`)

- Tracked tickers should be the **watchlist ∪ open positions**, so held tickers keep a price after leaving the watchlist (design §12).
- At start-up, call `source.set_initial_prices()` with each position's latest trade price, so the simulator doesn't reset P&L on restart.
- If a ticker has no cached price yet (e.g. a Massive add before the next poll), the trade route should refuse the trade.
