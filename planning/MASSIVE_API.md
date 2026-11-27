# Massive API Reference (formerly Polygon.io)

Reference documentation for the Massive (formerly Polygon.io) REST API as used in FinAlly. Polygon.io rebranded to Massive in early 2026; the Python package is now `massive` and the base URL is `https://api.massive.com`.

## Overview

| Item | Detail |
|------|--------|
| **Base URL** | `https://api.massive.com` (legacy `https://api.polygon.io` still works) |
| **Python package** | `massive` — install via `pip install massive-api-client` or `uv add massive-api-client` |
| **Min Python** | 3.8+ |
| **Auth (header)** | `Authorization: Bearer <API_KEY>` |
| **Auth (query)** | `?apiKey=<API_KEY>` |
| **Source** | [GitHub — client-python](https://github.com/massive-com/client-python) |

The Python client handles authentication automatically when initialized with an API key or when the `MASSIVE_API_KEY` environment variable is set.

## Rate Limits & Plan Tiers

| Tier | Rate Limit | Data Freshness | Price |
|------|-----------|----------------|-------|
| Free | 5 requests/minute | End-of-day | $0 |
| Stocks Starter | Higher (not published) | 15-min delayed | $29/mo |
| Stocks Developer | Higher | 15-min delayed | — |
| Stocks Advanced | Unlimited (recommended: <100 req/s) | Real-time | $199/mo |
| Stocks Business | Unlimited | Real-time + FMV | Custom |

For FinAlly, we poll on a timer sized to the user's tier:
- **Free tier**: poll every 15 seconds (safe under 5 req/min)
- **Paid tiers**: poll every 2–5 seconds

## Client Initialization

```python
from massive import RESTClient

# Option 1: reads MASSIVE_API_KEY from environment automatically
client = RESTClient()

# Option 2: pass explicitly
client = RESTClient(api_key="your_key_here")
```

## Endpoints Used in FinAlly

### 1. Snapshot — Multiple Tickers (Primary Endpoint)

Gets current prices for multiple tickers in a **single API call**. This is the main endpoint we use for polling — one call covers the entire watchlist.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT`

**Python client**:
```python
from massive import RESTClient

client = RESTClient()  # uses MASSIVE_API_KEY env var

# Get snapshots for specific tickers (one API call)
snapshots = client.get_snapshot_all(
    market_type="stocks",
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    print(f"  Today's change: {snap.todays_change_perc}%")
    print(f"  Day OHLC: O={snap.day.open} H={snap.day.high} L={snap.day.low} C={snap.day.close}")
    print(f"  Volume: {snap.day.volume}")
```

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tickers` | string (comma-separated) | No | Ticker symbols. Empty = all tickers (10,000+) |
| `include_otc` | boolean | No | Include OTC securities. Default: false |

**Response structure** (per ticker in the `tickers` array):
```json
{
  "ticker": "AAPL",
  "day": {
    "o": 129.61,
    "h": 130.15,
    "l": 125.07,
    "c": 125.07,
    "v": 111237700,
    "vw": 127.35
  },
  "prevDay": {
    "o": 130.00,
    "h": 131.50,
    "l": 129.00,
    "c": 129.61,
    "v": 95000000,
    "vw": 130.10
  },
  "lastTrade": {
    "p": 125.07,
    "s": 100,
    "x": 11,
    "t": 1675190399000,
    "c": [14, 41]
  },
  "lastQuote": {
    "P": 125.08,
    "p": 125.06,
    "S": 1000,
    "s": 500,
    "t": 1675190399500
  },
  "min": {
    "o": 125.05,
    "h": 125.10,
    "l": 125.00,
    "c": 125.07,
    "v": 50000,
    "vw": 125.06,
    "av": 111000000,
    "t": 1675190340000,
    "n": 200
  },
  "todaysChange": -4.54,
  "todaysChangePerc": -3.50,
  "updated": 1675190399000
}
```

**Key fields we extract**:
- `lastTrade.p` → current price (via Python client: `snap.last_trade.price`)
- `prevDay.c` → previous close (via Python client: `snap.prev_day.close`)
- `todaysChangePerc` → day change percentage (via Python client: `snap.todays_change_perc`)
- `lastTrade.t` → timestamp in Unix milliseconds (divide by 1000 for seconds)

**Python client field mapping** — the client maps the compact JSON keys to readable attributes:
| JSON | Python attribute |
|------|-----------------|
| `lastTrade.p` | `snap.last_trade.price` |
| `lastTrade.s` | `snap.last_trade.size` |
| `lastTrade.t` | `snap.last_trade.timestamp` |
| `lastQuote.P` | `snap.last_quote.ask_price` |
| `lastQuote.p` | `snap.last_quote.bid_price` |
| `lastQuote.S` | `snap.last_quote.ask_size` |
| `lastQuote.s` | `snap.last_quote.bid_size` |
| `day.c` | `snap.day.close` |
| `day.o` | `snap.day.open` |
| `day.h` | `snap.day.high` |
| `day.l` | `snap.day.low` |
| `day.v` | `snap.day.volume` |
| `prevDay.c` | `snap.prev_day.close` |

### 2. Single Ticker Snapshot

For getting detailed data on one ticker (e.g., when user clicks a ticker for the detail view).

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers/{stocksTicker}`

**Python client**:
```python
snapshot = client.get_snapshot_ticker(
    market_type="stocks",
    ticker="AAPL",
)

print(f"Price: ${snapshot.last_trade.price}")
print(f"Bid/Ask: ${snapshot.last_quote.bid_price} / ${snapshot.last_quote.ask_price}")
print(f"Day range: ${snapshot.day.low} - ${snapshot.day.high}")
print(f"Change: {snapshot.todays_change_perc}%")
```

**Response** wraps a single `ticker` object (same schema as above).

### 3. Previous Close (Aggregates)

Gets the previous day's OHLCV bar for a ticker. Useful for seed prices or reference.

**REST**: `GET /v2/aggs/ticker/{ticker}/prev`

**Python client**:
```python
prev = client.get_previous_close_agg(ticker="AAPL")

for agg in prev:
    print(f"Previous close: ${agg.close}")
    print(f"OHLC: O={agg.open} H={agg.high} L={agg.low} C={agg.close}")
    print(f"Volume: {agg.volume}")
```

### 4. Aggregates (Historical Bars)

Historical OHLCV bars over a date range. Not needed for live polling but useful if we add historical charts.

**REST**: `GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`

**Python client**:
```python
aggs = []
for a in client.list_aggs(
    "AAPL",
    1,
    "minute",
    "2024-01-01",
    "2024-01-31",
    limit=50000,
):
    aggs.append(a)

for a in aggs:
    print(f"Time: {a.timestamp}, O={a.open} H={a.high} L={a.low} C={a.close} V={a.volume}")
```

### 5. Last Trade / Last Quote

Individual endpoints for the most recent trade or NBBO quote.

```python
# Last trade
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} x {trade.size}")

# Last NBBO quote
quote = client.get_last_quote(ticker="AAPL")
print(f"Bid: ${quote.bid} x {quote.bid_size}")
print(f"Ask: ${quote.ask} x {quote.ask_size}")
```

### 6. Unified Snapshot (v3 — Alternative)

A newer endpoint that supports multiple asset types in a single call.

**REST**: `GET /v3/snapshot?ticker.any_of=AAPL,GOOGL,MSFT`

**Key differences from v2**:
- Supports stocks, options, forex, crypto in one call (use prefixes: `O:` options, `C:` forex, `X:` crypto, `I:` indices)
- Max 250 tickers per request via `ticker.any_of`
- Paginated results via `next_url`
- Returns `session` object instead of separate `day`/`prevDay`

We use the **v2 snapshot** in FinAlly because it's simpler for stocks-only use and the Python client has a clean wrapper.

## How FinAlly Uses the API

The `MassiveDataSource` runs as a background asyncio task:

1. Collects all tickers from the active watchlist
2. Calls `get_snapshot_all()` with those tickers — **one API call per poll**
3. Extracts `last_trade.price` and `last_trade.timestamp` from each snapshot
4. Writes to the shared in-memory `PriceCache`
5. Sleeps for the poll interval, then repeats

```python
import asyncio
from massive import RESTClient

async def poll_massive(client: RESTClient, tickers: list[str], price_cache, interval: float = 15.0):
    """Background polling loop for the Massive API."""
    while True:
        if tickers:
            # Synchronous client — run in thread to avoid blocking the event loop
            snapshots = await asyncio.to_thread(
                client.get_snapshot_all,
                market_type="stocks",
                tickers=tickers,
            )
            for snap in snapshots:
                price_cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.timestamp / 1000,  # ms → seconds
                )
        await asyncio.sleep(interval)
```

## Error Handling

The client raises exceptions for HTTP errors:

| Status | Meaning | FinAlly handling |
|--------|---------|-----------------|
| 401 | Invalid API key | Log error, keep retrying (key may be fixed) |
| 403 | Insufficient plan permissions | Log error, keep retrying |
| 429 | Rate limit exceeded | Log warning, back off (the sleep interval handles this) |
| 5xx | Server error | Client has built-in retry (3 retries by default) |

All errors are caught in the poll loop and logged — a failed poll does not crash the application. The next poll cycle retries automatically.

## Important Notes

- The snapshot endpoint returns data for **all requested tickers in one call** — critical for staying within free-tier rate limits
- All API timestamps are **Unix milliseconds** — divide by 1000 for Python `time.time()` compatibility
- During market closed hours, `last_trade.price` reflects the last traded price (may include after-hours)
- Snapshot data **resets daily at 3:30 AM EST** and repopulates starting ~4:00 AM EST
- The `day` object resets at market open; during pre-market, values may be from the previous session
- The Python client is **synchronous** — in an async application, wrap calls with `asyncio.to_thread()`
