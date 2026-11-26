# Market Data Backend — Code Review (Round 2)

**Date:** 2026-11-26
**Scope:** `backend/app/market/` (9 modules, 349 statements), `backend/tests/market/` (6 modules, 73 tests), `backend/pyproject.toml`
**Reviewed against:** `planning/PLAN.md` §6/§10, `planning/MARKET_DATA_DESIGN.md` (current contract, incl. §15 change list), `planning/MARKET_DATA_SUMMARY.md`, `planning/archive/*`
**Previous review:** `planning/archive/MARKET_DATA_REVIEW.md` (2026-02-10)

---

## 1. Verdict

The simulator path is **production-ready for the next build phase**: the GBM maths is correct, the cache is sound, and the SSE endpoint streams correctly end-to-end under uvicorn.

The **Massive (real data) path is broken**. Every real snapshot is rejected, so the cache never fills in Massive mode. The unit tests don't catch this because their mocks invent an attribute the real model doesn't have.

None of the 9 items in `MARKET_DATA_DESIGN.md` §15 have been implemented yet. `MARKET_DATA_SUMMARY.md` says the subsystem is "complete … all issues resolved". That's only true of the *first* review's issues, and the summary should be corrected (see §6).

| Area | Status |
|---|---|
| Tests | ✅ 73/73 pass (4.2 s), no warnings |
| Lint (`ruff check`) | ✅ clean |
| Format (`ruff format --check`) | ⚠️ 3 test files would be reformatted |
| Coverage | 91% overall. `stream.py` 33%, everything else ≥ 94% |
| Simulator mode, end-to-end | ✅ verified over HTTP: `retry:` then a `data:` frame every ~500 ms |
| Massive mode, end-to-end | ❌ **Critical bug**: no prices ever reach the cache |
| Design-doc §15 change list | ❌ 0 of 9 items done |

---

## 2. How this review was carried out

1. Read all planning docs: PLAN, design, summary, and the four archive docs.
2. Read every source and test file in full.
3. `uv sync --extra dev`, then `pytest --cov=app --cov-report=term-missing`, `ruff check`, and `ruff format --check`.
4. Inspected the **installed** `massive` 2.2.0 models (`dataclasses.fields(LastTrade)`, `TickerSnapshot.from_dict`).
5. Wrote a probe script that reproduces each suspected bug against the real code (results quoted in §3).
6. Started a throwaway FastAPI app with uvicorn, the simulator source and `create_stream_router`, then read the SSE stream with `curl -N`.

---

## 3. Findings (all reproduced unless marked *inspection*)

### 3.1 🔴 Critical: Massive client reads a non-existent `last_trade.timestamp`

`massive_client.py:101-103`

```python
price = snap.last_trade.price
timestamp = snap.last_trade.timestamp / 1000.0   # ← no such field
```

The real `massive.rest.models.LastTrade` (v2.2.0) has these fields: `ticker, trf_timestamp, sequence_number, sip_timestamp, participant_timestamp, conditions, correction, id, price, trf_id, size, exchange, tape`. There is no `timestamp`, and `sip_timestamp` is in **nanoseconds**, not milliseconds.

Reproduced with a real `TickerSnapshot.from_dict(...)`:

```
LOG WARNING Skipping snapshot for AAPL: 'LastTrade' object has no attribute 'timestamp'
cache AAPL = None
```

The `AttributeError` is caught by the per-snapshot `except` and logged as a warning. The result is that **every ticker is skipped on every poll**, so a user with a `MASSIVE_API_KEY` sees an empty watchlist and can't trade.

**Why the tests pass:** `test_massive.py::_make_snapshot` builds a bare `MagicMock()` and sets `last_trade.timestamp` by hand. A `MagicMock` accepts any attribute, so the tests check the code against itself rather than against the library.

**Fix:** follow design §9.2/§9.3 (`_extract()`):
- use `sip_timestamp / 1e9`, falling back to `updated` or `time.time()`;
- fall back to `day.close` and then `prev_day.close` for the price when `last_trade` is missing (pre-market);
- return `prev_day.close` as the reference price.

Rewrite the tests to use `TickerSnapshot.from_dict(...)` (design §14.4), or at minimum `MagicMock(spec=LastTrade)`.

### 3.2 🟠 High: no "daily change %" anywhere in the data model

PLAN §10 requires the watchlist to show *daily change %*. `PriceUpdate` only has tick-to-tick `change` / `change_percent`, which are almost always about ±0.01% in the simulator, so they mean nothing to a user as a "daily" figure. The frontend has no way to compute a day baseline either, because sparklines only start at page load.

**Fix:** design §4/§5: add `reference_price` plus `day_change` / `day_change_percent`. Massive sets the reference to `prev_day.close`; the simulator uses the session's first price.

### 3.3 🟠 High: SSE never tells the client that the last ticker was removed (`stream.py:80`)

```python
if prices:          # empty map → nothing is sent
```

`cache.remove()` also doesn't bump `version` (§3.6). Together these mean that when the watchlist goes from 1 ticker to 0, no frame is ever sent and the browser keeps showing the removed ticker.

Reproduced: after removing the last ticker, the only events seen were `retry:` and the original `data:` frame.

For non-empty watchlists the simulator hides this, because the next tick bumps the version. In Massive mode, a removed ticker stays on screen for up to a full poll interval (15 s).

**Fix:** design §11.2: send `{}` when the map is empty and bump `version` in `remove()`.

### 3.4 🟠 High: `create_stream_router` mutates a module-level router (`stream.py:17,26`)

Each call adds another `/prices` route to the **same** global `APIRouter`. Reproduced: two calls return the same object with `['/api/stream/prices', '/api/stream/prices']`, and the first registration (bound to the *first* cache) wins.

Any test that builds more than one app, or a future app factory, will quietly stream from a stale cache. The previous review flagged this as "Low". I've raised it because it **will** bite the API-route tests that get written next.

**Fix:** create the `APIRouter` inside the factory.

### 3.5 🟠 Medium: Massive writes a ghost entry for a ticker removed during a poll

The HTTP call runs in a worker thread (`asyncio.to_thread`). If `remove_ticker("GOOGL")` runs while that call is in flight, the results still include GOOGL and get written back into the cache. After that:
- the ticker is no longer tracked, so it is never refreshed;
- it is never removed again, so it stays in the cache.

Reproduced: `tracked: ['AAPL']  cache has GOOGL: True`. The stale price then streams to the client forever and is used for valuation.

On the same theme, `_fetch_snapshots` passes `self._tickers` itself (not a copy) to the worker thread, while `add_ticker` appends to it from the event loop. *(Inspection.)*

**Fix:** snapshot the list before the thread call, and drop results for tickers no longer in `self._tickers` (design §9.3).

### 3.6 🟡 Medium: `PriceCache` inconsistencies (`cache.py`)

| Line | Issue | Reproduced |
|---|---|---|
| 30 | `ts = timestamp or time.time()` replaces an explicit `0.0` with *now* | `ts: 1795651377.2` for input `0.0` |
| 59-62 | `remove()` doesn't bump `version`, so SSE change detection misses removals (see §3.3) | version `1 → 1` |
| 64-67 | `version` is read without the lock. Harmless under the GIL but inconsistent, and the venv here runs **Python 3.13**, where free-threading builds exist | inspection |
| 36 | Prices are rounded to 2 dp, so sub-$1 tickers lose most of their precision | `0.0123 → 0.01` |

The last item is low priority (default tickers are all > $100) but is worth noting before users add penny stocks through the chat.

### 3.7 🟡 Medium: ticker handling differs between the two sources (contract violation)

| Behaviour | Simulator | Massive |
|---|---|---|
| `add_ticker("aapl")` | creates a **second** ticker `aapl` at a random price | normalizes to `AAPL` |
| `add_ticker` before `start()` | silently dropped | remembered |
| Validation of junk input (`"DROP TABLE"`) | none | none |

Reproduced on the simulator: `['AAPL', 'aapl']` with `{'AAPL': 190.0, 'aapl': 62.76}`.

PLAN §12 asks for tests that "both implementations conform to the abstract interface", and no such test exists. **Fix:** a shared `normalize_ticker()` in `interface.py` (design §6.1), the simulator honouring pre-start adds (§8.6), and `tests/market/test_conformance.py` parametrized over both sources (§14.1).

### 3.8 🟡 Medium: Massive start-up and add latency *(inspection)*

- `start()` **awaits** the first poll inline (`massive_client.py:46`). If FastAPI's lifespan awaits `start()`, a slow or failing API delays server start-up. The `massive` client retries 5xx responses, so the delay can be long. Consider running the first poll as a task, or adding a short timeout.
- `add_ticker` waits up to a full interval (15 s) before the new ticker gets a price. During that window a buy order for it will have no price to fill at. Design §9.3 proposes an early, rate-limited poll.
- There is no backoff on 429/401. A bad key logs an error every 15 s forever. The poll interval is hard-coded, with no `MASSIVE_POLL_INTERVAL` env var.

### 3.9 🟢 Low: simulator restarts lose P&L continuity *(inspection, integration concern)*

On every process start the simulator resets known tickers to `SEED_PRICES`, and unknown tickers to `random.uniform(50, 300)`. Positions in the database keep their `avg_cost`. After a container restart:
- a position in `PYPL` bought at a random $112 might be re-seeded at $287, giving a fake +156% P&L;
- any drift on the default tickers disappears.

Design §7's deterministic `seed_price_for()` fixes only the "random" half of this. A better approach is for the app lifespan to seed the simulator from the last known price, either the position's `avg_cost` or the latest trade price, when one exists. This belongs in the integration phase (§12), but the simulator should accept initial prices to make it possible, for example `start(tickers, initial_prices=None)`.

### 3.10 🟢 Low: minor code points

- `stream.py:86-87` catches `asyncio.CancelledError` and doesn't re-raise. Starlette tolerates this, but swallowing cancellation is an anti-pattern; log it and re-raise.
- `stream.py:43` sets a `Connection: keep-alive` header. This is redundant on HTTP/1.1 and forbidden on HTTP/2 if the app is ever put behind an HTTP/2 proxy. Harmless today.
- There is no SSE heartbeat. In Massive mode, or with an empty watchlist, the stream can be idle for long stretches, and App Runner/Render proxies close idle connections (design §11.1 proposes a 15 s `: ping`).
- `GBMSimulator` uses the global `np.random` / `random` state. Tests can't seed a single instance, and nothing is reproducible per simulator. Passing an `np.random.Generator` in would fix both.
- `dt` is fixed at 0.5 s/year-fraction even if `SimulatorDataSource(update_interval=...)` is changed. It should be derived as `update_interval / TRADING_SECONDS_PER_YEAR`.
- `SimulatorDataSource.start()` called twice leaks the first task. The interface documents this as undefined, but a guard costs one line.
- `factory.py` imports `massive_client` (and therefore `massive`) eagerly even in simulator mode. That's fine while `massive` is a core dependency, but it adds start-up import cost for 99% of users.

---

## 4. Test suite assessment

**Strengths:** fast (4 s), deterministic, well named. Cache and model tests are thorough, and simulator tests cover the Cholesky build, add/remove, and positivity.

**Gaps (ranked):**

1. **Mocks are not grounded in the real library.** This is the root cause of §3.1. Use real `TickerSnapshot.from_dict` objects or `spec=` mocks.
2. **`stream.py` is at 33%.** `_generate_events` can be tested without a server using a fake `Request` (design §14.5). The probe used for this review is effectively that test.
3. **No conformance suite** across both sources (PLAN §12 explicitly asks for one).
4. **No statistical GBM test.** Nothing checks that realised volatility and correlation match the parameters (design §14.2). Current tests only check that prices stay positive and change.
5. **No concurrency test** for the cache, and no test for remove-during-poll (§3.5).
6. Housekeeping:
   - `ruff format` would reformat 3 test files.
   - `@pytest.mark.asyncio` on `TestMassiveDataSource` is redundant with `asyncio_mode = "auto"`.
   - The `event_loop_policy` fixture in `conftest.py` returns `asyncio.DefaultEventLoopPolicy`, which is deprecated in Python 3.14.

---

## 5. What is good (keep it)

- **Architecture.** The strategy pattern with a single cache, and producers-push/consumers-pull, are the right shape. No downstream code needs to know which source is running.
- **GBM implementation is mathematically correct.** It uses the Itô-corrected drift `(μ − σ²/2)dt` and Cholesky-correlated shocks. The correlation matrix is positive-definite for any mix of known and unknown tickers (verified: Cholesky succeeds for 40 unknown tickers plus all 10 defaults). Per-tick moves on AAPL are about ±1¢, which looks realistic.
- **The `PriceUpdate` frozen/slots dataclass with derived properties** keeps the wire format in one place (`to_dict`).
- **Version-based SSE change detection** avoids redundant frames. The `retry: 1000` and `X-Accel-Buffering: no` headers are correct. Verified working end-to-end over HTTP.
- **The background loops never die.** Both sources catch and log exceptions and carry on.
- **Clean lifecycle.** `stop()` is idempotent and awaits the cancelled task.
- All issues from the first review were correctly fixed (`pyproject.toml` wheel config, eager imports, return type, `get_tickers()`, constants, test imports).

---

## 6. Documentation discrepancies

| Doc | Says | Actually |
|---|---|---|
| `MARKET_DATA_SUMMARY.md` | "Complete, tested, reviewed, all issues resolved" | Round-1 issues are resolved; the 9 design §15 gaps, including a critical bug, are open |
| `MARKET_DATA_SUMMARY.md` | `massive_client.py` coverage 56%, overall 84% | 94% and 91% today |
| `MARKET_DATA_SUMMARY.md` | "8 modules" | 9 `.py` files (8 + `__init__`). Trivial |
| `MARKET_DATA_DESIGN.md` §15 | Lists the gaps accurately | ✅ Confirmed. Every §15 item matches what I found independently |

---

## 7. Recommended action plan

Do these **before** building `app/main.py` and the routes, because the routes depend on the fixed contract:

| # | Priority | Change | Design ref |
|---|---|---|---|
| 1 | **P0** | Fix Massive field mapping (`sip_timestamp` ns, price fallbacks). Rewrite Massive tests on real models | §9.2, §9.3, §14.4 |
| 2 | **P0** | Router created inside `create_stream_router`; send `{}` on an empty map; `remove()` bumps version | §11.2, §5 |
| 3 | P1 | `reference_price` / `day_change_percent` on `PriceUpdate` | §4, §5 |
| 4 | P1 | `normalize_ticker()` shared by both sources and the watchlist API; simulator honours pre-start adds; conformance tests | §6.1, §8.6, §14.1 |
| 5 | P1 | Massive: copy the ticker list for the thread; drop results for removed tickers; backoff; `MASSIVE_POLL_INTERVAL`; early poll on add | §9.3, §10 |
| 6 | P2 | `timestamp=0.0` respected; `version` read under the lock; SSE `: ping` heartbeat; re-raise `CancelledError` | §5, §11 |
| 7 | P2 | SSE generator tests and GBM statistics tests; `ruff format` | §14.2, §14.5 |
| 8 | P3 | Simulator accepts initial prices so restarts keep P&L continuity; injectable RNG; `dt` derived from interval | new |
| 9 | — | Update `MARKET_DATA_SUMMARY.md` once 1–7 land | — |

Items 1–7 are exactly the design doc's §15 list plus three small additions (re-raise `CancelledError`, the snapshot-copy race, sub-dollar rounding). The design document already has target code for nearly all of them.
