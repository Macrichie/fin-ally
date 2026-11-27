# Market Simulator Design

Approach and code structure for simulating realistic stock prices when no Massive API key is configured. This is the default mode — most users will run the simulator.

## Overview

The simulator uses **Geometric Brownian Motion (GBM)** to generate realistic stock price paths. GBM is the standard model underlying Black-Scholes option pricing — prices evolve multiplicatively with random noise, can never go negative, and exhibit the lognormal distribution observed in real markets.

Updates run at **500ms intervals**, producing a continuous stream of small price changes that feel alive in the UI.

## GBM Math

At each time step, a stock price evolves as:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

Where:
- **S(t)** = current price
- **mu** = annualized drift (expected return), e.g. 0.05 (5% per year)
- **sigma** = annualized volatility, e.g. 0.22 (22% per year)
- **dt** = time step as a fraction of a trading year
- **Z** = standard normal random variable (drawn from N(0,1))

### Time Step Calculation

We update every 500ms. A trading year has ~252 days * 6.5 hours/day = 5,896,800 trading seconds:

```
dt = 0.5 / (252 * 6.5 * 3600) ≈ 8.48e-8
```

This tiny `dt` produces **sub-cent moves per tick** that accumulate naturally over time:
- With `sigma=0.22` (AAPL), expect ~$0.01 moves per tick
- With `sigma=0.50` (TSLA), expect ~$0.03 moves per tick
- Over a simulated trading day (~47,000 ticks), the range matches real intraday behavior

### Why GBM Works Here

- **Multiplicative** — `exp()` is always positive, so prices can never go negative
- **Lognormal** — matches the statistical distribution of real stock returns
- **Configurable** — per-ticker `mu` and `sigma` create distinct personalities
- **Cheap** — one `exp()` and one normal draw per ticker per tick

## Correlated Moves

Real stocks don't move independently — tech stocks tend to move together, financials track each other. We use **Cholesky decomposition** of a correlation matrix to generate correlated random draws.

### How It Works

1. Define a correlation matrix `C` based on sector groupings
2. Compute `L = cholesky(C)` — the lower-triangular Cholesky factor
3. Each step, draw independent standard normals `Z_independent`
4. Transform: `Z_correlated = L @ Z_independent`
5. Use `Z_correlated[i]` as the random input for ticker `i`

### Correlation Structure

| Pair | Correlation | Rationale |
|------|------------|-----------|
| Tech ↔ Tech | 0.6 | AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX move together |
| Finance ↔ Finance | 0.5 | JPM, V move together |
| Cross-sector | 0.3 | Tech ↔ Finance baseline |
| TSLA ↔ anything | 0.3 | TSLA is in the tech set but behaves independently |
| Unknown ↔ anything | 0.3 | Dynamically added tickers get the default |

The correlation matrix is **rebuilt** whenever tickers are added or removed. This is O(n^2) for the matrix construction and O(n^3) for Cholesky, but n < 50 tickers so it's negligible.

### Implementation

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
            corr[i, j] = rho
            corr[j, i] = rho

    self._cholesky = np.linalg.cholesky(corr)
```

The correlation matrix must be **positive semi-definite** for Cholesky to succeed. Our structure guarantees this because all pairwise correlations are between 0.3 and 0.6, and the diagonal is 1.0.

## Random Events (Shocks)

Every step, each ticker has a small probability (~0.1%) of a **random event** — a sudden 2-5% move in either direction. This adds visual drama and makes the dashboard interesting to watch.

```python
if random.random() < event_probability:  # 0.001 default
    shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
    price *= (1 + shock)
```

**Expected frequency** with 10 tickers at 2 ticks/second:
- Per ticker: ~1 event per 500 seconds (~8.3 minutes)
- Across all tickers: ~1 event per 50 seconds
- Enough to be noticed, not enough to be distracting

## Seed Prices & Per-Ticker Parameters

**File: `backend/app/market/seed_prices.py`**

### Starting Prices

Realistic prices for the default watchlist (approximate as of project creation — they will drift from reality, which is fine for a demo):

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}
```

Tickers added dynamically (not in this list) start at a **random price between $50-$300**.

### Per-Ticker GBM Parameters

Each ticker has its own volatility (`sigma`) and drift (`mu`) to reflect real-world behavior:

```python
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},   # Moderate volatility
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},   # Lowest tech volatility
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # Highest volatility, low drift
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # High vol, strong upward drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # Low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # Lowest volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}  # For unknown tickers
```

### Correlation Groups

```python
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR = 0.3
TSLA_CORR = 0.3
```

## GBMSimulator Class

**File: `backend/app/market/simulator.py`**

The core simulation engine. Stateful — holds current prices, parameters, and the precomputed Cholesky matrix.

```python
class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT, event_probability: float = 0.001):
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance one time step. Returns {ticker: new_price}.
        This is the hot path — called every 500ms.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            # GBM step
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= (1 + shock)

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        """Add ticker, rebuild Cholesky."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove ticker, rebuild Cholesky."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)
```

### Internal Methods

- **`_add_ticker_internal(ticker)`** — adds ticker without rebuilding Cholesky (used in batch initialization)
- **`_rebuild_cholesky()`** — reconstructs the correlation matrix and its Cholesky factor
- **`_pairwise_correlation(t1, t2)`** — static method returning the correlation coefficient for a pair

## SimulatorDataSource

The `MarketDataSource` implementation that wraps `GBMSimulator` in an async loop. See `MARKET_INTERFACE.md` for the full implementation.

Key responsibilities:
1. Create the `GBMSimulator` on `start()`
2. Seed the `PriceCache` with initial prices before the loop starts
3. Call `step()` every 500ms, writing results to the cache
4. Delegate `add_ticker()` / `remove_ticker()` to both the simulator and the cache

## File Structure

```
backend/app/market/
├── seed_prices.py     # Constants only: SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS,
│                      #   CORRELATION_GROUPS, INTRA_TECH_CORR, INTRA_FINANCE_CORR,
│                      #   CROSS_GROUP_CORR, TSLA_CORR
└── simulator.py       # GBMSimulator class + SimulatorDataSource class
```

`seed_prices.py` is pure data — no logic. `simulator.py` contains all the simulation logic.

## Behavior Notes

- **Prices never go negative** — GBM is multiplicative (`exp()` is always positive)
- **Sub-cent moves per tick** accumulate naturally over a session. With `sigma=0.50` (TSLA) and a full simulated day, the intraday range is roughly realistic.
- **The drift term is negligible** at this `dt` — over 500ms, `mu * dt ≈ 4e-9`. Drift only matters when accumulating over many thousands of steps. This is correct GBM behavior.
- **Correlation is structural, not exact** — the Cholesky approach means tech stocks *tend* to move together, but individual steps can diverge. This matches real markets.
- **Random events bypass correlation** — shocks are applied independently after the correlated GBM step. A shock on AAPL doesn't drag GOOGL along. This is intentional — events represent company-specific news.
- **Adding tickers is O(n^2)** due to Cholesky rebuild, but n < 50 so it's instant.
- **No calendar awareness** — the simulator runs continuously. It doesn't know about market hours, weekends, or holidays. This is fine for a demo that runs in a session.
- **Deterministic seeding possible** — for tests, `np.random.seed()` and `random.seed()` can be set before creating the simulator to get reproducible paths.

## Test Coverage

The simulator has 27 tests across two modules:

| Module | Tests | What's tested |
|--------|-------|---------------|
| `test_simulator.py` | 17 | GBMSimulator: step produces valid prices, add/remove tickers, Cholesky rebuild, random events, correlation structure, edge cases |
| `test_simulator_source.py` | 10 | SimulatorDataSource: start/stop lifecycle, cache population, add/remove passthrough, background task behavior |

All tests pass. Coverage for `simulator.py` is 98%.
