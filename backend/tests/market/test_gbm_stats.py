"""Statistical tests: realised GBM volatility and correlation match the parameters."""

import numpy as np
import pytest

from app.market.seed_prices import TICKER_PARAMS
from app.market.simulator import GBMSimulator

N_STEPS = 20_000


def log_returns(sim: GBMSimulator, steps: int = N_STEPS) -> dict[str, np.ndarray]:
    series = {t: [sim.get_price(t)] for t in sim.get_tickers()}
    for _ in range(steps):
        sim.step()
        for t in series:
            series[t].append(sim.get_price(t))  # unrounded internal price
    return {t: np.diff(np.log(v)) for t, v in series.items()}


@pytest.mark.parametrize("ticker", ["AAPL", "TSLA", "V"])
def test_log_returns_have_expected_volatility(ticker):
    sim = GBMSimulator([ticker], event_probability=0.0, rng=np.random.default_rng(0))
    r = log_returns(sim)[ticker]
    expected = TICKER_PARAMS[ticker]["sigma"] * np.sqrt(GBMSimulator.DEFAULT_DT)
    assert abs(r.std() / expected - 1) < 0.05


def test_realised_correlation_matches_sector_rules():
    sim = GBMSimulator(
        ["AAPL", "MSFT", "JPM", "V", "TSLA"], event_probability=0.0, rng=np.random.default_rng(1)
    )
    r = log_returns(sim)

    def corr(a, b):
        return np.corrcoef(r[a], r[b])[0, 1]

    assert corr("AAPL", "MSFT") == pytest.approx(0.6, abs=0.05)
    assert corr("JPM", "V") == pytest.approx(0.5, abs=0.05)
    assert corr("AAPL", "JPM") == pytest.approx(0.3, abs=0.05)
    assert corr("TSLA", "AAPL") == pytest.approx(0.3, abs=0.05)


def test_cholesky_ok_for_many_unknown_tickers():
    sim = GBMSimulator([f"T{i}" for i in range(40)] + ["AAPL", "TSLA", "JPM", "V"])
    assert len(sim.step()) == 44
    eig = np.linalg.eigvalsh(sim._cholesky @ sim._cholesky.T)
    assert eig.min() > 0


def test_same_seed_is_reproducible():
    a = GBMSimulator(["AAPL", "MSFT"], rng=np.random.default_rng(42))
    b = GBMSimulator(["AAPL", "MSFT"], rng=np.random.default_rng(42))
    for _ in range(100):
        assert a.step() == b.step()


def test_dt_scales_volatility():
    """A 4x larger dt gives ~2x larger per-step moves."""
    base = GBMSimulator(["AAPL"], event_probability=0.0, rng=np.random.default_rng(3))
    big = GBMSimulator(
        ["AAPL"],
        dt=4 * GBMSimulator.DEFAULT_DT,
        event_probability=0.0,
        rng=np.random.default_rng(3),
    )
    ratio = log_returns(big, 5_000)["AAPL"].std() / log_returns(base, 5_000)["AAPL"].std()
    assert ratio == pytest.approx(2.0, rel=0.02)


def test_events_produce_jumps():
    sim = GBMSimulator(["AAPL"], event_probability=1.0, rng=np.random.default_rng(5))
    r = np.abs(log_returns(sim, 200)["AAPL"])
    assert (r > np.log(1.019)).all()  # every step carries a >= 2% shock
    assert (r < np.log(1 / 0.94)).all()
