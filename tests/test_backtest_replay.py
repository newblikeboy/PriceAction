from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from app.backtest import BacktestRunner
from app.config import StrategyConfig
from app.domain import LevelSet, SignalCandidate


class FakeLevels:
    def calculate(self, candles_5m: pd.DataFrame, trading_date: date) -> LevelSet:
        return LevelSet(trading_date=trading_date, pdh=120, pdl=80, pdc=100, orh=105, orl=95)


class FakeSignals:
    def __init__(self) -> None:
        self.option_snapshot: dict[str, Any] | None = None
        self.visible_max_times: list[pd.Timestamp] = []

    def generate_for_candle(
        self,
        candles_5m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
        candle_time,
    ):
        current = pd.to_datetime(candle_time)
        assert candles_5m.index.max() == current
        self.visible_max_times.append(candles_5m.index.max())
        if current.strftime("%H:%M") != "09:35":
            return [], []
        signal = SignalCandidate(
            date=str(trading_date),
            time="09:40",
            symbol="NIFTY",
            direction="CE",
            setup_type="TEST_REPLAY_SIGNAL",
            entry_index_price=100.0,
            sl_index_price=95.0,
            target_index_price=110.0,
            risk_points=5.0,
            reward_points=10.0,
            risk_reward=2.0,
            setup_score=90,
            features={"date": str(trading_date), "time": "09:35"},
        )
        return [signal], []


def test_backtest_runner_replays_only_visible_candles() -> None:
    cfg = StrategyConfig(opening_range_end="09:30", no_fresh_trade_after="15:00")
    runner = BacktestRunner(cfg)
    fake_signals = FakeSignals()
    runner.levels = FakeLevels()
    runner.signals = fake_signals
    candles = _candles()

    result = runner.run(candles)

    assert [ts.strftime("%H:%M") for ts in fake_signals.visible_max_times] == ["09:30", "09:35"]
    assert len(result.trades) == 1
    assert result.trades[0].entry_time == "09:40"
    assert result.trades[0].exit_reason == "TARGET_HIT"


def _candles() -> pd.DataFrame:
    rows = [
        {"datetime": "2024-01-01 09:15", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 0},
        {"datetime": "2024-01-01 09:30", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 0},
        {"datetime": "2024-01-01 09:35", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 0},
        {"datetime": "2024-01-01 09:40", "open": 100, "high": 111, "low": 99, "close": 110, "volume": 0},
    ]
    frame = pd.DataFrame(rows)
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame = frame.set_index("datetime")
    frame["date"] = frame.index.date
    frame["time"] = frame.index.strftime("%H:%M")
    return frame
