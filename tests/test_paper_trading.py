from __future__ import annotations

import pandas as pd

from app.config import StrategyConfig
from app.domain import SignalCandidate
from app.paper_trading import PaperTradeEngine


def test_paper_trade_locks_profit_after_one_r() -> None:
    cfg = StrategyConfig(
        paper_profit_lock_after_r=1.0,
        paper_profit_lock_r=0.5,
        paper_breakeven_after_r=1.0,
        paper_near_target_exit_pct=0.0,
    )
    engine = PaperTradeEngine(cfg)
    trade = engine.create_trade(
        SignalCandidate(
            date="2024-01-01",
            time="09:30",
            symbol="NIFTY",
            direction="CE",
            setup_type="TEST",
            entry_index_price=100.0,
            sl_index_price=90.0,
            target_index_price=130.0,
            risk_points=10.0,
            reward_points=30.0,
            risk_reward=3.0,
            setup_score=80,
            features={},
        )
    )
    candles = pd.DataFrame(
        [
            {"datetime": pd.Timestamp("2024-01-01 09:30"), "open": 100.0, "high": 111.0, "low": 99.0, "close": 110.0},
            {"datetime": pd.Timestamp("2024-01-01 09:35"), "open": 110.0, "high": 110.5, "low": 104.0, "close": 105.0},
        ]
    ).set_index("datetime")
    candles["date"] = candles.index.date
    candles["time"] = candles.index.strftime("%H:%M")

    result = engine.simulate_trade(trade, candles)

    assert result.exit_reason == "PROFIT_LOCK_HIT"
    assert result.exit_index_price == 105.0
    assert result.underlying_points == 5.0
    assert result.r_multiple == 0.5
