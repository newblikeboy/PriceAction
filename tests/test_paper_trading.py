from __future__ import annotations

from datetime import datetime

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


def test_live_quote_stream_locks_profit_like_replay() -> None:
    # The live quote path must trail the stop with the same engine as replay/backtest:
    # once price reaches 1R the stop locks +0.5R, and a pullback closes PROFIT_LOCK_HIT.
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

    trade = engine.update_open_trade_with_quote(trade, 111.0, datetime(2024, 1, 1, 9, 35))
    assert trade.status == "OPEN"
    assert trade.features.get("profit_lock_active") is True
    assert trade.features.get("active_sl_index_price") == 105.0

    trade = engine.update_open_trade_with_quote(trade, 104.0, datetime(2024, 1, 1, 9, 40))
    assert trade.status == "CLOSED"
    assert trade.exit_reason == "PROFIT_LOCK_HIT"
    assert trade.exit_index_price == 105.0
    assert trade.r_multiple == 0.5
