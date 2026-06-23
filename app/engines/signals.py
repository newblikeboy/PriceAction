from __future__ import annotations

from datetime import date

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import LevelSet, SignalCandidate, SkippedSignal
from app.engines.smart_trades import SmartTradeEngine


class SignalEngine:
    """Smart-zone-only signal engine.

    Legacy exact-price setups were intentionally removed from this layer so the
    trade pipeline can only emit smart-zone setups.
    """

    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg
        self.smart_trades = SmartTradeEngine(cfg)
        self.option_snapshot: dict | None = None

    def generate_for_day(
        self,
        candles_5m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        self.smart_trades.option_snapshot = self.option_snapshot
        return self.smart_trades.generate_for_day(candles_5m, levels, trading_date)

    def generate_for_candle(
        self,
        candles_5m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
        candle_time,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        self.smart_trades.option_snapshot = self.option_snapshot
        return self.smart_trades.generate_for_candle(candles_5m, levels, trading_date, candle_time)
