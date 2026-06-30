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
        signals, skipped = self.smart_trades.generate_for_day(candles_5m, levels, trading_date)
        return self._apply_entry_time_filters(signals, skipped)

    def generate_for_candle(
        self,
        candles_5m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
        candle_time,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        self.smart_trades.option_snapshot = self.option_snapshot
        signals, skipped = self.smart_trades.generate_for_candle(candles_5m, levels, trading_date, candle_time)
        return self._apply_entry_time_filters(signals, skipped)

    def generate_for_candle_rows(
        self,
        all_rows: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
        candle_time,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        self.smart_trades.option_snapshot = self.option_snapshot
        signals, skipped = self.smart_trades.generate_for_candle_rows(all_rows, levels, trading_date, candle_time)
        return self._apply_entry_time_filters(signals, skipped)

    def _apply_entry_time_filters(
        self,
        signals: list[SignalCandidate],
        skipped: list[SkippedSignal],
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        allowed: list[SignalCandidate] = []
        for signal in signals:
            entry_time = str(signal.time)
            if entry_time >= self.cfg.no_fresh_trade_after:
                skipped.append(
                    SkippedSignal(
                        signal.date,
                        entry_time,
                        signal.direction,
                        signal.setup_type,
                        "Fresh entry blocked from session cutoff",
                        {"entry_time": entry_time, "cutoff": self.cfg.no_fresh_trade_after},
                    )
                )
                continue
            if (
                signal.setup_type == "SMART_ZONE_BREAK_CONFIRMATION"
                and self.cfg.smart_trade_block_break_start
                <= entry_time
                < self.cfg.smart_trade_block_break_end
            ):
                skipped.append(
                    SkippedSignal(
                        signal.date,
                        entry_time,
                        signal.direction,
                        signal.setup_type,
                        "Break confirmation blocked during midday window",
                        {
                            "entry_time": entry_time,
                            "blocked_from": self.cfg.smart_trade_block_break_start,
                            "blocked_until": self.cfg.smart_trade_block_break_end,
                        },
                    )
                )
                continue
            allowed.append(signal)
        return allowed, skipped
