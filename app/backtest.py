from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import PaperTrade, SkippedSignal
from app.engines.levels import LevelEngine
from app.engines.signals import SignalEngine
from app.paper_trading import PaperTradeEngine


@dataclass
class BacktestResult:
    trades: list[PaperTrade]
    skipped_signals: list[SkippedSignal]
    summary: dict[str, Any]


class BacktestRunner:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg
        self.levels = LevelEngine(cfg)
        self.signals = SignalEngine(cfg)
        self.paper = PaperTradeEngine(cfg)

    def run(
        self,
        candles_5m: pd.DataFrame,
        candles_1m: pd.DataFrame,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> BacktestResult:
        trades: list[PaperTrade] = []
        skipped: list[SkippedSignal] = []
        trading_dates = sorted(candles_5m["date"].unique())
        total_days = len(trading_dates)
        if progress_callback:
            progress_callback(
                {
                    "completed_days": 0,
                    "total_days": total_days,
                    "percent": 0,
                    "current_step": "Preparing candles",
                }
            )
        for index, trading_date in enumerate(trading_dates, start=1):
            day_levels = self.levels.calculate(candles_5m, trading_date)
            day_signals, day_skipped = self.signals.generate_for_day(candles_5m, candles_1m, day_levels, trading_date)
            skipped.extend(day_skipped)
            trades.extend(self.paper.simulate_many(day_signals, candles_1m))
            if progress_callback:
                progress_callback(
                    {
                        "completed_days": index,
                        "total_days": total_days,
                        "percent": round(index / total_days * 100, 2) if total_days else 100,
                        "current_step": f"Processed {trading_date}",
                        "trades_count": len(trades),
                        "skipped_count": len(skipped),
                    }
                )
        return BacktestResult(trades=trades, skipped_signals=skipped, summary=self.summary(trades, candles_5m))

    def summary(self, trades: list[PaperTrade], candles_5m: pd.DataFrame) -> dict[str, Any]:
        closed = [trade for trade in trades if trade.status == "CLOSED"]
        wins = [trade for trade in closed if trade.result == "WIN"]
        losses = [trade for trade in closed if trade.result == "LOSS"]
        r_values = [trade.r_multiple or 0 for trade in closed]
        point_values = [self._trade_points(trade) for trade in closed]
        setup_perf = self._group_points(closed, "setup_type")
        direction_perf = self._group_points(closed, "direction")
        trade_days = {trade.date for trade in trades}
        all_days = {str(day) for day in candles_5m["date"].unique()}
        return {
            "total_trades": len(closed),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(closed) * 100, 2) if closed else 0,
            "average_points": round(sum(point_values) / len(point_values), 2) if point_values else 0,
            "total_points": round(sum(point_values), 2),
            "max_drawdown_points": self._max_drawdown(point_values),
            "average_R": round(sum(r_values) / len(r_values), 3) if r_values else 0,
            "total_R": round(sum(r_values), 3),
            "max_drawdown_R": self._max_drawdown(r_values),
            "best_setup_type": max(setup_perf, key=lambda key: setup_perf[key]["average_points"]) if setup_perf else None,
            "worst_setup_type": min(setup_perf, key=lambda key: setup_perf[key]["average_points"]) if setup_perf else None,
            "CE_performance": direction_perf.get("CE", {"trades": 0, "average_points": 0, "total_points": 0}),
            "PE_performance": direction_perf.get("PE", {"trades": 0, "average_points": 0, "total_points": 0}),
            "no_trade_days": sorted(all_days - trade_days),
        }

    def _group_points(self, trades: list[PaperTrade], attr: str) -> dict[str, dict[str, float]]:
        grouped: dict[str, list[float]] = {}
        for trade in trades:
            grouped.setdefault(getattr(trade, attr), []).append(self._trade_points(trade))
        return {
            key: {"trades": len(values), "average_points": round(sum(values) / len(values), 2), "total_points": round(sum(values), 2)}
            for key, values in grouped.items()
        }

    def _trade_points(self, trade: PaperTrade) -> float:
        if trade.exit_index_price is None:
            return 0.0
        if trade.direction == "CE":
            return round(float(trade.exit_index_price - trade.entry_index_price), 2)
        return round(float(trade.entry_index_price - trade.exit_index_price), 2)

    def _max_drawdown(self, r_values: list[float]) -> float:
        peak = 0.0
        equity = 0.0
        max_dd = 0.0
        for value in r_values:
            equity += value
            peak = max(peak, equity)
            max_dd = min(max_dd, equity - peak)
        return round(max_dd, 3)
