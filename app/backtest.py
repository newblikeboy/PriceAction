from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import PaperTrade, SignalCandidate, SkippedSignal
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
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        option_snapshot: dict[str, Any] | None = None,
        test_start_date: str | date | None = None,
        test_end_date: str | date | None = None,
    ) -> BacktestResult:
        trades: list[PaperTrade] = []
        skipped: list[SkippedSignal] = []
        self.signals.option_snapshot = option_snapshot
        candles_5m = self._normalize_candles(candles_5m)
        trading_dates = self._test_dates(candles_5m, test_start_date, test_end_date)
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
        open_until: pd.Timestamp | None = None
        for index, trading_date in enumerate(trading_dates, start=1):
            day_rows = candles_5m[candles_5m["date"] == trading_date]
            for candle_time, row in day_rows.iterrows():
                if row["time"] < self.cfg.opening_range_end:
                    continue
                if row["time"] > self.cfg.no_fresh_trade_after:
                    continue
                if open_until is not None and pd.to_datetime(candle_time) <= open_until:
                    continue
                visible_candles = candles_5m[candles_5m.index <= candle_time]
                visible_levels = self.levels.calculate(visible_candles, trading_date)
                candle_signals, candle_skipped = self.signals.generate_for_candle(
                    visible_candles,
                    visible_levels,
                    trading_date,
                    candle_time,
                )
                skipped.extend(candle_skipped)
                if not candle_signals:
                    continue
                signal = max(candle_signals, key=lambda item: item.setup_score)
                entry_at = pd.to_datetime(f"{signal.date} {signal.time}")
                if open_until is not None and entry_at <= open_until:
                    skipped.append(
                        SkippedSignal(
                            signal.date,
                            str(signal.features.get("time") or signal.time),
                            signal.direction,
                            signal.setup_type,
                            "Another trade is already open",
                            {"entry_time": signal.time, "open_until": open_until.strftime("%H:%M")},
                        )
                    )
                    continue
                for extra_signal in candle_signals:
                    if extra_signal is signal:
                        continue
                    skipped.append(
                        SkippedSignal(
                            extra_signal.date,
                            str(extra_signal.features.get("time") or extra_signal.time),
                            extra_signal.direction,
                            extra_signal.setup_type,
                            "Another higher-scored trade was selected on this candle",
                            {"selected_setup": signal.setup_type, "selected_score": signal.setup_score},
                        )
                    )
                simulated = self.paper.simulate_trade(self.paper.create_trade(signal), candles_5m)
                trades.append(simulated)
                if simulated.exit_time:
                    open_until = pd.to_datetime(f"{simulated.date} {simulated.exit_time}")
                else:
                    open_until = pd.to_datetime(f"{signal.date} {self.cfg.square_off_time}")
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
        summary_candles = candles_5m[candles_5m["date"].isin(trading_dates)]
        return BacktestResult(trades=trades, skipped_signals=skipped, summary=self.summary(trades, summary_candles))

    def _normalize_candles(self, candles_5m: pd.DataFrame) -> pd.DataFrame:
        frame = candles_5m.copy()
        if "datetime" in frame.columns:
            frame["datetime"] = pd.to_datetime(frame["datetime"])
            frame = frame.set_index("datetime")
        frame.index = pd.to_datetime(frame.index)
        frame = frame.sort_index()
        if "date" not in frame.columns:
            frame["date"] = frame.index.date
        if "time" not in frame.columns:
            frame["time"] = frame.index.strftime("%H:%M")
        return frame

    @staticmethod
    def _test_dates(
        candles_5m: pd.DataFrame,
        test_start_date: str | date | None,
        test_end_date: str | date | None,
    ) -> list[date]:
        dates = sorted(candles_5m["date"].unique())
        if test_start_date is not None:
            start = pd.to_datetime(test_start_date).date()
            dates = [item for item in dates if item >= start]
        if test_end_date is not None:
            end = pd.to_datetime(test_end_date).date()
            dates = [item for item in dates if item <= end]
        return dates

    def _select_one_trade_at_a_time(
        self,
        signals: list[SignalCandidate],
        candles_5m: pd.DataFrame,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        selected: list[SignalCandidate] = []
        skipped: list[SkippedSignal] = []
        open_until: pd.Timestamp | None = None
        for signal in sorted(signals, key=lambda item: (item.date, item.time, -item.setup_score)):
            entry_at = pd.to_datetime(f"{signal.date} {signal.time}")
            if open_until is not None and entry_at <= open_until:
                skipped.append(
                    SkippedSignal(
                        signal.date,
                        str(signal.features.get("time") or signal.time),
                        signal.direction,
                        signal.setup_type,
                        "Another trade is already open",
                        {"entry_time": signal.time, "open_until": open_until.strftime("%H:%M")},
                    )
                )
                continue
            simulated = self.paper.simulate_trade(self.paper.create_trade(signal), candles_5m)
            selected.append(signal)
            if simulated.exit_time:
                open_until = pd.to_datetime(f"{simulated.date} {simulated.exit_time}")
            else:
                open_until = pd.to_datetime(f"{signal.date} {self.cfg.square_off_time}")
        return selected, skipped

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
