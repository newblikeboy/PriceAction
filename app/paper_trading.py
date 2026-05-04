from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import PaperTrade, SignalCandidate


class PaperTradeEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg

    def create_trade(self, signal: SignalCandidate) -> PaperTrade:
        trade = PaperTrade.from_signal(signal)
        trade.underlying_entry_price = trade.entry_index_price
        return trade

    def update_open_trade_with_quote(self, trade: PaperTrade, quote_price: float, quote_time: datetime | None = None) -> PaperTrade:
        if trade.status != "OPEN":
            return trade
        ts = quote_time or datetime.now()
        row = pd.Series({"high": quote_price, "low": quote_price, "close": quote_price, "time": ts.strftime("%H:%M")})
        self._update_excursions(trade, row)
        if trade.direction == "CE":
            if quote_price <= trade.sl_index_price:
                return self._close(trade, ts, trade.sl_index_price, "SL_HIT")
            if quote_price >= trade.target_index_price:
                return self._close(trade, ts, trade.target_index_price, "TARGET_HIT")
        else:
            if quote_price >= trade.sl_index_price:
                return self._close(trade, ts, trade.sl_index_price, "SL_HIT")
            if quote_price <= trade.target_index_price:
                return self._close(trade, ts, trade.target_index_price, "TARGET_HIT")
        if ts.strftime("%H:%M") >= self.cfg.square_off_time:
            return self._close(trade, ts, quote_price, "TIME_EXIT")
        return trade

    def simulate_trade(self, trade: PaperTrade, candles_1m: pd.DataFrame) -> PaperTrade:
        start = pd.to_datetime(f"{trade.date} {trade.entry_time}")
        day = candles_1m[(candles_1m.index >= start) & (candles_1m["date"].astype(str) == trade.date)]
        for ts, row in day.iterrows():
            self._update_excursions(trade, row)
            if trade.direction == "CE":
                if row["low"] <= trade.sl_index_price:
                    return self._close(trade, ts, trade.sl_index_price, "SL_HIT")
                if row["high"] >= trade.target_index_price:
                    return self._close(trade, ts, trade.target_index_price, "TARGET_HIT")
            else:
                if row["high"] >= trade.sl_index_price:
                    return self._close(trade, ts, trade.sl_index_price, "SL_HIT")
                if row["low"] <= trade.target_index_price:
                    return self._close(trade, ts, trade.target_index_price, "TARGET_HIT")
            if row["time"] >= self.cfg.square_off_time:
                return self._close(trade, ts, float(row["close"]), "TIME_EXIT")
        if not day.empty:
            row = day.iloc[-1]
            return self._close(trade, day.index[-1], float(row["close"]), "DATA_END_EXIT")
        return trade

    def simulate_many(self, signals: Iterable[SignalCandidate], candles_1m: pd.DataFrame) -> list[PaperTrade]:
        return [self.simulate_trade(self.create_trade(signal), candles_1m) for signal in signals]

    def _update_excursions(self, trade: PaperTrade, row: pd.Series) -> None:
        if trade.direction == "CE":
            favorable = float(row["high"] - trade.entry_index_price)
            adverse = float(trade.entry_index_price - row["low"])
        else:
            favorable = float(trade.entry_index_price - row["low"])
            adverse = float(row["high"] - trade.entry_index_price)
        trade.max_favorable_excursion = max(trade.max_favorable_excursion, round(favorable, 2))
        trade.max_adverse_excursion = max(trade.max_adverse_excursion, round(adverse, 2))

    def _close(self, trade: PaperTrade, ts: datetime, price: float, reason: str) -> PaperTrade:
        trade.status = "CLOSED"
        trade.exit_time = ts.strftime("%H:%M")
        trade.exit_index_price = round(float(price), 2)
        trade.exit_reason = reason
        trade.underlying_entry_price = trade.underlying_entry_price or trade.entry_index_price
        trade.underlying_exit_price = trade.exit_index_price
        if trade.direction == "CE":
            underlying_points = trade.exit_index_price - trade.entry_index_price
        else:
            underlying_points = trade.entry_index_price - trade.exit_index_price
        trade.underlying_points = round(underlying_points, 2)
        option_entry = float(trade.option_entry_ltp or 0)
        option_exit = float(trade.option_exit_ltp or trade.option_mark_ltp or 0)
        if option_entry > 0 and option_exit > 0:
            points = option_exit - option_entry
            trade.option_exit_ltp = round(option_exit, 2)
            trade.option_points = round(points, 2)
            trade.pnl_source = "option_quote"
        else:
            points = underlying_points
            trade.pnl_source = "underlying_backtest"
        trade.features["points"] = round(points, 2)
        trade.r_multiple = round(points / trade.risk_points, 3) if trade.risk_points else 0
        trade.result = "WIN" if trade.r_multiple > 0 else "LOSS" if trade.r_multiple < 0 else "FLAT"
        trade.features.update(
            {
                "result": trade.result,
                "points": round(points, 2),
                "option_points": trade.option_points,
                "pnl_source": trade.pnl_source,
                "underlying_points": trade.underlying_points,
                "R_multiple": trade.r_multiple,
                "max_favorable_excursion": trade.max_favorable_excursion,
                "max_adverse_excursion": trade.max_adverse_excursion,
                "reason_for_exit": reason,
            }
        )
        return trade
