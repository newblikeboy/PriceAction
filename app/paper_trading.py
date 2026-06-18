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
        selected = trade.features.get("selected_option_contract") if isinstance(trade.features, dict) else None
        if isinstance(selected, dict):
            trade.option_symbol = str(selected.get("symbol") or "").strip() or None
            trade.option_side = str(selected.get("side") or trade.direction).strip() or trade.direction
            strike = selected.get("strike")
            try:
                trade.option_strike = round(float(strike), 2)
            except (TypeError, ValueError):
                trade.option_strike = None
            trade.pnl_source = "underlying_backtest"
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

    def simulate_trade(self, trade: PaperTrade, candles_5m: pd.DataFrame) -> PaperTrade:
        start = pd.to_datetime(f"{trade.date} {trade.entry_time}")
        day = candles_5m[(candles_5m.index >= start) & (candles_5m["date"].astype(str) == trade.date)]
        active_sl = float(trade.sl_index_price)
        breakeven_active = False
        profit_lock_active = False
        near_target_pct = float(getattr(self.cfg, "paper_near_target_exit_pct", 0.0) or 0.0)
        near_target_pct = min(max(near_target_pct, 0.0), 1.0)
        for ts, row in day.iterrows():
            self._update_excursions(trade, row)
            if trade.direction == "CE":
                if row["low"] <= active_sl:
                    return self._close(trade, ts, active_sl, "PROFIT_LOCK_HIT" if profit_lock_active else "BREAKEVEN_HIT" if breakeven_active else "SL_HIT")
                if row["high"] >= trade.target_index_price:
                    return self._close(trade, ts, trade.target_index_price, "TARGET_HIT")
                near_target = trade.entry_index_price + (trade.reward_points * near_target_pct)
                if near_target_pct > 0 and row["high"] >= near_target:
                    return self._close(trade, ts, near_target, "NEAR_TARGET_EXIT")
            else:
                if row["high"] >= active_sl:
                    return self._close(trade, ts, active_sl, "PROFIT_LOCK_HIT" if profit_lock_active else "BREAKEVEN_HIT" if breakeven_active else "SL_HIT")
                if row["low"] <= trade.target_index_price:
                    return self._close(trade, ts, trade.target_index_price, "TARGET_HIT")
                near_target = trade.entry_index_price - (trade.reward_points * near_target_pct)
                if near_target_pct > 0 and row["low"] <= near_target:
                    return self._close(trade, ts, near_target, "NEAR_TARGET_EXIT")
            if not breakeven_active and not profit_lock_active and trade.risk_points > 0:
                lock_after_r = float(getattr(self.cfg, "paper_profit_lock_after_r", 0.0) or 0.0)
                lock_r = float(getattr(self.cfg, "paper_profit_lock_r", 0.0) or 0.0)
                breakeven_after_r = float(getattr(self.cfg, "paper_breakeven_after_r", 0.0) or 0.0)
                if lock_after_r > 0 and lock_r > 0 and trade.max_favorable_excursion >= trade.risk_points * lock_after_r:
                    lock_points = trade.risk_points * lock_r
                    active_sl = round(float(trade.entry_index_price + lock_points if trade.direction == "CE" else trade.entry_index_price - lock_points), 2)
                    profit_lock_active = True
                    trade.features["profit_lock_armed_at"] = ts.strftime("%H:%M")
                    trade.features["profit_lock_R"] = round(lock_r, 3)
                elif breakeven_after_r > 0 and trade.max_favorable_excursion >= trade.risk_points * breakeven_after_r:
                    active_sl = float(trade.entry_index_price)
                    breakeven_active = True
                    trade.features["breakeven_armed_at"] = ts.strftime("%H:%M")
            if row["time"] >= self.cfg.square_off_time:
                return self._close(trade, ts, float(row["close"]), "TIME_EXIT")
        if not day.empty:
            row = day.iloc[-1]
            return self._close(trade, day.index[-1], float(row["close"]), "DATA_END_EXIT")
        return trade

    def update_open_trade_with_candle(self, trade: PaperTrade, ts: datetime, row: pd.Series) -> PaperTrade:
        if trade.status != "OPEN":
            return trade
        active_sl = float(trade.features.get("active_sl_index_price") or trade.sl_index_price)
        breakeven_active = bool(trade.features.get("breakeven_active"))
        profit_lock_active = bool(trade.features.get("profit_lock_active"))
        near_target_pct = float(getattr(self.cfg, "paper_near_target_exit_pct", 0.0) or 0.0)
        near_target_pct = min(max(near_target_pct, 0.0), 1.0)
        self._update_excursions(trade, row)
        if trade.direction == "CE":
            if row["low"] <= active_sl:
                return self._close(trade, ts, active_sl, "PROFIT_LOCK_HIT" if profit_lock_active else "BREAKEVEN_HIT" if breakeven_active else "SL_HIT")
            if row["high"] >= trade.target_index_price:
                return self._close(trade, ts, trade.target_index_price, "TARGET_HIT")
            near_target = trade.entry_index_price + (trade.reward_points * near_target_pct)
            if near_target_pct > 0 and row["high"] >= near_target:
                return self._close(trade, ts, near_target, "NEAR_TARGET_EXIT")
        else:
            if row["high"] >= active_sl:
                return self._close(trade, ts, active_sl, "PROFIT_LOCK_HIT" if profit_lock_active else "BREAKEVEN_HIT" if breakeven_active else "SL_HIT")
            if row["low"] <= trade.target_index_price:
                return self._close(trade, ts, trade.target_index_price, "TARGET_HIT")
            near_target = trade.entry_index_price - (trade.reward_points * near_target_pct)
            if near_target_pct > 0 and row["low"] <= near_target:
                return self._close(trade, ts, near_target, "NEAR_TARGET_EXIT")
        if not breakeven_active and not profit_lock_active and trade.risk_points > 0:
            lock_after_r = float(getattr(self.cfg, "paper_profit_lock_after_r", 0.0) or 0.0)
            lock_r = float(getattr(self.cfg, "paper_profit_lock_r", 0.0) or 0.0)
            breakeven_after_r = float(getattr(self.cfg, "paper_breakeven_after_r", 0.0) or 0.0)
            if lock_after_r > 0 and lock_r > 0 and trade.max_favorable_excursion >= trade.risk_points * lock_after_r:
                lock_points = trade.risk_points * lock_r
                trade.features["active_sl_index_price"] = round(float(trade.entry_index_price + lock_points if trade.direction == "CE" else trade.entry_index_price - lock_points), 2)
                trade.features["profit_lock_active"] = True
                trade.features["profit_lock_armed_at"] = ts.strftime("%H:%M")
                trade.features["profit_lock_R"] = round(lock_r, 3)
            elif breakeven_after_r > 0 and trade.max_favorable_excursion >= trade.risk_points * breakeven_after_r:
                trade.features["active_sl_index_price"] = round(float(trade.entry_index_price), 2)
                trade.features["breakeven_active"] = True
                trade.features["breakeven_armed_at"] = ts.strftime("%H:%M")
        if str(row.get("time") or ts.strftime("%H:%M")) >= self.cfg.square_off_time:
            return self._close(trade, ts, float(row["close"]), "TIME_EXIT")
        return trade

    def simulate_many(self, signals: Iterable[SignalCandidate], candles_5m: pd.DataFrame) -> list[PaperTrade]:
        return [self.simulate_trade(self.create_trade(signal), candles_5m) for signal in signals]

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
