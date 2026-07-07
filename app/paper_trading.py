from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Iterable

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import PaperTrade, SignalCandidate


ZONE_LOSS_COOLDOWN_SETUPS = {
    "SMART_ZONE_BREAK_CONFIRMATION",
    "SMART_ZONE_TREND_CONTINUATION",
}


def failed_zone_ids_from_trades(trades: Iterable[dict[str, Any]]) -> set[str]:
    zone_ids: set[str] = set()
    for trade in trades:
        if str(trade.get("result") or "").upper() != "LOSS":
            continue
        features: Any = trade.get("features")
        if not isinstance(features, dict):
            features = trade.get("features_json")
        if isinstance(features, str):
            try:
                features = json.loads(features)
            except (TypeError, json.JSONDecodeError):
                features = {}
        zone = features.get("smart_zone") if isinstance(features, dict) else None
        zone_id = zone.get("zone_id") if isinstance(zone, dict) else None
        if zone_id:
            zone_ids.add(str(zone_id))
    return zone_ids


def filter_failed_zone_signals(
    signals: Iterable[SignalCandidate],
    failed_zone_ids: set[str],
    cfg: StrategyConfig = config,
) -> tuple[list[SignalCandidate], list[SignalCandidate]]:
    allowed: list[SignalCandidate] = []
    blocked: list[SignalCandidate] = []
    for signal in signals:
        zone = signal.features.get("smart_zone") if isinstance(signal.features, dict) else None
        zone_id = str(zone.get("zone_id")) if isinstance(zone, dict) and zone.get("zone_id") else None
        should_block = (
            cfg.smart_trade_zone_loss_cooldown_enabled
            and signal.setup_type in ZONE_LOSS_COOLDOWN_SETUPS
            and zone_id is not None
            and zone_id in failed_zone_ids
        )
        (blocked if should_block else allowed).append(signal)
    return allowed, blocked


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
        # A single live quote is just a one-point candle; route it through the same
        # candle update used by replay/backtest so the live trade gets identical
        # breakeven / profit-lock / near-target / square-off handling.
        if trade.status != "OPEN":
            return trade
        ts = quote_time or datetime.now()
        row = pd.Series({"high": quote_price, "low": quote_price, "close": quote_price, "time": ts.strftime("%H:%M")})
        return self.update_open_trade_with_candle(trade, ts, row)

    def exit_decision_for_quote(
        self,
        *,
        direction: str,
        quote_price: float,
        active_sl: float,
        target_price: float,
        entry_price: float,
        reward_points: float,
        breakeven_active: bool = False,
        profit_lock_active: bool = False,
    ) -> tuple[float, str] | None:
        stop_reason = "PROFIT_LOCK_HIT" if profit_lock_active else "BREAKEVEN_HIT" if breakeven_active else "SL_HIT"
        near_target_pct = float(getattr(self.cfg, "paper_near_target_exit_pct", 0.0) or 0.0)
        near_target_pct = min(max(near_target_pct, 0.0), 1.0)
        if direction == "CE":
            if quote_price <= active_sl:
                return active_sl, stop_reason
            if quote_price >= target_price:
                return target_price, "TARGET_HIT"
            near_target = entry_price + (reward_points * near_target_pct)
            if near_target_pct > 0 and quote_price >= near_target:
                return near_target, "NEAR_TARGET_EXIT"
        elif direction == "PE":
            if quote_price >= active_sl:
                return active_sl, stop_reason
            if quote_price <= target_price:
                return target_price, "TARGET_HIT"
            near_target = entry_price - (reward_points * near_target_pct)
            if near_target_pct > 0 and quote_price <= near_target:
                return near_target, "NEAR_TARGET_EXIT"
        return None

    def simulate_trade(self, trade: PaperTrade, candles_5m: pd.DataFrame) -> PaperTrade:
        # Backtest drives the trade candle-by-candle through the exact same update
        # used by the replay bar, so backtest and replay can never diverge. The only
        # extra is a data-end fallback close when the day's candles run out.
        start = pd.to_datetime(f"{trade.date} {trade.entry_time}")
        day = candles_5m[(candles_5m.index >= start) & (candles_5m["date"].astype(str) == trade.date)]
        for ts, row in day.iterrows():
            if trade.status != "OPEN":
                break
            trade = self.update_open_trade_with_candle(trade, ts, row)
        if trade.status == "OPEN" and not day.empty:
            trade = self._close(trade, day.index[-1], float(day.iloc[-1]["close"]), "DATA_END_EXIT")
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
        trailed_sl, breakeven_active, profit_lock_active, armed = self.arm_protective_sl(
            direction=trade.direction,
            entry_price=trade.entry_index_price,
            risk_points=trade.risk_points,
            max_favorable_excursion=trade.max_favorable_excursion,
            active_sl=active_sl,
            breakeven_active=breakeven_active,
            profit_lock_active=profit_lock_active,
        )
        if armed == "profit_lock":
            trade.features["active_sl_index_price"] = trailed_sl
            trade.features["profit_lock_active"] = True
            trade.features["profit_lock_armed_at"] = ts.strftime("%H:%M")
            trade.features["profit_lock_R"] = round(float(getattr(self.cfg, "paper_profit_lock_r", 0.0) or 0.0), 3)
        elif armed == "breakeven":
            trade.features["active_sl_index_price"] = trailed_sl
            trade.features["breakeven_active"] = True
            trade.features["breakeven_armed_at"] = ts.strftime("%H:%M")
        if str(row.get("time") or ts.strftime("%H:%M")) >= self.cfg.square_off_time:
            return self._close(trade, ts, float(row["close"]), "TIME_EXIT")
        return trade

    def simulate_many(self, signals: Iterable[SignalCandidate], candles_5m: pd.DataFrame) -> list[PaperTrade]:
        return [self.simulate_trade(self.create_trade(signal), candles_5m) for signal in signals]

    def arm_protective_sl(
        self,
        *,
        direction: str,
        entry_price: float,
        risk_points: float,
        max_favorable_excursion: float,
        active_sl: float,
        breakeven_active: bool,
        profit_lock_active: bool,
    ) -> tuple[float, bool, bool, str | None]:
        """Breakeven / profit-lock arming shared by replay, backtest and live so all
        three trail the stop with identical rules. Once the move reaches
        ``paper_profit_lock_after_r`` of risk it locks ``paper_profit_lock_r`` of
        profit; otherwise once it reaches ``paper_breakeven_after_r`` it moves the
        stop to entry. Returns the (possibly trailed) active SL, the updated
        breakeven/profit-lock flags, and which protection armed this call (or None)."""
        if breakeven_active or profit_lock_active or risk_points <= 0:
            return active_sl, breakeven_active, profit_lock_active, None
        lock_after_r = float(getattr(self.cfg, "paper_profit_lock_after_r", 0.0) or 0.0)
        lock_r = float(getattr(self.cfg, "paper_profit_lock_r", 0.0) or 0.0)
        breakeven_after_r = float(getattr(self.cfg, "paper_breakeven_after_r", 0.0) or 0.0)
        if lock_after_r > 0 and lock_r > 0 and max_favorable_excursion >= risk_points * lock_after_r:
            lock_points = risk_points * lock_r
            trailed = round(float(entry_price + lock_points if direction == "CE" else entry_price - lock_points), 2)
            return trailed, breakeven_active, True, "profit_lock"
        if breakeven_after_r > 0 and max_favorable_excursion >= risk_points * breakeven_after_r:
            return round(float(entry_price), 2), True, profit_lock_active, "breakeven"
        return active_sl, breakeven_active, profit_lock_active, None

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
        # Futures-only: paper trade PnL always tracks the underlying index move.
        points = underlying_points
        trade.pnl_source = "underlying_backtest"
        trade.features["points"] = round(points, 2)
        trade.r_multiple = round(points / trade.risk_points, 3) if trade.risk_points else 0
        trade.result = "WIN" if trade.r_multiple > 0 else "LOSS" if trade.r_multiple < 0 else "FLAT"
        trade.features.update(
            {
                "result": trade.result,
                "points": round(points, 2),
                "pnl_source": trade.pnl_source,
                "underlying_points": trade.underlying_points,
                "R_multiple": trade.r_multiple,
                "max_favorable_excursion": trade.max_favorable_excursion,
                "max_adverse_excursion": trade.max_adverse_excursion,
                "reason_for_exit": reason,
            }
        )
        return trade
