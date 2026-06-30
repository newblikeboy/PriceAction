from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import PaperTrade, SignalCandidate, SkippedSignal
from app.engines.levels import LevelEngine
from app.engines.signals import SignalEngine
from app.paper_trading import PaperTradeEngine


IST = ZoneInfo("Asia/Kolkata")
ZONE_LOSS_COOLDOWN_SETUPS = {
    "SMART_ZONE_BREAK_CONFIRMATION",
    "SMART_ZONE_TREND_CONTINUATION",
}


def replay_chart_time(value: Any) -> int:
    dt = pd.to_datetime(value)
    utc_clock = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=ZoneInfo("UTC"))
    return int(utc_clock.timestamp())


@dataclass
class ReplayBarSession:
    symbol: str
    start_date: str
    end_date: str
    candles_5m: pd.DataFrame
    warmup_candles: int = 30
    context_trading_days: int = 2
    cfg: StrategyConfig = config
    session_id: str = field(default_factory=lambda: uuid4().hex)
    current_index: int = field(init=False)
    replay_start_index: int = field(init=False)
    pending_signal: SignalCandidate | None = None
    open_trade: PaperTrade | None = None
    trades: list[PaperTrade] = field(default_factory=list)
    skipped: list[SkippedSignal] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    failed_zone_ids_by_date: dict[str, set[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.candles_5m = self._normalize(self.candles_5m)
        if self.candles_5m.empty:
            raise ValueError("Replay needs at least one 5m candle.")
        self.context_trading_days = max(0, int(self.context_trading_days or 0))
        self.candles_5m = self._trim_context_days(self.candles_5m)
        self.replay_start_index = self._first_replay_index()
        self.warmup_candles = self.replay_start_index
        self.current_index = self.replay_start_index
        self.levels = LevelEngine(self.cfg)
        self.signals = SignalEngine(self.cfg)
        self.paper = PaperTradeEngine(self.cfg)
        self._chart_zone_cache: dict[int, list[dict[str, Any]]] = {}
        self._engine_rows = self.signals.smart_trades._rows(self.candles_5m)
        self._level_cache: dict[int, Any] = {}

    def reset(self) -> dict[str, Any]:
        self.current_index = self.replay_start_index
        self.pending_signal = None
        self.open_trade = None
        self.trades.clear()
        self.skipped.clear()
        self.events.clear()
        self.failed_zone_ids_by_date.clear()
        self._chart_zone_cache.clear()
        self._level_cache.clear()
        return self.payload()

    def next(self, count: int = 1) -> dict[str, Any]:
        count = max(1, int(count or 1))
        frames: list[dict[str, Any]] = []
        for _ in range(count):
            if self.current_index >= len(self.candles_5m) - 1:
                break
            self.current_index += 1
            self._evaluate_current_candle()
            frames.append(self._compact_payload(delta_from_index=self.current_index))
        if frames:
            frames[-1] = self.payload(delta_from_index=self.current_index)
        payload = dict(frames[-1]) if frames else self.payload(delta_from_index=self.current_index + 1)
        payload["frames"] = frames
        return payload

    def previous(self) -> dict[str, Any]:
        target = max(self.replay_start_index, self.current_index - 1)
        return self.seek(target)

    def seek(self, target_index: int) -> dict[str, Any]:
        target_index = max(0, min(int(target_index), len(self.candles_5m) - 1))
        target_index = max(self.replay_start_index, target_index)
        self.current_index = self.replay_start_index
        self.pending_signal = None
        self.open_trade = None
        self.trades.clear()
        self.skipped.clear()
        self.events.clear()
        self.failed_zone_ids_by_date.clear()
        self._chart_zone_cache.clear()
        self._level_cache.clear()
        while self.current_index < target_index:
            self.current_index += 1
            self._evaluate_current_candle()
        return self.payload()

    def payload(self, delta_from_index: int | None = None) -> dict[str, Any]:
        visible = self.visible_candles()
        current_ts = visible.index[-1]
        current_row = visible.iloc[-1]
        current_price = float(current_row["close"])
        zones = self._zones_payload(visible, current_price, current_ts)
        is_delta = delta_from_index is not None
        delta_candles = pd.DataFrame()
        if is_delta:
            start = max(self.replay_start_index, min(int(delta_from_index or 0), self.current_index + 1))
            delta_candles = self.candles_5m.iloc[start : self.current_index + 1].copy()
        return {
            "session_id": self.session_id,
            "is_delta": is_delta,
            "symbol": self.symbol,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "warmup_candles": self.warmup_candles,
            "context_trading_days": self.context_trading_days,
            "context_candles": self.replay_start_index,
            "current_index": self.current_index,
            "visible_candles": len(visible),
            "total_candles": len(self.candles_5m) - self.replay_start_index,
            "current_time": str(current_ts),
            "current_price": round(current_price, 2),
            "is_done": self.current_index >= len(self.candles_5m) - 1,
            "candles": self._candles_payload(visible) if not is_delta else [],
            "candles_delta": self._candles_payload(delta_candles) if is_delta else [],
            "zones": zones,
            "open_trade": self._trade_payload(self.open_trade) if self.open_trade else None,
            "pending_signal": self._signal_payload(self.pending_signal) if self.pending_signal else None,
            "trades": [self._trade_payload(trade) for trade in self.trades],
            "skipped": [self._skipped_payload(item) for item in self.skipped[-80:]],
            "events": self.events[-80:],
            "markers": self._markers_payload(),
            "trade_levels": self._trade_levels_payload(),
            "summary": self._summary_payload(),
        }

    def visible_candles(self) -> pd.DataFrame:
        return self.candles_5m.iloc[self.replay_start_index : self.current_index + 1].copy()

    def engine_candles(self) -> pd.DataFrame:
        return self.candles_5m.iloc[: self.current_index + 1]

    def _compact_payload(self, delta_from_index: int) -> dict[str, Any]:
        start = max(self.replay_start_index, min(int(delta_from_index or 0), self.current_index + 1))
        delta_candles = self.candles_5m.iloc[start : self.current_index + 1]
        current_ts = self.candles_5m.index[self.current_index]
        current_row = self.candles_5m.iloc[self.current_index]
        return {
            "session_id": self.session_id,
            "is_delta": True,
            "compact": True,
            "symbol": self.symbol,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "current_index": self.current_index,
            "visible_candles": self.current_index - self.replay_start_index + 1,
            "total_candles": len(self.candles_5m) - self.replay_start_index,
            "current_time": str(current_ts),
            "current_price": round(float(current_row["close"]), 2),
            "is_done": self.current_index >= len(self.candles_5m) - 1,
            "candles": [],
            "candles_delta": self._candles_payload(delta_candles),
        }

    def _levels_for_current_index(self, visible: pd.DataFrame, trading_date: date) -> Any:
        cached = self._level_cache.get(self.current_index)
        if cached is not None:
            return cached
        level_set = self.levels.calculate(visible, trading_date)
        self._level_cache[self.current_index] = level_set
        return level_set

    def _evaluate_current_candle(self) -> None:
        ts = self.candles_5m.index[self.current_index]
        row = self.candles_5m.iloc[self.current_index]
        self._activate_pending_signal(ts)
        if self.open_trade is not None:
            before_status = self.open_trade.status
            self.open_trade = self.paper.update_open_trade_with_candle(self.open_trade, ts.to_pydatetime(), row)
            if before_status == "OPEN" and self.open_trade.status == "CLOSED":
                self.trades.append(self.open_trade)
                self._record_failed_zone(self.open_trade)
                self.events.append({"time": str(ts), "type": "trade_closed", "message": f"{self.open_trade.result} {self.open_trade.exit_reason}"})
                self.open_trade = None
        self._activate_pending_signal(ts)
        if self.pending_signal is not None:
            return
        if str(row["time"]) < self.cfg.opening_range_end or str(row["time"]) >= self.cfg.no_fresh_trade_after:
            return

        visible = self.engine_candles()
        trading_date = row["date"]
        level_set = self._levels_for_current_index(visible, trading_date)
        signal_rows = self._engine_rows.iloc[: self.current_index + 1]
        candle_signals, candle_skipped = self.signals.generate_for_candle_rows(signal_rows, level_set, trading_date, ts)
        self.skipped.extend(candle_skipped)
        if not candle_signals:
            return
        candidates = self._filter_failed_zone_signals(candle_signals)
        if self.open_trade is not None:
            candidates = [signal for signal in candidates if self._can_queue_reversal(signal)]
            for blocked_signal in candle_signals:
                if blocked_signal in candidates:
                    continue
                self.skipped.append(
                    SkippedSignal(
                        blocked_signal.date,
                        str(blocked_signal.features.get("time") or blocked_signal.time),
                        blocked_signal.direction,
                        blocked_signal.setup_type,
                        "Replay trade already open",
                        {"open_trade": self.open_trade.to_dict(), "setup_score": blocked_signal.setup_score},
                    )
                )
        if not candidates:
            return
        selected = max(candidates, key=lambda item: item.setup_score)
        self.pending_signal = selected
        for extra_signal in candle_signals:
            if extra_signal is selected:
                continue
            self.skipped.append(
                SkippedSignal(
                    extra_signal.date,
                    str(extra_signal.features.get("time") or extra_signal.time),
                    extra_signal.direction,
                    extra_signal.setup_type,
                    "Another higher-scored replay trade was selected on this candle",
                    {"selected_setup": selected.setup_type, "selected_score": selected.setup_score},
                )
            )
        self.events.append({"time": str(ts), "type": "signal", "message": f"{selected.direction} {selected.setup_type} queued for {selected.time}"})

    def _activate_pending_signal(self, ts: pd.Timestamp) -> None:
        if self.pending_signal is None:
            return
        entry_at = pd.to_datetime(f"{self.pending_signal.date} {self.pending_signal.time}")
        if ts < entry_at:
            return
        if self.open_trade is not None:
            if not self._can_queue_reversal(self.pending_signal):
                return
            self.open_trade = self.paper._close(self.open_trade, ts.to_pydatetime(), self.pending_signal.entry_index_price, "REVERSAL_EXIT")
            self.trades.append(self.open_trade)
            self._record_failed_zone(self.open_trade)
            self.events.append({"time": str(ts), "type": "trade_closed", "message": f"{self.open_trade.result} REVERSAL_EXIT"})
            self.open_trade = None
        self.open_trade = self.paper.create_trade(self.pending_signal)
        self.events.append({"time": str(ts), "type": "trade_opened", "message": f"{self.open_trade.direction} {self.open_trade.setup_type} opened"})
        self.pending_signal = None

    def _can_queue_reversal(self, signal: SignalCandidate) -> bool:
        if self.open_trade is None:
            return False
        if signal.direction == self.open_trade.direction:
            return False
        return signal.setup_type in {
            "SMART_ZONE_REJECTION_OVERRIDE",
            "SMART_ZONE_SWEEP_RECLAIM_DISPLACEMENT",
        }

    def _filter_failed_zone_signals(self, signals: list[SignalCandidate]) -> list[SignalCandidate]:
        if not self.cfg.smart_trade_zone_loss_cooldown_enabled:
            return signals
        out: list[SignalCandidate] = []
        for signal in signals:
            zone_id = self._signal_zone_id(signal)
            failed_zone_ids = self.failed_zone_ids_by_date.get(signal.date, set())
            blocked = (
                signal.setup_type in ZONE_LOSS_COOLDOWN_SETUPS
                and zone_id is not None
                and zone_id in failed_zone_ids
            )
            if blocked:
                self.skipped.append(
                    SkippedSignal(
                        signal.date,
                        str(signal.features.get("time") or signal.time),
                        signal.direction,
                        signal.setup_type,
                        "Zone blocked after same-day losing trade",
                        {"zone_id": zone_id},
                    )
                )
                continue
            out.append(signal)
        return out

    def _record_failed_zone(self, trade: PaperTrade) -> None:
        if not self.cfg.smart_trade_zone_loss_cooldown_enabled or trade.result != "LOSS":
            return
        zone_id = self._trade_zone_id(trade)
        if not zone_id:
            return
        self.failed_zone_ids_by_date.setdefault(trade.date, set()).add(zone_id)

    @staticmethod
    def _signal_zone_id(signal: SignalCandidate) -> str | None:
        zone = signal.features.get("smart_zone") if isinstance(signal.features, dict) else None
        if isinstance(zone, dict):
            value = zone.get("zone_id")
            return str(value) if value else None
        return None

    @staticmethod
    def _trade_zone_id(trade: PaperTrade) -> str | None:
        zone = trade.features.get("smart_zone") if isinstance(trade.features, dict) else None
        if isinstance(zone, dict):
            value = zone.get("zone_id")
            return str(value) if value else None
        return None

    def _zones_payload(self, visible: pd.DataFrame, current_price: float, current_ts: pd.Timestamp) -> list[dict[str, Any]]:
        anchor_index = self._zone_anchor_index()
        cached = self._chart_zone_cache.get(anchor_index)
        if cached is not None:
            return cached

        anchor_index = max(0, min(anchor_index, len(self.candles_5m) - 1))
        anchor_ts = pd.to_datetime(self.candles_5m.index[anchor_index])
        anchor_price = float(self.candles_5m.iloc[anchor_index]["close"])
        zones = self.signals.smart_trades.zones_for_candle_rows(
            self._engine_rows,
            anchor_ts.date(),
            anchor_ts,
        )

        today_date = anchor_ts.date()

        # Focus = top 5 zones closest to current price with score >= 70
        all_zones = sorted(zones, key=lambda zone: float(zone.score), reverse=True)[:15]
        zones_by_distance = sorted(all_zones, key=lambda z: abs(float(z.midpoint) - anchor_price))
        focus_ids = {z.zone_id for z in zones_by_distance[:5] if float(z.score) >= 70}

        out: list[dict[str, Any]] = []
        for zone in all_zones:
            zone_type = str(zone.zone_type or "")
            color = "#16a34a" if any(tag in zone_type for tag in ("demand", "swing_low", "breakout", "gap_up", "equal_lows", "bullish")) else "#dc2626"
            created_date = pd.to_datetime(zone.created_at).date()
            is_anchor = created_date < today_date
            out.append({
                "zone_id": zone.zone_id,
                "name": zone_type.upper().replace("_", " "),
                "zone_type": zone.zone_type,
                "low": round(float(zone.low), 2),
                "high": round(float(zone.high), 2),
                "midpoint": round(float(zone.midpoint), 2),
                "score": zone.score,
                "status": zone.status,
                "enhancers": zone.enhancers,
                "color": color,
                # When this zone became visible to a trader (None = prior-session, always shown)
                "formed_at": None if is_anchor else str(zone.created_at),
                "is_anchor": is_anchor,
                # Nearest high-scoring zones to current price = lookahead focus
                "is_focus": zone.zone_id in focus_ids,
            })
        self._chart_zone_cache[anchor_index] = out
        return out

    def _zone_anchor_index(self) -> int:
        current_date = self.candles_5m.iloc[self.current_index]["date"]
        day_positions = self.candles_5m.index[self.candles_5m["date"] == current_date]
        if day_positions.empty:
            return self.current_index
        day_start = int(self.candles_5m.index.get_loc(day_positions[0]))
        day_offset = self.current_index - day_start
        intraday_anchor = self.signals.smart_trades._intraday_refresh_anchor_index(day_offset)
        return day_start if intraday_anchor is None else day_start + intraday_anchor

    def _zone_history(self, visible: pd.DataFrame, current_ts: pd.Timestamp) -> pd.DataFrame:
        days = int(getattr(self.cfg, "smart_trade_zone_history_days", 0) or 0)
        if days <= 0:
            return visible
        current_date = pd.to_datetime(current_ts).date()
        previous_dates = sorted({day for day in visible["date"].unique() if day < current_date})
        keep_dates = set(previous_dates[-days:])
        keep_dates.add(current_date)
        history = visible[visible["date"].isin(keep_dates)]
        return history if not history.empty else visible

    def _trade_levels_payload(self) -> list[dict[str, Any]]:
        source = self.open_trade
        if source is None and self.pending_signal is not None:
            source = self.paper.create_trade(self.pending_signal)
        if source is None:
            return []
        return [
            {"price": source.entry_index_price, "name": "ENTRY", "color": "#1546c2"},
            {"price": source.sl_index_price, "name": "SL", "color": "#dc2626"},
            {"price": source.target_index_price, "name": "TARGET", "color": "#16a34a"},
        ]

    def _markers_payload(self) -> list[dict[str, Any]]:
        markers: list[dict[str, Any]] = []
        for trade in self.trades:
            entry_at = pd.to_datetime(f"{trade.date} {trade.entry_time}")
            markers.append(
                {
                    "time": replay_chart_time(entry_at),
                    "position": "belowBar" if trade.direction == "CE" else "aboveBar",
                    "color": "#1546c2",
                    "shape": "arrowUp" if trade.direction == "CE" else "arrowDown",
                    "text": f"{trade.direction} entry",
                }
            )
            if trade.exit_time:
                exit_at = pd.to_datetime(f"{trade.date} {trade.exit_time}")
                markers.append(
                    {
                        "time": replay_chart_time(exit_at),
                        "position": "aboveBar" if trade.direction == "CE" else "belowBar",
                        "color": "#16a34a" if trade.result == "WIN" else "#dc2626" if trade.result == "LOSS" else "#64748b",
                        "shape": "circle",
                        "text": f"{trade.result or ''} {trade.underlying_points or ''}",
                    }
                )
        if self.open_trade is not None:
            entry_at = pd.to_datetime(f"{self.open_trade.date} {self.open_trade.entry_time}")
            markers.append(
                {
                    "time": replay_chart_time(entry_at),
                    "position": "belowBar" if self.open_trade.direction == "CE" else "aboveBar",
                    "color": "#1546c2",
                    "shape": "arrowUp" if self.open_trade.direction == "CE" else "arrowDown",
                    "text": f"OPEN {self.open_trade.direction}",
                }
            )
        return markers

    def _summary_payload(self) -> dict[str, Any]:
        wins = [trade for trade in self.trades if trade.result == "WIN"]
        losses = [trade for trade in self.trades if trade.result == "LOSS"]
        points = [float(trade.underlying_points or 0) for trade in self.trades]
        r_values = [float(trade.r_multiple or 0) for trade in self.trades]
        total = len(self.trades)
        return {
            "trades": total,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / total * 100, 2) if total else 0,
            "total_points": round(sum(points), 2),
            "average_points": round(sum(points) / total, 2) if total else 0,
            "total_R": round(sum(r_values), 3),
            "open_trade": 1 if self.open_trade else 0,
            "pending_signal": 1 if self.pending_signal else 0,
        }

    def _candles_payload(self, candles: pd.DataFrame) -> list[dict[str, Any]]:
        return [
            {
                "time": replay_chart_time(ts),
                "open": round(float(row.open), 2),
                "high": round(float(row.high), 2),
                "low": round(float(row.low), 2),
                "close": round(float(row.close), 2),
            }
            for ts, row in candles.iterrows()
        ]

    def _signal_payload(self, signal: SignalCandidate) -> dict[str, Any]:
        return {
            "date": signal.date,
            "time": signal.time,
            "direction": signal.direction,
            "setup_type": signal.setup_type,
            "entry_index_price": signal.entry_index_price,
            "sl_index_price": signal.sl_index_price,
            "target_index_price": signal.target_index_price,
            "risk_reward": signal.risk_reward,
            "setup_score": signal.setup_score,
            "reason": " | ".join(signal.notes),
        }

    def _trade_payload(self, trade: PaperTrade) -> dict[str, Any]:
        mfe = float(trade.max_favorable_excursion or 0.0)
        mae = float(trade.max_adverse_excursion or 0.0)
        risk = float(trade.risk_points or 0.0)
        return {
            "date": trade.date,
            "entry_time": trade.entry_time,
            "exit_time": trade.exit_time,
            "direction": trade.direction,
            "setup_type": trade.setup_type,
            "entry_index_price": trade.entry_index_price,
            "sl_index_price": trade.sl_index_price,
            "target_index_price": trade.target_index_price,
            "exit_index_price": trade.exit_index_price,
            "exit_reason": trade.exit_reason,
            "status": trade.status,
            "result": trade.result,
            "points": trade.underlying_points,
            "r_multiple": trade.r_multiple,
            "risk_points": trade.risk_points,
            "reward_points": trade.reward_points,
            "planned_rr": trade.risk_reward,
            "max_favorable_excursion": round(mfe, 2),
            "max_adverse_excursion": round(mae, 2),
            "mfe_r": round(mfe / risk, 3) if risk else 0,
            "mae_r": round(mae / risk, 3) if risk else 0,
            "target_source": trade.features.get("target_source") if isinstance(trade.features, dict) else None,
            "target_name": trade.features.get("target_name") if isinstance(trade.features, dict) else None,
            "setup_score": trade.setup_score,
            "reason": " | ".join(trade.notes),
        }

    @staticmethod
    def _skipped_payload(item: SkippedSignal) -> dict[str, Any]:
        return {
            "date": item.date,
            "time": item.time,
            "direction": item.potential_direction,
            "setup": item.potential_setup,
            "reason": item.skip_reason,
        }

    def _trim_context_days(self, candles: pd.DataFrame) -> pd.DataFrame:
        start = pd.to_datetime(self.start_date).date()
        replay = candles[candles["date"] >= start]
        if replay.empty:
            raise ValueError("Replay needs candles inside the selected date range.")
        previous_dates = sorted({day for day in candles["date"].unique() if day < start})
        keep_previous = set(previous_dates[-self.context_trading_days :]) if self.context_trading_days else set()
        trimmed = candles[(candles["date"].isin(keep_previous)) | (candles["date"] >= start)].copy()
        return trimmed.sort_index()

    def _first_replay_index(self) -> int:
        start = pd.to_datetime(self.start_date).date()
        indexes = [index for index, day in enumerate(self.candles_5m["date"]) if day >= start]
        if not indexes:
            raise ValueError("Replay start date is outside loaded candles.")
        return int(indexes[0])

    @staticmethod
    def _normalize(candles_5m: pd.DataFrame) -> pd.DataFrame:
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
