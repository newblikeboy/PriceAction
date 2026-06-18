from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

from app.config import StrategyConfig, config
from app.engines.levels import LevelEngine


# Anchor zones: only show well-scored, non-broken zones; cap at top N
_ANCHOR_MIN_SCORE: float = 65.0
_MAX_ANCHOR_ZONES: int = 8

# Intraday zones: wait for ATR to stabilise, then refresh every hour not every candle
_MIN_INTRADAY_CANDLES: int = 24       # 2 hours of 5m data before first compute
_INTRADAY_RECOMPUTE_EVERY: int = 12   # recompute checkpoint every 12 candles (1 hour)
_INTRADAY_MIN_SCORE: float = 60.0
_MAX_INTRADAY_ZONES: int = 6


def zone_chart_time(value: Any) -> int:
    dt = pd.to_datetime(value)
    utc = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=ZoneInfo("UTC"))
    return int(utc.timestamp())


@dataclass
class ZoneDetectionSession:
    symbol: str
    start_date: str
    end_date: str
    candles_5m: pd.DataFrame
    cfg: StrategyConfig = field(default_factory=lambda: config)
    session_id: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        self.candles_5m = self._normalize(self.candles_5m)
        if self.candles_5m.empty:
            raise ValueError("Zone Detection needs at least one 5m candle.")
        self.levels = LevelEngine(self.cfg)
        self._active_date: date = pd.to_datetime(self.end_date).date()

        active_mask = self.candles_5m["date"] == self._active_date
        self._active_indices: list[Any] = list(self.candles_5m.index[active_mask])

        self._current: int = 0
        self._anchor_cache: list[dict[str, Any]] | None = None
        self._intraday_cache: dict[int, list[dict[str, Any]]] = {}

    @property
    def total_active_candles(self) -> int:
        return len(self._active_indices)

    def reset(self) -> dict[str, Any]:
        self._current = 0
        self._intraday_cache.clear()
        return self.payload()

    def next(self, count: int = 1) -> dict[str, Any]:
        count = max(1, int(count or 1))
        prev = self._current
        self._current = min(self._current + count, self.total_active_candles)
        return self.payload(prev_index=prev)

    def previous(self) -> dict[str, Any]:
        self._current = max(0, self._current - 1)
        return self.payload()

    def payload(self, *, initial_load: bool = False, prev_index: int | None = None) -> dict[str, Any]:
        anchor_zones = self._get_anchor_zones()
        intraday_zones = self._get_intraday_zones()
        zones = self._merge_zones(anchor_zones, intraday_zones)

        anchor_rows = self.candles_5m[self.candles_5m["date"] < self._active_date]
        current_price: float | None = None
        current_time: str | None = None
        if self._current > 0 and self._active_indices:
            last_ts = self._active_indices[self._current - 1]
            current_price = round(float(self.candles_5m.at[last_ts, "close"]), 2)
            current_time = str(last_ts)
        elif not anchor_rows.empty:
            current_price = round(float(anchor_rows.iloc[-1]["close"]), 2)

        result: dict[str, Any] = {
            "session_id": self.session_id,
            "symbol": self.symbol,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "active_date": str(self._active_date),
            "current_candle_index": self._current,
            "prev_index": prev_index,
            "total_active_candles": self.total_active_candles,
            "is_done": self._current >= self.total_active_candles,
            "current_price": current_price,
            "current_time": current_time,
            "zones": zones,
            "anchor_zone_count": len(anchor_zones),
            "intraday_zone_count": len(intraday_zones),
        }

        if initial_load:
            result["anchor_candles"] = self._candles_payload(anchor_rows)
            if self._active_indices:
                active_rows = self.candles_5m.loc[self._active_indices]
                result["active_candles"] = self._candles_payload(active_rows)
            else:
                result["active_candles"] = []

        return result

    def _get_anchor_zones(self) -> list[dict[str, Any]]:
        if self._anchor_cache is not None:
            return self._anchor_cache
        rows = self.candles_5m[self.candles_5m["date"] < self._active_date]
        history_days = int(getattr(self.cfg, "smart_trade_zone_history_days", 0) or 0)
        if history_days > 0 and not rows.empty:
            previous_dates = sorted({day for day in rows["date"].unique() if day < self._active_date})
            rows = rows[rows["date"].isin(set(previous_dates[-history_days:]))]
        if rows.empty:
            self._anchor_cache = []
            return []
        price = float(rows.iloc[-1]["close"])
        result = self.levels.calculate_smart_zones(rows, current_price=price)
        filtered = [z for z in result.zones if z.score >= _ANCHOR_MIN_SCORE and z.status != "broken"]
        filtered.sort(key=lambda z: z.score, reverse=True)
        self._anchor_cache = [self._zone_dict(z, is_anchor=True) for z in filtered[:_MAX_ANCHOR_ZONES]]
        return self._anchor_cache

    def _get_intraday_zones(self) -> list[dict[str, Any]]:
        if self._current < _MIN_INTRADAY_CANDLES or not self._active_indices:
            return []
        # Snap to hourly checkpoint so zones stay stable between refreshes
        checkpoint = (self._current // _INTRADAY_RECOMPUTE_EVERY) * _INTRADAY_RECOMPUTE_EVERY
        checkpoint = max(checkpoint, _MIN_INTRADAY_CANDLES)
        if checkpoint in self._intraday_cache:
            return self._intraday_cache[checkpoint]
        active_idx = self._active_indices[:checkpoint]
        today = self.candles_5m.loc[active_idx]
        if today.empty:
            self._intraday_cache[checkpoint] = []
            return []
        price = float(today.iloc[-1]["close"])
        result = self.levels.calculate_smart_zones(today, current_price=price)
        filtered = [z for z in result.zones if z.score >= _INTRADAY_MIN_SCORE and z.status != "broken"]
        filtered.sort(key=lambda z: z.score, reverse=True)
        zones = [self._zone_dict(z, is_anchor=False) for z in filtered[:_MAX_INTRADAY_ZONES]]
        self._intraday_cache[checkpoint] = zones
        return zones

    @staticmethod
    def _merge_zones(anchor: list[dict[str, Any]], intraday: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for zone in anchor + intraday:
            zid = str(zone["zone_id"])
            if zid not in seen:
                seen.add(zid)
                out.append(zone)
        return out

    @staticmethod
    def _zone_dict(zone: Any, *, is_anchor: bool) -> dict[str, Any]:
        zone_type = str(zone.zone_type or "")
        bullish_tags = ("demand", "swing_low", "breakout", "gap_up", "equal_lows", "bullish")
        color = "#16a34a" if any(t in zone_type for t in bullish_tags) else "#dc2626"
        enhancers = zone.enhancers or {}
        return {
            "zone_id": zone.zone_id,
            "name": zone_type.upper().replace("_", " "),
            "zone_type": zone_type,
            "low": round(float(zone.low), 2),
            "high": round(float(zone.high), 2),
            "midpoint": round(float(zone.midpoint), 2),
            "score": round(float(zone.score), 1),
            "status": zone.status,
            "touch_count": zone.touch_count,
            "reaction_count": zone.reaction_count,
            "break_count": zone.break_count,
            "freshness_score": round(float(zone.freshness_score), 1),
            "reaction_score": round(float(zone.reaction_score), 1),
            "enhancer_total": round(float(enhancers.get("total_points") or 0), 2),
            "color": color,
            "is_anchor": is_anchor,
            "source": "prior session" if is_anchor else "intraday",
        }

    @staticmethod
    def _candles_payload(df: pd.DataFrame) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for ts, row in df.iterrows():
            out.append({
                "time": zone_chart_time(ts),
                "open": round(float(row["open"]), 2),
                "high": round(float(row["high"]), 2),
                "low": round(float(row["low"]), 2),
                "close": round(float(row["close"]), 2),
            })
        return out

    @staticmethod
    def _normalize(candles: pd.DataFrame) -> pd.DataFrame:
        frame = candles.copy()
        if "datetime" in frame.columns:
            frame["datetime"] = pd.to_datetime(frame["datetime"])
            frame = frame.set_index("datetime")
        frame.index = pd.to_datetime(frame.index)
        frame = frame.sort_index()
        for col in ["open", "high", "low", "close"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame = frame.dropna(subset=["open", "high", "low", "close"])
        if "date" not in frame.columns:
            frame["date"] = frame.index.date
        if "time" not in frame.columns:
            frame["time"] = frame.index.strftime("%H:%M")
        return frame
