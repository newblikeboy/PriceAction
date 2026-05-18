from __future__ import annotations

from typing import Any

import pandas as pd

from app.config import StrategyConfig, config
from app.engines.structure import StructureEngine


class HTFBiasEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg
        self.structure = StructureEngine(cfg)

    def context(self, candles_5m: pd.DataFrame, as_of_time) -> dict[str, Any]:
        if candles_5m.empty:
            return self._empty("No 5m candles")
        frame = candles_5m.sort_index()
        as_of = pd.to_datetime(as_of_time)
        available = frame[frame.index <= as_of].copy()
        if available.empty:
            return self._empty("No completed 5m candles as of setup")

        fifteen = self._timeframe_bias(available, "15min", self.cfg.htf_15m_min_bars)
        sixty = self._timeframe_bias(available, "60min", self.cfg.htf_60m_min_bars)
        combined, reason = self._combine(fifteen["bias"], sixty["bias"])
        return {
            "enabled": self.cfg.htf_bias_filter_enabled,
            "bias": combined,
            "reason": reason,
            "as_of": as_of.strftime("%Y-%m-%d %H:%M"),
            "15m": fifteen,
            "60m": sixty,
        }

    def allows(self, direction: str, context: dict[str, Any]) -> bool:
        if not self.cfg.htf_bias_filter_enabled:
            return True
        bias = context.get("bias")
        if bias in {None, "neutral", "mixed"}:
            return self.cfg.htf_bias_allow_neutral
        return (direction == "CE" and bias == "bullish") or (direction == "PE" and bias == "bearish")

    def _timeframe_bias(self, candles: pd.DataFrame, rule: str, min_bars: int) -> dict[str, Any]:
        frame = self._resample(candles, rule)
        if len(frame) < min_bars:
            return {"bias": "neutral", "reason": "insufficient HTF candles", "bars": int(len(frame)), "timeframe": rule}

        index = len(frame) - 1
        trend = self.structure.trend(frame, index)
        close_now = float(frame.iloc[-1]["close"])
        close_then = float(frame.iloc[max(0, index - min_bars + 1)]["close"])
        ema_fast = frame["close"].ewm(span=min(3, len(frame)), adjust=False).mean()
        ema_slow = frame["close"].ewm(span=min(6, len(frame)), adjust=False).mean()
        slope_points = round(close_now - close_then, 2)
        ema_delta = round(float(ema_fast.iloc[-1] - ema_slow.iloc[-1]), 2)

        if trend == "up" or (slope_points > 0 and ema_delta > 0):
            bias = "bullish"
        elif trend == "down" or (slope_points < 0 and ema_delta < 0):
            bias = "bearish"
        else:
            bias = "neutral"
        return {
            "bias": bias,
            "reason": f"trend={trend}, slope={slope_points}, ema_delta={ema_delta}",
            "trend": trend,
            "bars": int(len(frame)),
            "timeframe": rule,
            "last_time": str(frame.index[-1]),
            "last_close": round(close_now, 2),
            "slope_points": slope_points,
            "ema_delta": ema_delta,
        }

    @staticmethod
    def _resample(candles: pd.DataFrame, rule: str) -> pd.DataFrame:
        frame = candles.copy()
        if "volume" not in frame.columns:
            frame["volume"] = 0
        out = frame.resample(rule, label="left", closed="left").agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        out = out.dropna(subset=["open", "high", "low", "close"])
        out["date"] = out.index.date
        out["time"] = out.index.strftime("%H:%M")
        return out

    @staticmethod
    def _combine(fifteen: str, sixty: str) -> tuple[str, str]:
        if fifteen == sixty and fifteen in {"bullish", "bearish"}:
            return fifteen, "15m and 60m aligned"
        if fifteen in {"bullish", "bearish"} and sixty == "neutral":
            return fifteen, "15m directional, 60m neutral"
        if sixty in {"bullish", "bearish"} and fifteen == "neutral":
            return sixty, "60m directional, 15m neutral"
        if {fifteen, sixty} == {"bullish", "bearish"}:
            return "mixed", "15m and 60m conflict"
        return "neutral", "HTF bias neutral"

    @staticmethod
    def _empty(reason: str) -> dict[str, Any]:
        return {
            "enabled": config.htf_bias_filter_enabled,
            "bias": "neutral",
            "reason": reason,
            "15m": {"bias": "neutral", "reason": reason, "bars": 0, "timeframe": "15min"},
            "60m": {"bias": "neutral", "reason": reason, "bars": 0, "timeframe": "60min"},
        }
