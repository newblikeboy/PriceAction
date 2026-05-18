from __future__ import annotations

from typing import Any

import pandas as pd

from app.config import StrategyConfig, config


class FairValueGapEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg

    def detect_at(self, candles: pd.DataFrame, index: int) -> dict[str, Any] | None:
        rows = candles.reset_index()
        if index < 2 or index >= len(rows):
            return None
        first = rows.iloc[index - 2]
        middle = rows.iloc[index - 1]
        third = rows.iloc[index]
        first_high = float(first["high"])
        first_low = float(first["low"])
        third_high = float(third["high"])
        third_low = float(third["low"])

        if third_low > first_high:
            return self._gap("bullish", first_high, third_low, first, middle, third, index)
        if third_high < first_low:
            return self._gap("bearish", third_high, first_low, first, middle, third, index)
        return None

    def context(self, candles: pd.DataFrame, index: int, trade_direction: str) -> dict[str, Any]:
        expected = "bullish" if trade_direction == "CE" else "bearish"
        rows = candles.reset_index()
        if rows.empty:
            return {"present": False}

        candidates: list[dict[str, Any]] = []
        start = max(2, index - self.cfg.fvg_lookback_candles + 1)
        for gap_index in range(start, index + 1):
            gap = self.detect_at(candles, gap_index)
            if not gap or gap["direction"] != expected:
                continue
            mitigated = self._is_fully_mitigated(rows, gap, gap_index + 1, index)
            gap = {
                **gap,
                "age_candles": int(index - gap_index),
                "fully_mitigated": mitigated,
                "entry_candle_touches": self._row_touches_gap(rows.iloc[index], gap),
            }
            candidates.append(gap)

        unmitigated = [gap for gap in candidates if not gap["fully_mitigated"]]
        selected = unmitigated[-1] if unmitigated else candidates[-1] if candidates else None
        if selected is None:
            return {"present": False}
        return {"present": True, **selected}

    def _gap(
        self,
        direction: str,
        low: float,
        high: float,
        first: pd.Series,
        middle: pd.Series,
        third: pd.Series,
        index: int,
    ) -> dict[str, Any] | None:
        size = round(float(high - low), 2)
        if size < self.cfg.fvg_min_points:
            return None
        midpoint = round(float((low + high) / 2), 2)
        return {
            "direction": direction,
            "index": int(index),
            "start_time": str(first["datetime"]),
            "middle_time": str(middle["datetime"]),
            "end_time": str(third["datetime"]),
            "low": round(float(low), 2),
            "high": round(float(high), 2),
            "midpoint": midpoint,
            "size": size,
        }

    @staticmethod
    def _row_touches_gap(row: pd.Series, gap: dict[str, Any]) -> bool:
        return bool(float(row["low"]) <= float(gap["high"]) and float(row["high"]) >= float(gap["low"]))

    def _is_fully_mitigated(self, rows: pd.DataFrame, gap: dict[str, Any], start: int, end: int) -> bool:
        if start > end:
            return False
        future = rows.iloc[start : end + 1]
        if future.empty:
            return False
        if gap["direction"] == "bullish":
            return bool((future["low"] <= float(gap["low"])).any())
        return bool((future["high"] >= float(gap["high"])).any())
