from __future__ import annotations

from typing import Any

import pandas as pd


class OrderBlockEngine:
    def detect(self, candles: pd.DataFrame, index: int, direction: str) -> dict[str, Any] | None:
        rows = candles.reset_index()
        start = max(0, index - 8)
        prior = rows.iloc[start:index]
        if prior.empty:
            return None
        if direction == "CE":
            opposite = prior[prior["close"] < prior["open"]]
            if opposite.empty:
                return None
            candle = opposite.iloc[-1]
            return self._zone(candle, "bullish")
        opposite = prior[prior["close"] > prior["open"]]
        if opposite.empty:
            return None
        candle = opposite.iloc[-1]
        return self._zone(candle, "bearish")

    def was_retested(self, candles: pd.DataFrame, zone: dict[str, Any], after_index: int, before_index: int) -> bool:
        rows = candles.reset_index().iloc[after_index:before_index]
        if rows.empty:
            return False
        return bool(((rows["low"] <= zone["high"]) & (rows["high"] >= zone["low"])).any())

    def is_retest(self, row: pd.Series, zone: dict[str, Any] | None) -> bool:
        if not zone:
            return False
        return bool(row["low"] <= zone["high"] and row["high"] >= zone["low"])

    def _zone(self, candle: pd.Series, direction: str) -> dict[str, Any]:
        return {
            "direction": direction,
            "time": str(candle["datetime"]),
            "low": float(candle["low"]),
            "high": float(candle["high"]),
            "size": float(candle["high"] - candle["low"]),
        }
