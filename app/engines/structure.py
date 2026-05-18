from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from app.config import StrategyConfig, config
from app.engines.levels import LevelEngine


class StructureEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg
        self.levels = LevelEngine(cfg)

    def confirmed_swings_until(self, candles: pd.DataFrame, index: int) -> dict[str, list[dict[str, Any]]]:
        rows = candles.iloc[: index + 1]
        return self.levels.detect_swings(rows)

    def last_swing_high(self, candles: pd.DataFrame, index: int) -> dict[str, Any] | None:
        swings = self.confirmed_swings_until(candles, index)["highs"]
        return swings[-1] if swings else None

    def last_swing_low(self, candles: pd.DataFrame, index: int) -> dict[str, Any] | None:
        swings = self.confirmed_swings_until(candles, index)["lows"]
        return swings[-1] if swings else None

    def bos(self, candles: pd.DataFrame, index: int) -> dict[str, Any]:
        rows = candles.reset_index()
        row = rows.iloc[index]
        high = self.last_swing_high(candles, index - 1)
        low = self.last_swing_low(candles, index - 1)
        bullish = high is not None and float(row["close"]) > float(high["price"])
        bearish = low is not None and float(row["close"]) < float(low["price"])
        return {
            "direction": "bullish" if bullish else "bearish" if bearish else None,
            "is_bos": bullish or bearish,
            "broken_level": high if bullish else low if bearish else None,
            "strength": self._strength(row, high if bullish else low if bearish else None, "bullish" if bullish else "bearish" if bearish else None),
        }

    def structure_shift(self, candles: pd.DataFrame, index: int) -> dict[str, Any]:
        base = self.bos(candles, index)
        direction = base.get("direction")
        trend_before = self.trend(candles, index - 1)
        is_structure_break = bool(base.get("is_bos"))
        is_continuation_bos = (
            (direction == "bullish" and trend_before == "up")
            or (direction == "bearish" and trend_before == "down")
        )
        is_choch = (
            (direction == "bullish" and trend_before == "down")
            or (direction == "bearish" and trend_before == "up")
        )
        strength = float(base.get("strength") or 0)
        is_mss = bool(is_structure_break and (is_choch or trend_before == "range") and strength >= self.cfg.mss_min_strength)
        break_type = None
        if is_continuation_bos:
            break_type = "BOS"
        elif is_choch:
            break_type = "MSS" if is_mss else "CHOCH"
        elif is_mss:
            break_type = "MSS"
        elif is_structure_break:
            break_type = "STRUCTURE_BREAK"
        return {
            **base,
            "trend_before": trend_before,
            "is_structure_break": is_structure_break,
            "is_bos": is_continuation_bos,
            "is_choch": is_choch,
            "is_mss": is_mss,
            "break_type": break_type,
        }

    def trend(self, candles: pd.DataFrame, index: int) -> Literal["up", "down", "range"]:
        swings = self.confirmed_swings_until(candles, index)
        highs = swings["highs"][-2:]
        lows = swings["lows"][-2:]
        if len(highs) == 2 and len(lows) == 2:
            if highs[-1]["price"] > highs[-2]["price"] and lows[-1]["price"] > lows[-2]["price"]:
                return "up"
            if highs[-1]["price"] < highs[-2]["price"] and lows[-1]["price"] < lows[-2]["price"]:
                return "down"
        return "range"

    def one_min_confirmation(self, candles_1m: pd.DataFrame, until_time, direction: str, lookback: int = 8) -> bool:
        window = candles_1m[candles_1m.index <= until_time].tail(lookback)
        if len(window) < 4:
            return False
        highs = window["high"].tail(4).to_list()
        lows = window["low"].tail(4).to_list()
        if direction == "CE":
            return highs[-1] > max(highs[:-1]) and lows[-1] > min(lows[:-1])
        return lows[-1] < min(lows[:-1]) and highs[-1] < max(highs[:-1])

    def _strength(self, row: pd.Series, level: dict[str, Any] | None, direction: str | None) -> float:
        if not level or not direction:
            return 0.0
        candle_range = max(float(row["high"] - row["low"]), 0.01)
        if direction == "bullish":
            return round((float(row["close"]) - float(level["price"])) / candle_range, 4)
        return round((float(level["price"]) - float(row["close"])) / candle_range, 4)
