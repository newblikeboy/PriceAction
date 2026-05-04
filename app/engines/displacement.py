from __future__ import annotations

from typing import Literal

import pandas as pd

from app.config import StrategyConfig, config


class DisplacementEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg

    def analyze(self, candles: pd.DataFrame, index: int) -> dict:
        rows = candles.reset_index()
        if index < 0 or index >= len(rows):
            return {"direction": None, "is_displacement": False}
        row = rows.iloc[index]
        candle_range = float(row["high"] - row["low"])
        body = abs(float(row["close"] - row["open"]))
        body_pct = body / candle_range if candle_range > 0 else 0.0
        lookback = rows.iloc[max(0, index - self.cfg.displacement_avg_lookback) : index]
        avg_range = float((lookback["high"] - lookback["low"]).mean()) if not lookback.empty else candle_range
        range_ratio = candle_range / avg_range if avg_range > 0 else 1.0
        close_position = (float(row["close"]) - float(row["low"])) / candle_range if candle_range > 0 else 0.5

        bullish = (
            row["close"] > row["open"]
            and body_pct >= self.cfg.displacement_body_pct
            and range_ratio >= self.cfg.displacement_range_multiplier
            and close_position >= (1 - self.cfg.close_near_extreme_pct)
        )
        bearish = (
            row["close"] < row["open"]
            and body_pct >= self.cfg.displacement_body_pct
            and range_ratio >= self.cfg.displacement_range_multiplier
            and close_position <= self.cfg.close_near_extreme_pct
        )
        direction: Literal["bullish", "bearish"] | None = "bullish" if bullish else "bearish" if bearish else None
        return {
            "direction": direction,
            "is_displacement": bool(direction),
            "body_pct": round(body_pct, 4),
            "range_ratio": round(range_ratio, 4),
            "close_position": round(close_position, 4),
            "range": round(candle_range, 4),
        }
