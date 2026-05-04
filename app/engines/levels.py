from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import LevelSet


class LevelEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg

    def calculate(self, candles_5m: pd.DataFrame, trading_date: date) -> LevelSet:
        today = candles_5m[candles_5m["date"] == trading_date]
        previous = candles_5m[candles_5m["date"] < trading_date]
        previous_session = pd.DataFrame()
        if not previous.empty:
            previous_date = sorted(previous["date"].unique())[-1]
            previous_session = previous[previous["date"] == previous_date]
        pdh = float(previous_session["high"].max()) if not previous_session.empty else None
        pdl = float(previous_session["low"].min()) if not previous_session.empty else None
        pdc = float(previous_session.iloc[-1]["close"]) if not previous_session.empty else None
        opening = today[
            (today["time"] >= self.cfg.opening_range_start)
            & (today["time"] < self.cfg.opening_range_end)
        ]
        orh = float(opening["high"].max()) if not opening.empty else None
        orl = float(opening["low"].min()) if not opening.empty else None
        swings = self.detect_swings(today)
        return LevelSet(
            trading_date=trading_date,
            pdh=pdh,
            pdl=pdl,
            pdc=pdc,
            orh=orh,
            orl=orl,
            swing_highs=swings["highs"],
            swing_lows=swings["lows"],
            day_high=float(today["high"].max()) if not today.empty else None,
            day_low=float(today["low"].min()) if not today.empty else None,
        )

    def detect_swings(self, candles: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
        highs: list[dict[str, Any]] = []
        lows: list[dict[str, Any]] = []
        left = self.cfg.swing_left
        right = self.cfg.swing_right
        rows = candles.reset_index()
        for i in range(left, len(rows) - right):
            row = rows.iloc[i]
            left_rows = rows.iloc[i - left : i]
            right_rows = rows.iloc[i + 1 : i + right + 1]
            if row["high"] > left_rows["high"].max() and row["high"] > right_rows["high"].max():
                highs.append({"time": row["datetime"], "price": float(row["high"]), "index": int(i)})
            if row["low"] < left_rows["low"].min() and row["low"] < right_rows["low"].min():
                lows.append({"time": row["datetime"], "price": float(row["low"]), "index": int(i)})
        return {"highs": highs, "lows": lows}

    def round_levels(self, price: float, count: int = 3) -> list[float]:
        step = self.cfg.round_number_step
        base = round(price / step) * step
        return [float(base + (i * step)) for i in range(-count, count + 1)]

    def major_levels(self, levels: LevelSet, current_price: float) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for name, price in [
            ("PDH", levels.pdh),
            ("PDL", levels.pdl),
            ("PDC", levels.pdc),
            ("ORH", levels.orh),
            ("ORL", levels.orl),
            ("DAY_HIGH", levels.day_high),
            ("DAY_LOW", levels.day_low),
        ]:
            if price is not None:
                out.append({"name": name, "price": float(price)})
        for swing in levels.swing_highs:
            out.append({"name": "SWING_HIGH", "price": swing["price"], "time": str(swing["time"])})
        for swing in levels.swing_lows:
            out.append({"name": "SWING_LOW", "price": swing["price"], "time": str(swing["time"])})
        for level in self.round_levels(current_price):
            out.append({"name": "ROUND_NUMBER", "price": level})
        return out
