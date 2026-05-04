from __future__ import annotations

from typing import Any

import pandas as pd

from app.domain import LevelSet


class LiquidityEngine:
    def sweeps(self, row: pd.Series, levels: LevelSet) -> list[dict[str, Any]]:
        checks: list[tuple[str, float | None, str]] = [
            ("ORL", levels.orl, "bullish"),
            ("PDL", levels.pdl, "bullish"),
            ("ORH", levels.orh, "bearish"),
            ("PDH", levels.pdh, "bearish"),
        ]
        checks.extend(("SWING_LOW", swing["price"], "bullish") for swing in levels.swing_lows)
        checks.extend(("SWING_HIGH", swing["price"], "bearish") for swing in levels.swing_highs)
        found: list[dict[str, Any]] = []
        for name, price, direction in checks:
            if price is None:
                continue
            if direction == "bullish" and row["low"] < price and row["close"] > price:
                found.append({"level": name, "price": float(price), "direction": "CE", "depth": float(price - row["low"])})
            if direction == "bearish" and row["high"] > price and row["close"] < price:
                found.append({"level": name, "price": float(price), "direction": "PE", "depth": float(row["high"] - price)})
        return found

    def next_target(self, levels: LevelSet, current_price: float, direction: str, round_levels: list[float]) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        raw: list[tuple[str, float | None]] = [
            ("PDH", levels.pdh),
            ("PDL", levels.pdl),
            ("ORH", levels.orh),
            ("ORL", levels.orl),
            ("DAY_HIGH", levels.day_high),
            ("DAY_LOW", levels.day_low),
        ]
        raw.extend(("SWING_HIGH", swing["price"]) for swing in levels.swing_highs)
        raw.extend(("SWING_LOW", swing["price"]) for swing in levels.swing_lows)
        raw.extend(("ROUND_NUMBER", level) for level in round_levels)
        for name, price in raw:
            if price is None:
                continue
            price = float(price)
            if direction == "CE" and price > current_price:
                candidates.append({"name": name, "price": price, "distance": price - current_price})
            if direction == "PE" and price < current_price:
                candidates.append({"name": name, "price": price, "distance": current_price - price})
        if not candidates:
            return None
        return min(candidates, key=lambda item: item["distance"])
