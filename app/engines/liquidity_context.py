from __future__ import annotations

from typing import Any

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import LevelSet


class LiquidityContextEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg

    def context(
        self,
        rows: pd.DataFrame,
        index: int,
        levels: LevelSet,
        direction: str,
        entry_price: float,
        target_price: float,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        range_info = self._dealing_range(levels, entry_price)
        setup_level = self._setup_level(extra)
        target_level = {
            "name": "TARGET",
            "price": float(target_price),
            "side": "buy_side" if direction == "CE" else "sell_side",
        }
        inducement = self._inducement(rows, index, levels, direction, range_info)
        return {
            "range": range_info,
            "setup_level": self._classify_level(setup_level, range_info) if setup_level else None,
            "target_level": self._classify_level(target_level, range_info),
            "inducement": inducement,
        }

    def _setup_level(self, extra: dict[str, Any]) -> dict[str, Any] | None:
        if extra.get("sweep"):
            sweep = extra["sweep"]
            return {
                "name": sweep.get("level"),
                "price": sweep.get("price"),
                "side": "sell_side" if sweep.get("direction") == "CE" else "buy_side",
            }
        if extra.get("target_hit"):
            hit = extra["target_hit"]
            return {
                "name": hit.get("level"),
                "price": hit.get("price"),
                "side": "buy_side" if hit.get("direction") == "PE" else "sell_side",
            }
        if extra.get("break_level") is not None:
            return {
                "name": "BREAK_LEVEL",
                "price": extra.get("break_level"),
                "side": extra.get("break_side") or "unknown",
            }
        if extra.get("order_block"):
            ob = extra["order_block"]
            midpoint = (float(ob["low"]) + float(ob["high"])) / 2
            return {"name": "ORDER_BLOCK", "price": midpoint, "side": "internal"}
        return None

    def _classify_level(self, level: dict[str, Any], range_info: dict[str, Any]) -> dict[str, Any]:
        price = self._to_float(level.get("price"))
        name = str(level.get("name") or "UNKNOWN")
        side = str(level.get("side") or self._side_from_price(price, range_info))
        classification = "unknown"
        if price is not None:
            low = float(range_info["low"])
            high = float(range_info["high"])
            if price <= low or price >= high:
                classification = "external"
            elif name in {"PDH", "PDL"}:
                classification = "external"
            elif name in {"ORH", "ORL", "SWING_HIGH", "SWING_LOW", "ROUND_NUMBER", "ORDER_BLOCK", "BREAK_LEVEL"}:
                classification = "internal"
            else:
                classification = "internal"
        return {
            "name": name,
            "price": None if price is None else round(price, 2),
            "side": side,
            "classification": classification,
            "range_source": range_info["source"],
        }

    def _inducement(
        self,
        rows: pd.DataFrame,
        index: int,
        levels: LevelSet,
        direction: str,
        range_info: dict[str, Any],
    ) -> dict[str, Any]:
        lookback = max(3, int(self.cfg.inducement_lookback_candles))
        start = max(0, index - lookback)
        window = rows.iloc[start : index + 1]
        candidates = self._internal_levels(levels, range_info, direction)
        if window.empty or not candidates:
            return {"present": False, "reason": "No internal liquidity sweep in lookback"}

        for _, candle in window.iterrows():
            high = float(candle["high"])
            low = float(candle["low"])
            close = float(candle["close"])
            for candidate in candidates:
                price = float(candidate["price"])
                if direction == "CE" and low < price and close > price:
                    return self._inducement_payload(candidate, candle, price - low)
                if direction == "PE" and high > price and close < price:
                    return self._inducement_payload(candidate, candle, high - price)
        return {"present": False, "reason": "No internal liquidity sweep in lookback"}

    def _internal_levels(self, levels: LevelSet, range_info: dict[str, Any], direction: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if direction == "CE":
            if levels.orl is not None:
                out.append({"name": "ORL", "price": float(levels.orl), "side": "sell_side"})
            out.extend({"name": "SWING_LOW", "price": float(swing["price"]), "side": "sell_side"} for swing in levels.swing_lows)
        else:
            if levels.orh is not None:
                out.append({"name": "ORH", "price": float(levels.orh), "side": "buy_side"})
            out.extend({"name": "SWING_HIGH", "price": float(swing["price"]), "side": "buy_side"} for swing in levels.swing_highs)
        return [
            candidate
            for candidate in out
            if self._classify_level(candidate, range_info)["classification"] == "internal"
        ]

    @staticmethod
    def _inducement_payload(candidate: dict[str, Any], candle: pd.Series, depth: float) -> dict[str, Any]:
        return {
            "present": True,
            "level": candidate["name"],
            "price": round(float(candidate["price"]), 2),
            "side": candidate["side"],
            "sweep_time": candle["time"],
            "depth": round(float(depth), 2),
        }

    def _dealing_range(self, levels: LevelSet, fallback_price: float) -> dict[str, Any]:
        latest_high = levels.swing_highs[-1]["price"] if levels.swing_highs else None
        latest_low = levels.swing_lows[-1]["price"] if levels.swing_lows else None
        if latest_high is not None and latest_low is not None and latest_high != latest_low:
            return {
                "high": max(float(latest_high), float(latest_low)),
                "low": min(float(latest_high), float(latest_low)),
                "source": "confirmed_swing_range",
            }
        if levels.day_high is not None and levels.day_low is not None and levels.day_high != levels.day_low:
            return {
                "high": max(float(levels.day_high), float(levels.day_low)),
                "low": min(float(levels.day_high), float(levels.day_low)),
                "source": "day_range",
            }
        if levels.pdh is not None and levels.pdl is not None and levels.pdh != levels.pdl:
            return {
                "high": max(float(levels.pdh), float(levels.pdl)),
                "low": min(float(levels.pdh), float(levels.pdl)),
                "source": "previous_day_range",
            }
        width = max(float(self.cfg.premium_discount_min_range_points), 1.0)
        return {
            "high": round(float(fallback_price) + width / 2, 2),
            "low": round(float(fallback_price) - width / 2, 2),
            "source": "fallback_entry_range",
        }

    @staticmethod
    def _side_from_price(price: float | None, range_info: dict[str, Any]) -> str:
        if price is None:
            return "unknown"
        midpoint = (float(range_info["low"]) + float(range_info["high"])) / 2
        return "buy_side" if price >= midpoint else "sell_side"

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
