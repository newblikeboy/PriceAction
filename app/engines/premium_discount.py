from __future__ import annotations

from typing import Any

from app.config import StrategyConfig, config
from app.domain import LevelSet


class PremiumDiscountEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg

    def context(self, levels: LevelSet, price: float) -> dict[str, Any]:
        range_info = self._dealing_range(levels)
        if not range_info:
            return {"valid": False, "zone": "unknown", "reason": "No valid dealing range"}

        low = float(range_info["low"])
        high = float(range_info["high"])
        width = high - low
        if width < self.cfg.premium_discount_min_range_points:
            return {
                "valid": False,
                "zone": "unknown",
                "reason": "Dealing range too narrow",
                **range_info,
                "range_points": round(width, 2),
            }

        position = (float(price) - low) / width
        half_band = self.cfg.premium_discount_equilibrium_band_pct / 2
        discount_cutoff = 0.5 - half_band
        premium_cutoff = 0.5 + half_band
        if position <= discount_cutoff:
            zone = "discount"
        elif position >= premium_cutoff:
            zone = "premium"
        else:
            zone = "equilibrium"
        return {
            "valid": True,
            "zone": zone,
            "position": round(position, 4),
            "low": round(low, 2),
            "high": round(high, 2),
            "midpoint": round((low + high) / 2, 2),
            "range_points": round(width, 2),
            "source": range_info["source"],
            "discount_cutoff": round(discount_cutoff, 4),
            "premium_cutoff": round(premium_cutoff, 4),
        }

    def allows(self, direction: str, context: dict[str, Any]) -> bool:
        if not self.cfg.premium_discount_filter_enabled:
            return True
        if not context.get("valid"):
            return True
        zone = context.get("zone")
        if zone == "equilibrium":
            return self.cfg.premium_discount_allow_equilibrium
        if direction == "CE":
            return zone == "discount"
        return zone == "premium"

    @staticmethod
    def _dealing_range(levels: LevelSet) -> dict[str, Any] | None:
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
        return None
