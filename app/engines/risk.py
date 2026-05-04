from __future__ import annotations

from typing import Any

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import LevelSet
from app.engines.levels import LevelEngine
from app.engines.liquidity import LiquidityEngine


class RiskEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg
        self.level_engine = LevelEngine(cfg)
        self.liquidity = LiquidityEngine()

    def build_plan(
        self,
        row: pd.Series,
        levels: LevelSet,
        direction: str,
        invalidation_points: list[float],
    ) -> tuple[dict[str, Any] | None, str | None]:
        entry = float(row["close"]) if self.cfg.entry_mode == "close" else float(row["high"] if direction == "CE" else row["low"])
        if not invalidation_points:
            return None, "No clear candle-based invalidation point"

        if direction == "CE":
            sl = min(invalidation_points) - self.cfg.sl_buffer_points
            risk = entry - sl
        else:
            sl = max(invalidation_points) + self.cfg.sl_buffer_points
            risk = sl - entry
        if risk <= 0:
            return None, "Invalid candle-based SL"
        if risk > self.cfg.max_entry_sl_points:
            return None, "Entry too far from SL candle"

        target = self.liquidity.next_target(levels, entry, direction, self.level_engine.round_levels(entry))
        if target is None:
            return None, "Target liquidity is already reached"
        reward = abs(float(target["price"]) - entry)
        rr = reward / risk if risk > 0 else 0
        if rr < self.cfg.minimum_rr:
            return None, f"RR below 1:{self.cfg.minimum_rr:g}"
        return {
            "entry": round(entry, 2),
            "sl": round(sl, 2),
            "target": round(float(target["price"]), 2),
            "target_name": target["name"],
            "risk_points": round(risk, 2),
            "reward_points": round(reward, 2),
            "risk_reward": round(rr, 2),
        }, None

    def score(
        self,
        level_quality: int,
        displacement: dict[str, Any],
        bos: dict[str, Any],
        has_entry: bool,
        risk_plan: dict[str, Any],
        time_quality: int,
    ) -> int:
        disp_score = min(
            self.cfg.weight_displacement_max, 
            int(10 + 5 * displacement.get("body_pct", 0) + 4 * displacement.get("range_ratio", 0))
        )
        bos_score = self.cfg.weight_bos if bos.get("is_bos") else 0
        entry_score = self.cfg.weight_entry if has_entry else 0
        sl_score = self.cfg.weight_sl if risk_plan.get("risk_points", 0) > 0 else 0
        rr = risk_plan.get("risk_reward", 0)
        rr_score = 10 if rr >= 3 else 8 if rr >= 2.5 else 6 if rr >= 2 else 0
        return min(100, level_quality + disp_score + bos_score + entry_score + sl_score + rr_score + time_quality)
