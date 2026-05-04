from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import LevelSet, SignalCandidate, SkippedSignal
from app.engines.displacement import DisplacementEngine
from app.engines.liquidity import LiquidityEngine
from app.engines.order_block import OrderBlockEngine
from app.engines.risk import RiskEngine
from app.engines.structure import StructureEngine


class SignalEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg
        self.displacement = DisplacementEngine(cfg)
        self.structure = StructureEngine(cfg)
        self.liquidity = LiquidityEngine()
        self.order_blocks = OrderBlockEngine()
        self.risk = RiskEngine(cfg)
        self.failed_levels: dict[str, int] = defaultdict(int)

    def generate_for_day(
        self,
        candles_5m: pd.DataFrame,
        candles_1m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        rows = candles_5m[candles_5m["date"] == trading_date].reset_index()
        signals: list[SignalCandidate] = []
        skipped: list[SkippedSignal] = []
        if levels.orh is None or levels.orl is None:
            return signals, [SkippedSignal(str(trading_date), "09:30", "CE", "OPENING_RANGE", "Opening range unavailable", {})]

        for i, row in rows.iterrows():
            if row["time"] < self.cfg.opening_range_end:
                continue
            if row["time"] > self.cfg.no_fresh_trade_after:
                continue
            if levels.orl < row["close"] < levels.orh:
                skipped.append(self._skip(row, "CE", "RANGE_CONTEXT", "Price is inside ORH and ORL after 09:30", {"orh": levels.orh, "orl": levels.orl}))
                continue

            signals.extend(self._opening_range_candidates(rows, i, row, candles_1m, levels, skipped))
            signals.extend(self._sweep_candidates(rows, i, row, candles_1m, levels, skipped))
            signals.extend(self._ob_retest_candidates(rows, i, row, candles_1m, levels, skipped))
        return signals, skipped

    def _opening_range_candidates(self, rows, i: int, row: pd.Series, candles_1m: pd.DataFrame, levels: LevelSet, skipped: list[SkippedSignal]) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        if row["high"] > levels.orh and row["close"] <= levels.orh:
            skipped.append(self._skip(row, "CE", "ORH_BREAKOUT_CONTINUATION", "Breakout is wick-only", {"orh": levels.orh}))
        if row["low"] < levels.orl and row["close"] >= levels.orl:
            skipped.append(self._skip(row, "PE", "ORL_BREAKDOWN_CONTINUATION", "Breakdown is wick-only", {"orl": levels.orl}))

        if row["close"] > levels.orh:
            signal = self._build_signal("CE", "ORH_BREAKOUT_CONTINUATION", rows, i, row, candles_1m, levels, [float(row["low"])], {"break_level": levels.orh})
            self._append_or_skip(signal, skipped, row, "CE", "ORH_BREAKOUT_CONTINUATION", out)
        if row["close"] < levels.orl:
            signal = self._build_signal("PE", "ORL_BREAKDOWN_CONTINUATION", rows, i, row, candles_1m, levels, [float(row["high"])], {"break_level": levels.orl})
            self._append_or_skip(signal, skipped, row, "PE", "ORL_BREAKDOWN_CONTINUATION", out)
        return out

    def _sweep_candidates(self, rows, i: int, row: pd.Series, candles_1m: pd.DataFrame, levels: LevelSet, skipped: list[SkippedSignal]) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        for sweep in self.liquidity.sweeps(row, levels):
            direction = sweep["direction"]
            setup = "BULLISH_LIQUIDITY_SWEEP_REVERSAL" if direction == "CE" else "BEARISH_LIQUIDITY_SWEEP_REVERSAL"
            next_rows = rows.iloc[i : min(len(rows), i + 6)]
            disp_index = None
            expected = "bullish" if direction == "CE" else "bearish"
            for j in next_rows.index:
                disp = self.displacement.analyze(rows.set_index("datetime"), int(j))
                bos = self.structure.bos(rows.set_index("datetime"), int(j))
                candle = rows.iloc[int(j)]
                directional_close = candle["close"] > candle["open"] if direction == "CE" else candle["close"] < candle["open"]
                if disp["direction"] == expected or bos["direction"] == expected or directional_close:
                    disp_index = int(j)
                    break
            if disp_index is None:
                skipped.append(self._skip(row, direction, setup, "No directional confirmation after sweep", sweep))
                continue
            ob = self.order_blocks.detect(rows.set_index("datetime"), disp_index, direction)
            retest_row = rows.iloc[disp_index]
            invalidation = [float(row["low"] if direction == "CE" else row["high"])]
            if ob:
                invalidation.append(float(ob["low"] if direction == "CE" else ob["high"]))
            signal = self._build_signal(direction, setup, rows, disp_index, retest_row, candles_1m, levels, invalidation, {"sweep": sweep, "order_block": ob})
            self._append_or_skip(signal, skipped, retest_row, direction, setup, out)
        return out

    def _ob_retest_candidates(self, rows, i: int, row: pd.Series, candles_1m: pd.DataFrame, levels: LevelSet, skipped: list[SkippedSignal]) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        bos = self.structure.bos(rows.set_index("datetime"), i)
        if not bos["is_bos"]:
            return out
        direction = "CE" if bos["direction"] == "bullish" else "PE"
        disp = self.displacement.analyze(rows.set_index("datetime"), i)
        if disp["direction"] != bos["direction"]:
            return out
        ob = self.order_blocks.detect(rows.set_index("datetime"), i, direction)
        if not ob:
            return out
        next_index = min(i + 1, len(rows) - 1)
        next_row = rows.iloc[next_index]
        if not self.order_blocks.is_retest(next_row, ob):
            return out
        setup = "BULLISH_OB_RETEST_CONTINUATION" if direction == "CE" else "BEARISH_OB_RETEST_CONTINUATION"
        invalidation = [float(ob["low"] if direction == "CE" else ob["high"]), float(next_row["low"] if direction == "CE" else next_row["high"])]
        signal = self._build_signal(direction, setup, rows, next_index, next_row, candles_1m, levels, invalidation, {"order_block": ob})
        self._append_or_skip(signal, skipped, next_row, direction, setup, out)
        return out

    def _build_signal(
        self,
        direction: str,
        setup: str,
        rows: pd.DataFrame,
        i: int,
        row: pd.Series,
        candles_1m: pd.DataFrame,
        levels: LevelSet,
        invalidation_points: list[float],
        extra: dict[str, Any],
    ) -> tuple[SignalCandidate | None, str | None, dict[str, Any]]:
        candles = rows.set_index("datetime").sort_index()
        disp = self.displacement.analyze(candles, i)
        expected = "bullish" if direction == "CE" else "bearish"
        bos = self.structure.bos(candles, i)
        one_min = self.structure.one_min_confirmation(candles_1m, row["datetime"], direction)
        risk_plan, reason = self.risk.build_plan(row, levels, direction, invalidation_points)
        if reason:
            return None, reason, extra
        has_directional_displacement = disp.get("direction") == expected
        has_directional_bos = bos.get("direction") == expected
        has_directional_body = row["close"] > row["open"] if direction == "CE" else row["close"] < row["open"]
        if not (has_directional_displacement or has_directional_bos or one_min):
            return None, "No confirmation: needs displacement, BOS, or 1m structure", {"displacement": disp, "bos": bos, "one_min_confirmation": one_min, **extra}
        if ("ORH_BREAKOUT" in setup or "ORL_BREAKDOWN" in setup) and not (has_directional_body and (has_directional_displacement or one_min)):
            return None, "Breakout lacks candle/1m confirmation", {"displacement": disp, "bos": bos, "one_min_confirmation": one_min, **extra}
        time_quality = self._time_quality(row["time"])
        score = self._score_setup(setup, direction, row, disp, bos, one_min, risk_plan, time_quality)
        if score < self.cfg.min_setup_score:
            return None, f"Setup score below {self.cfg.min_setup_score}", {"score": score, "displacement": disp, "bos": bos, "one_min_confirmation": one_min, **extra}
        features = self._features(row, levels, disp, bos, risk_plan, {**extra, "one_min_confirmation": one_min, "score_model": "soft_confirmation"})
        entry_datetime = pd.to_datetime(row["datetime"]) + pd.Timedelta(minutes=5)
        return SignalCandidate(
            date=str(entry_datetime.date()),
            time=entry_datetime.strftime("%H:%M"),
            symbol=self.cfg.symbol,
            direction=direction,
            setup_type=setup,
            entry_index_price=risk_plan["entry"],
            sl_index_price=risk_plan["sl"],
            target_index_price=risk_plan["target"],
            risk_points=risk_plan["risk_points"],
            reward_points=risk_plan["reward_points"],
            risk_reward=risk_plan["risk_reward"],
            setup_score=score,
            features=features,
            notes=[f"Target liquidity: {risk_plan['target_name']}"],
        ), None, features

    def _append_or_skip(self, result, skipped, row, direction, setup, out) -> None:
        signal, reason, context = result
        if signal:
            out.append(signal)
        elif reason:
            skipped.append(self._skip(row, direction, setup, reason, context))

    def _skip(self, row: pd.Series, direction: str, setup: str, reason: str, context: dict[str, Any]) -> SkippedSignal:
        return SkippedSignal(str(row["date"]), row["time"], direction, setup, reason, context)

    def _time_quality(self, hhmm: str) -> int:
        if self.cfg.opening_range_end <= hhmm <= self.cfg.best_window_end:
            return 5
        if self.cfg.best_window_end < hhmm <= self.cfg.continuation_window_end:
            return 3
        return 1

    def _score_setup(
        self,
        setup: str,
        direction: str,
        row: pd.Series,
        disp: dict[str, Any],
        bos: dict[str, Any],
        one_min: bool,
        risk_plan: dict[str, Any],
        time_quality: int,
    ) -> int:
        expected = "bullish" if direction == "CE" else "bearish"
        score = 20
        if "ORH_BREAKOUT" in setup or "ORL_BREAKDOWN" in setup:
            score += 10
        if "LIQUIDITY_SWEEP" in setup:
            score += 8
        if "OB_RETEST" in setup:
            score += 8

        directional_body = row["close"] > row["open"] if direction == "CE" else row["close"] < row["open"]
        if directional_body:
            score += 8

        if disp.get("direction") == expected:
            score += 18
        else:
            body_pct = float(disp.get("body_pct") or 0)
            range_ratio = float(disp.get("range_ratio") or 0)
            close_position = float(disp.get("close_position") or 0.5)
            close_ok = close_position >= 0.65 if direction == "CE" else close_position <= 0.35
            if body_pct >= 0.45:
                score += 6
            if range_ratio >= 0.8:
                score += 4
            if close_ok:
                score += 4

        if bos.get("direction") == expected:
            score += 15
        elif bos.get("is_bos"):
            score -= 8

        if one_min:
            score += 10

        rr = float(risk_plan.get("risk_reward") or 0)
        if rr >= 3:
            score += 12
        elif rr >= 2:
            score += 9
        elif rr >= self.cfg.minimum_rr:
            score += 6

        risk_points = float(risk_plan.get("risk_points") or 0)
        if 0 < risk_points <= self.cfg.max_entry_sl_points * 0.5:
            score += 5
        elif 0 < risk_points <= self.cfg.max_entry_sl_points:
            score += 2

        score += time_quality
        return max(0, min(100, int(score)))

    def _features(self, row: pd.Series, levels: LevelSet, disp: dict[str, Any], bos: dict[str, Any], risk_plan: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
        return {
            "date": str(row["date"]),
            "time": row["time"],
            "day_type": "unknown",
            "expiry_day": False,
            "ORH": levels.orh,
            "ORL": levels.orl,
            "PDH": levels.pdh,
            "PDL": levels.pdl,
            "distance_from_ORH": None if levels.orh is None else round(float(row["close"] - levels.orh), 2),
            "distance_from_ORL": None if levels.orl is None else round(float(row["close"] - levels.orl), 2),
            "sweep_level": extra.get("sweep", {}).get("level"),
            "sweep_depth": extra.get("sweep", {}).get("depth"),
            "displacement_candle_body_percentage": disp.get("body_pct"),
            "displacement_range_vs_recent_average": disp.get("range_ratio"),
            "BOS_strength": bos.get("strength"),
            "BOS_direction": bos.get("direction"),
            "is_BOS": bos.get("is_bos"),
            "one_min_confirmation": extra.get("one_min_confirmation"),
            "OB_size": None if not extra.get("order_block") else extra["order_block"].get("size"),
            "retest_depth": None,
            "entry_price": risk_plan["entry"],
            "SL_price": risk_plan["sl"],
            "target_price": risk_plan["target"],
            "RR": risk_plan["risk_reward"],
            "reason_for_entry": extra,
        }
