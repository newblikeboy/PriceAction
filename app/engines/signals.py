from __future__ import annotations

from collections import defaultdict
from datetime import date
import math
from typing import Any

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import LevelSet, SignalCandidate, SkippedSignal
from app.engines.displacement import DisplacementEngine
from app.engines.fvg import FairValueGapEngine
from app.engines.htf_bias import HTFBiasEngine
from app.engines.liquidity import LiquidityEngine
from app.engines.liquidity_context import LiquidityContextEngine
from app.engines.order_block import OrderBlockEngine
from app.engines.premium_discount import PremiumDiscountEngine
from app.engines.risk import RiskEngine
from app.engines.smart_trades import SmartTradeEngine
from app.engines.structure import StructureEngine
from app.options_pricing import select_option_contract


class SignalEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg
        self.displacement = DisplacementEngine(cfg)
        self.fvg = FairValueGapEngine(cfg)
        self.htf_bias = HTFBiasEngine(cfg)
        self.structure = StructureEngine(cfg)
        self.liquidity = LiquidityEngine()
        self.liquidity_context = LiquidityContextEngine(cfg)
        self.order_blocks = OrderBlockEngine()
        self.premium_discount = PremiumDiscountEngine(cfg)
        self.risk = RiskEngine(cfg)
        self.smart_trades = SmartTradeEngine(cfg)
        self.option_snapshot: dict[str, Any] | None = None
        self.failed_levels: dict[str, int] = defaultdict(int)

    def generate_for_day(
        self,
        candles_5m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        rows = candles_5m[candles_5m["date"] == trading_date].reset_index()
        signals: list[SignalCandidate] = []
        skipped: list[SkippedSignal] = []
        if rows.empty:
            return signals, skipped
        htf_contexts = {} if not self.cfg.legacy_signal_setups_enabled else self._htf_contexts_for_rows(candles_5m, rows)
        self.smart_trades.option_snapshot = self.option_snapshot
        smart_signals, smart_skipped = self.smart_trades.generate_for_day(candles_5m, levels, trading_date, htf_contexts)
        signals.extend(smart_signals)
        skipped.extend(smart_skipped)
        if not self.cfg.legacy_signal_setups_enabled:
            return signals, skipped
        if levels.orh is None or levels.orl is None:
            skipped.append(SkippedSignal(str(trading_date), "09:30", "CE", "OPENING_RANGE", "Opening range unavailable", {}))
            return signals, skipped

        for i, row in rows.iterrows():
            if row["time"] < self.cfg.opening_range_end:
                continue
            if row["time"] > self.cfg.no_fresh_trade_after:
                continue
            as_of_levels = self._levels_as_of(levels, rows, i)
            if as_of_levels.orl < row["close"] < as_of_levels.orh:
                skipped.append(self._skip(row, "CE", "RANGE_CONTEXT", "Price is inside ORH and ORL after 09:30", {"orh": as_of_levels.orh, "orl": as_of_levels.orl}))
                continue

            signals.extend(self._opening_range_candidates(rows, i, row, as_of_levels, htf_contexts, skipped))
            signals.extend(self._sweep_candidates(rows, i, row, as_of_levels, htf_contexts, skipped))
            signals.extend(self._target_reversal_candidates(rows, i, row, as_of_levels, htf_contexts, skipped))
            signals.extend(self._ob_retest_candidates(rows, i, row, as_of_levels, htf_contexts, skipped))
        return signals, skipped

    def generate_for_candle(
        self,
        candles_5m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
        candle_time,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        if not self.cfg.legacy_signal_setups_enabled:
            self.smart_trades.option_snapshot = self.option_snapshot
            return self.smart_trades.generate_for_candle(candles_5m, levels, trading_date, candle_time)
        signals, skipped = self.generate_for_day(candles_5m, levels, trading_date)
        source_time = pd.to_datetime(candle_time).strftime("%H:%M")
        return (
            [signal for signal in signals if signal.features.get("time") == source_time],
            [item for item in skipped if item.time == source_time],
        )

    def _levels_as_of(self, levels: LevelSet, rows: pd.DataFrame, index: int) -> LevelSet:
        history = rows.iloc[: index + 1].copy()
        return LevelSet(
            trading_date=levels.trading_date,
            pdh=levels.pdh,
            pdl=levels.pdl,
            pdc=levels.pdc,
            orh=levels.orh,
            orl=levels.orl,
            swing_highs=self.structure.confirmed_swings_until(rows.set_index("datetime"), index)["highs"],
            swing_lows=self.structure.confirmed_swings_until(rows.set_index("datetime"), index)["lows"],
            day_high=float(history["high"].max()) if not history.empty else None,
            day_low=float(history["low"].min()) if not history.empty else None,
        )

    def _htf_contexts_for_rows(self, candles_5m: pd.DataFrame, rows: pd.DataFrame) -> dict[int, dict[str, Any]]:
        htf_frame = candles_5m
        if "datetime" in htf_frame.columns and not isinstance(htf_frame.index, pd.DatetimeIndex):
            htf_frame = htf_frame.copy()
            htf_frame["datetime"] = pd.to_datetime(htf_frame["datetime"])
            htf_frame = htf_frame.set_index("datetime")
        return {
            int(i): self.htf_bias.context(htf_frame, pd.to_datetime(row["datetime"]))
            for i, row in rows.iterrows()
        }

    def _opening_range_candidates(self, rows, i: int, row: pd.Series, levels: LevelSet, htf_contexts: dict[int, dict[str, Any]], skipped: list[SkippedSignal]) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        if row["high"] > levels.orh and row["close"] <= levels.orh:
            skipped.append(self._skip(row, "CE", "ORH_BREAKOUT_CONTINUATION", "Breakout is wick-only", {"orh": levels.orh}))
        if row["low"] < levels.orl and row["close"] >= levels.orl:
            skipped.append(self._skip(row, "PE", "ORL_BREAKDOWN_CONTINUATION", "Breakdown is wick-only", {"orl": levels.orl}))

        if row["close"] > levels.orh:
            signal = self._build_signal("CE", "ORH_BREAKOUT_CONTINUATION", rows, i, row, levels, [float(row["low"])], {"break_level": levels.orh, "break_side": "buy_side", "htf_bias": htf_contexts.get(int(i), {})})
            self._append_or_skip(signal, skipped, row, "CE", "ORH_BREAKOUT_CONTINUATION", out)
        if row["close"] < levels.orl:
            signal = self._build_signal("PE", "ORL_BREAKDOWN_CONTINUATION", rows, i, row, levels, [float(row["high"])], {"break_level": levels.orl, "break_side": "sell_side", "htf_bias": htf_contexts.get(int(i), {})})
            self._append_or_skip(signal, skipped, row, "PE", "ORL_BREAKDOWN_CONTINUATION", out)
        return out

    def _sweep_candidates(self, rows, i: int, row: pd.Series, levels: LevelSet, htf_contexts: dict[int, dict[str, Any]], skipped: list[SkippedSignal]) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        for sweep in self.liquidity.sweeps(row, levels):
            direction = sweep["direction"]
            setup = "BULLISH_LIQUIDITY_SWEEP_REVERSAL" if direction == "CE" else "BEARISH_LIQUIDITY_SWEEP_REVERSAL"
            sweep_close = pd.to_datetime(row["datetime"]) + pd.Timedelta(minutes=5)
            trigger_index = self._first_five_min_confirmation_after(rows, i, direction)
            sweep_context = {
                **sweep,
                "sweep_candle_time": row["time"],
                "sweep_close_time": sweep_close.strftime("%H:%M"),
            }
            if trigger_index is None:
                skipped.append(self._skip(row, direction, setup, "No 5m confirmation after sweep", sweep_context))
                continue
            ob = self.order_blocks.detect(rows.set_index("datetime"), i, direction)
            trigger_row = rows.iloc[trigger_index]
            invalidation = [float(row["low"] if direction == "CE" else row["high"])]
            if ob:
                invalidation.append(float(ob["low"] if direction == "CE" else ob["high"]))
            signal = self._build_signal(
                direction,
                setup,
                rows,
                trigger_index,
                trigger_row,
                levels,
                invalidation,
                {
                    "sweep": sweep_context,
                    "order_block": ob,
                    "trigger_timeframe": "5m",
                    "trigger_candle_time": trigger_row["time"],
                    "htf_bias": htf_contexts.get(int(trigger_index), {}),
                },
            )
            self._append_or_skip(signal, skipped, trigger_row, direction, setup, out)
        return out

    def _first_five_min_confirmation_after(self, rows: pd.DataFrame, sweep_index: int, direction: str) -> int | None:
        candles = rows.set_index("datetime").sort_index()
        expected = "bullish" if direction == "CE" else "bearish"
        next_rows = rows.iloc[sweep_index + 1 : min(len(rows), sweep_index + 6)]
        for j in next_rows.index:
            disp = self.displacement.analyze(candles, int(j))
            structure = self.structure.structure_shift(candles, int(j))
            candle = rows.iloc[int(j)]
            directional_body = candle["close"] > candle["open"] if direction == "CE" else candle["close"] < candle["open"]
            if disp.get("direction") == expected or structure.get("direction") == expected or directional_body:
                return int(j)
        return None

    def _target_reversal_candidates(self, rows, i: int, row: pd.Series, levels: LevelSet, htf_contexts: dict[int, dict[str, Any]], skipped: list[SkippedSignal]) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        if row["time"] < self.cfg.late_reversal_start:
            return out
        for hit in self._liquidity_target_hits(rows, i, row, levels):
            direction = hit["direction"]
            setup = "BULLISH_LIQUIDITY_TARGET_REVERSAL" if direction == "CE" else "BEARISH_LIQUIDITY_TARGET_REVERSAL"
            trigger_index = self._first_five_min_confirmation_after(rows, i, direction)
            if trigger_index is None:
                skipped.append(self._skip(row, direction, setup, "No 5m confirmation after liquidity target hit", hit))
                continue
            trigger_row = rows.iloc[trigger_index]
            invalidation = [float(row["low"] if direction == "CE" else row["high"])]
            signal = self._build_signal(
                direction,
                setup,
                rows,
                trigger_index,
                trigger_row,
                levels,
                invalidation,
                {
                    "target_hit": hit,
                    "trigger_timeframe": "5m",
                    "trigger_candle_time": trigger_row["time"],
                    "htf_bias": htf_contexts.get(int(trigger_index), {}),
                },
            )
            self._append_or_skip(signal, skipped, trigger_row, direction, setup, out)
        return out

    def _liquidity_target_hits(self, rows: pd.DataFrame, i: int, row: pd.Series, levels: LevelSet) -> list[dict[str, Any]]:
        high = float(row["high"])
        low = float(row["low"])
        open_price = float(row["open"])
        close = float(row["close"])
        candle_range = high - low
        if candle_range <= 0:
            return []

        close_position = (close - low) / candle_range
        upper_rejection = (high - close) / candle_range
        lower_rejection = (close - low) / candle_range
        min_rejection = self.cfg.target_reversal_min_rejection_pct
        bearish_rejection = close < open_price and upper_rejection >= min_rejection
        bullish_rejection = close > open_price and lower_rejection >= min_rejection
        if not bearish_rejection and not bullish_rejection:
            return []

        buffer = self.cfg.target_reversal_hit_buffer_points
        previous = rows.iloc[:i]
        previous_after_or = previous[previous["time"] >= self.cfg.opening_range_end]
        found: list[dict[str, Any]] = []
        seen: set[tuple[str, float]] = set()

        for target in self._reversal_levels(levels, low, high):
            direction = target["direction"]
            price = float(target["price"])
            key = (direction, round(price, 2))
            if key in seen:
                continue
            seen.add(key)

            if direction == "PE":
                if high < price - buffer or not bearish_rejection:
                    continue
                if self.cfg.target_reversal_require_fresh_touch and not previous_after_or.empty and float(previous_after_or["high"].max()) >= price - buffer:
                    continue
                found.append(
                    {
                        **target,
                        "direction": "PE",
                        "hit_candle_time": row["time"],
                        "hit_price": high,
                        "rejection_pct": round(upper_rejection, 3),
                        "close_position": round(close_position, 3),
                    }
                )
            else:
                if low > price + buffer or not bullish_rejection:
                    continue
                if self.cfg.target_reversal_require_fresh_touch and not previous_after_or.empty and float(previous_after_or["low"].min()) <= price + buffer:
                    continue
                found.append(
                    {
                        **target,
                        "direction": "CE",
                        "hit_candle_time": row["time"],
                        "hit_price": low,
                        "rejection_pct": round(lower_rejection, 3),
                        "close_position": round(close_position, 3),
                    }
                )
        return found

    def _reversal_levels(self, levels: LevelSet, low: float, high: float) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for name, price, direction in [
            ("PDH", levels.pdh, "PE"),
            ("ORH", levels.orh, "PE"),
            ("PDL", levels.pdl, "CE"),
            ("ORL", levels.orl, "CE"),
        ]:
            if price is not None:
                out.append({"level": name, "price": float(price), "direction": direction})
        out.extend({"level": "SWING_HIGH", "price": float(swing["price"]), "direction": "PE", "swing_time": str(swing.get("time"))} for swing in levels.swing_highs)
        out.extend({"level": "SWING_LOW", "price": float(swing["price"]), "direction": "CE", "swing_time": str(swing.get("time"))} for swing in levels.swing_lows)
        out.extend(self._round_reversal_levels(low, high))
        return out

    def _round_reversal_levels(self, low: float, high: float) -> list[dict[str, Any]]:
        step = float(self.cfg.round_number_step)
        buffer = self.cfg.target_reversal_hit_buffer_points
        start = int(math.floor((low - buffer) / step))
        end = int(math.ceil((high + buffer) / step))
        out: list[dict[str, Any]] = []
        for value in range(start, end + 1):
            price = float(value * step)
            out.append({"level": "ROUND_NUMBER", "price": price, "direction": "PE"})
            out.append({"level": "ROUND_NUMBER", "price": price, "direction": "CE"})
        return out

    def _ob_retest_candidates(self, rows, i: int, row: pd.Series, levels: LevelSet, htf_contexts: dict[int, dict[str, Any]], skipped: list[SkippedSignal]) -> list[SignalCandidate]:
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
        signal = self._build_signal(direction, setup, rows, next_index, next_row, levels, invalidation, {"order_block": ob, "htf_bias": htf_contexts.get(int(next_index), {})})
        self._append_or_skip(signal, skipped, next_row, direction, setup, out)
        return out

    def _build_signal(
        self,
        direction: str,
        setup: str,
        rows: pd.DataFrame,
        i: int,
        row: pd.Series,
        levels: LevelSet,
        invalidation_points: list[float],
        extra: dict[str, Any],
    ) -> tuple[SignalCandidate | None, str | None, dict[str, Any]]:
        candles = rows.set_index("datetime").sort_index()
        disp = self.displacement.analyze(candles, i)
        expected = "bullish" if direction == "CE" else "bearish"
        structure = self.structure.structure_shift(candles, i)
        fvg_context = self.fvg.context(candles, i, direction)
        risk_plan, reason = self.risk.build_plan(row, levels, direction, invalidation_points)
        if reason:
            return None, reason, {**extra, "fair_value_gap": fvg_context}
        premium_discount_context = self.premium_discount.context(levels, risk_plan["entry"])
        liquidity_context = self.liquidity_context.context(
            rows,
            i,
            levels,
            direction,
            risk_plan["entry"],
            risk_plan["target"],
            extra,
        )
        has_directional_displacement = disp.get("direction") == expected
        has_directional_structure = structure.get("direction") == expected
        has_directional_body = row["close"] > row["open"] if direction == "CE" else row["close"] < row["open"]
        has_five_min_trigger = extra.get("trigger_timeframe") == "5m" and has_directional_body
        if not (has_directional_displacement or has_directional_structure or has_five_min_trigger or has_directional_body):
            return None, "No confirmation: needs 5m displacement, BOS/CHoCH/MSS, or directional candle", {"displacement": disp, "bos": structure, "structure_shift": structure, "fair_value_gap": fvg_context, "premium_discount": premium_discount_context, "liquidity_context": liquidity_context, **extra}
        if ("ORH_BREAKOUT" in setup or "ORL_BREAKDOWN" in setup) and not (has_directional_body and (has_directional_displacement or has_directional_structure)):
            return None, "Breakout lacks 5m candle/structure confirmation", {"displacement": disp, "bos": structure, "structure_shift": structure, "fair_value_gap": fvg_context, "premium_discount": premium_discount_context, "liquidity_context": liquidity_context, **extra}
        time_quality = self._time_quality(row["time"])
        score = self._score_setup(setup, direction, row, disp, structure, has_five_min_trigger, risk_plan, time_quality, fvg_context, premium_discount_context, liquidity_context)
        if score < self.cfg.min_setup_score:
            return None, f"Setup score below {self.cfg.min_setup_score}", {"score": score, "displacement": disp, "bos": structure, "structure_shift": structure, "fair_value_gap": fvg_context, "premium_discount": premium_discount_context, "liquidity_context": liquidity_context, **extra}
        features = self._features(
            row,
            levels,
            disp,
            structure,
            risk_plan,
            {
                **extra,
                "structure_shift": structure,
                "five_min_confirmation": has_five_min_trigger or has_directional_body,
                "fair_value_gap": fvg_context,
                "premium_discount": premium_discount_context,
                "liquidity_context": liquidity_context,
                "score_model": "soft_confirmation",
            },
        )
        option_contract = select_option_contract(
            direction=direction,
            spot_price=risk_plan["entry"],
            setup_score=score,
            features=features,
            option_snapshot=self.option_snapshot,
            cfg=self.cfg,
        )
        features["selected_option_contract"] = option_contract
        if option_contract.get("source") == "fyers_option_chain":
            features["option_selection_status"] = "live_chain_contract_underlying_backtest_pnl"
        else:
            features["option_selection_status"] = "intended_contract_only_no_historical_option_chain"
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
            htf_context = signal.features.get("reason_for_entry", {}).get("htf_bias") or {}
            if not self.htf_bias.allows(signal.direction, htf_context):
                skipped.append(
                    SkippedSignal(
                        signal.date,
                        str(signal.features.get("time") or signal.time),
                        signal.direction,
                        signal.setup_type,
                        "HTF bias filter blocked signal",
                        {
                            "entry_time": signal.time,
                            "htf_bias": htf_context,
                            "setup_score": signal.setup_score,
                        },
                    )
                )
            elif not self.premium_discount.allows(signal.direction, signal.features.get("reason_for_entry", {}).get("premium_discount") or {}):
                skipped.append(
                    SkippedSignal(
                        signal.date,
                        str(signal.features.get("time") or signal.time),
                        signal.direction,
                        signal.setup_type,
                        "Premium/discount filter blocked signal",
                        {
                            "entry_time": signal.time,
                            "premium_discount": signal.features.get("reason_for_entry", {}).get("premium_discount") or {},
                            "setup_score": signal.setup_score,
                        },
                    )
                )
            else:
                out.append(signal)
        elif reason:
            skipped.append(self._skip(row, direction, setup, reason, context))

    @staticmethod
    def _opening_range_width(levels: LevelSet) -> float | None:
        if levels.orh is None or levels.orl is None:
            return None
        return round(float(levels.orh - levels.orl), 2)

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
        structure: dict[str, Any],
        five_min_confirmation: bool,
        risk_plan: dict[str, Any],
        time_quality: int,
        fvg_context: dict[str, Any] | None = None,
        premium_discount_context: dict[str, Any] | None = None,
        liquidity_context: dict[str, Any] | None = None,
    ) -> int:
        expected = "bullish" if direction == "CE" else "bearish"
        score = 20
        if "ORH_BREAKOUT" in setup or "ORL_BREAKDOWN" in setup:
            score += 10
        if "LIQUIDITY_SWEEP" in setup:
            score += 8
        if "LIQUIDITY_TARGET_REVERSAL" in setup:
            score += 10
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

        if structure.get("direction") == expected:
            if structure.get("is_mss"):
                score += 18
            elif structure.get("is_choch"):
                score += 14
            elif structure.get("is_bos"):
                score += 15
            else:
                score += 10
        elif structure.get("is_structure_break"):
            score -= 8

        if five_min_confirmation:
            score += 10

        if fvg_context and fvg_context.get("present"):
            if not fvg_context.get("fully_mitigated"):
                score += 6
            if int(fvg_context.get("age_candles") or 99) <= 2:
                score += 3
            if fvg_context.get("entry_candle_touches"):
                score += 3

        if premium_discount_context and premium_discount_context.get("valid"):
            zone = premium_discount_context.get("zone")
            if (direction == "CE" and zone == "discount") or (direction == "PE" and zone == "premium"):
                score += 6
            elif zone == "equilibrium":
                score += 2
            else:
                score -= 6

        if liquidity_context:
            if liquidity_context.get("inducement", {}).get("present"):
                score += 6
            if liquidity_context.get("target_level", {}).get("classification") == "external":
                score += 4
            if liquidity_context.get("setup_level", {}).get("classification") == "external" and "LIQUIDITY_SWEEP" in setup:
                score += 3

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

    def _features(self, row: pd.Series, levels: LevelSet, disp: dict[str, Any], structure: dict[str, Any], risk_plan: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
        return {
            "date": str(row["date"]),
            "time": row["time"],
            "day_type": "unknown",
            "expiry_day": False,
            "ORH": levels.orh,
            "ORL": levels.orl,
            "OR_range": self._opening_range_width(levels),
            "PDH": levels.pdh,
            "PDL": levels.pdl,
            "distance_from_ORH": None if levels.orh is None else round(float(row["close"] - levels.orh), 2),
            "distance_from_ORL": None if levels.orl is None else round(float(row["close"] - levels.orl), 2),
            "sweep_level": extra.get("sweep", {}).get("level"),
            "sweep_depth": extra.get("sweep", {}).get("depth"),
            "target_hit_level": extra.get("target_hit", {}).get("level"),
            "target_hit_price": extra.get("target_hit", {}).get("price"),
            "target_hit_rejection_pct": extra.get("target_hit", {}).get("rejection_pct"),
            "target_hit_close_position": extra.get("target_hit", {}).get("close_position"),
            "displacement_candle_body_percentage": disp.get("body_pct"),
            "displacement_range_vs_recent_average": disp.get("range_ratio"),
            "BOS_strength": structure.get("strength"),
            "BOS_direction": structure.get("direction") if structure.get("is_bos") else None,
            "is_BOS": structure.get("is_bos"),
            "structure_break_type": structure.get("break_type"),
            "structure_break_direction": structure.get("direction"),
            "structure_trend_before": structure.get("trend_before"),
            "is_structure_break": structure.get("is_structure_break"),
            "is_CHOCH": structure.get("is_choch"),
            "is_MSS": structure.get("is_mss"),
            "structure_shift_strength": structure.get("strength"),
            "five_min_confirmation": extra.get("five_min_confirmation"),
            "HTF_bias": extra.get("htf_bias", {}).get("bias"),
            "HTF_bias_reason": extra.get("htf_bias", {}).get("reason"),
            "HTF_15m_bias": extra.get("htf_bias", {}).get("15m", {}).get("bias"),
            "HTF_15m_reason": extra.get("htf_bias", {}).get("15m", {}).get("reason"),
            "HTF_60m_bias": extra.get("htf_bias", {}).get("60m", {}).get("bias"),
            "HTF_60m_reason": extra.get("htf_bias", {}).get("60m", {}).get("reason"),
            "PD_zone": extra.get("premium_discount", {}).get("zone"),
            "PD_valid": extra.get("premium_discount", {}).get("valid"),
            "PD_position": extra.get("premium_discount", {}).get("position"),
            "PD_range_low": extra.get("premium_discount", {}).get("low"),
            "PD_range_high": extra.get("premium_discount", {}).get("high"),
            "PD_midpoint": extra.get("premium_discount", {}).get("midpoint"),
            "PD_range_points": extra.get("premium_discount", {}).get("range_points"),
            "PD_range_source": extra.get("premium_discount", {}).get("source"),
            "liquidity_setup_classification": extra.get("liquidity_context", {}).get("setup_level", {}).get("classification"),
            "liquidity_setup_side": extra.get("liquidity_context", {}).get("setup_level", {}).get("side"),
            "liquidity_setup_name": extra.get("liquidity_context", {}).get("setup_level", {}).get("name"),
            "liquidity_target_classification": extra.get("liquidity_context", {}).get("target_level", {}).get("classification"),
            "liquidity_target_side": extra.get("liquidity_context", {}).get("target_level", {}).get("side"),
            "liquidity_range_source": extra.get("liquidity_context", {}).get("range", {}).get("source"),
            "inducement_present": extra.get("liquidity_context", {}).get("inducement", {}).get("present"),
            "inducement_level": extra.get("liquidity_context", {}).get("inducement", {}).get("level"),
            "inducement_sweep_time": extra.get("liquidity_context", {}).get("inducement", {}).get("sweep_time"),
            "inducement_depth": extra.get("liquidity_context", {}).get("inducement", {}).get("depth"),
            "FVG_present": extra.get("fair_value_gap", {}).get("present"),
            "FVG_direction": extra.get("fair_value_gap", {}).get("direction"),
            "FVG_low": extra.get("fair_value_gap", {}).get("low"),
            "FVG_high": extra.get("fair_value_gap", {}).get("high"),
            "FVG_midpoint": extra.get("fair_value_gap", {}).get("midpoint"),
            "FVG_size": extra.get("fair_value_gap", {}).get("size"),
            "FVG_age_candles": extra.get("fair_value_gap", {}).get("age_candles"),
            "FVG_fully_mitigated": extra.get("fair_value_gap", {}).get("fully_mitigated"),
            "FVG_entry_candle_touches": extra.get("fair_value_gap", {}).get("entry_candle_touches"),
            "OB_size": None if not extra.get("order_block") else extra["order_block"].get("size"),
            "retest_depth": None,
            "entry_price": risk_plan["entry"],
            "SL_price": risk_plan["sl"],
            "target_price": risk_plan["target"],
            "RR": risk_plan["risk_reward"],
            "reason_for_entry": extra,
        }
