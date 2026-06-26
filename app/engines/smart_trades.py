from __future__ import annotations

from datetime import date
import math
from typing import Any

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import LevelSet, SignalCandidate, SkippedSignal, SmartZone
from app.engines.displacement import DisplacementEngine
from app.engines.fvg import FairValueGapEngine
from app.engines.htf_bias import HTFBiasEngine
from app.engines.levels import LevelEngine, RESISTANCE_TYPES, SUPPORT_TYPES
from app.engines.liquidity import LiquidityEngine
from app.engines.premium_discount import PremiumDiscountEngine
from app.engines.structure import StructureEngine
from app.options_pricing import select_option_contract


class SmartTradeEngine:
    def __init__(self, cfg: StrategyConfig = config) -> None:
        self.cfg = cfg
        self.levels = LevelEngine(cfg)
        self.displacement = DisplacementEngine(cfg)
        self.structure = StructureEngine(cfg)
        self.fvg = FairValueGapEngine(cfg)
        self.htf_bias = HTFBiasEngine(cfg)
        self.premium_discount = PremiumDiscountEngine(cfg)
        self.liquidity = LiquidityEngine()
        self.option_snapshot: dict[str, Any] | None = None
        self._snapshot_cache: dict[pd.Timestamp, tuple[list[SmartZone], float]] = {}
        self._htf_context_cache: dict[pd.Timestamp, dict[str, Any]] = {}
        self._prev_day_zones_cache: dict[date, list[SmartZone]] = {}

    def generate_for_day(
        self,
        candles_5m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
        htf_contexts: dict[int, dict[str, Any]] | None = None,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        signals: list[SignalCandidate] = []
        skipped: list[SkippedSignal] = []
        if not self.cfg.smart_trade_enabled:
            return signals, skipped

        all_rows = self._rows(candles_5m)
        day_rows = all_rows[all_rows["date"] == trading_date].reset_index(drop=True)
        if day_rows.empty:
            return signals, skipped

        self._previous_day_zones(all_rows, trading_date)
        seen: set[tuple[str, str, str]] = set()
        one_shot_taken: set[tuple[str, str, str]] = set()
        cached_zones: list[SmartZone] = []
        last_zone_refresh_index = -10_000
        for break_index, break_row in day_rows.iterrows():
            if break_row["time"] < self.cfg.opening_range_end or break_row["time"] > self.cfg.no_fresh_trade_after:
                continue
            global_break_index = int(break_row["global_index"])
            history = self._history_before(all_rows, break_row["datetime"])
            if len(history) < max(self.cfg.smart_atr_period, self.cfg.swing_left + self.cfg.swing_right + 3):
                continue

            previous_close = float(history.iloc[-1]["close"])
            atr = self._latest_atr(history)
            refresh_every = max(int(self.cfg.smart_trade_zone_refresh_candles), 1)
            if break_index - last_zone_refresh_index >= refresh_every:
                cached_zones = self._known_zones(all_rows, previous_close, break_row["datetime"], trading_date)
                last_zone_refresh_index = int(break_index)
            zones = cached_zones
            day_trend = self._day_trend(day_rows, int(break_index))
            for zone in zones:
                direction = self._break_direction(zone, previous_close, float(break_row["close"]))
                key = (zone.zone_id, direction, str(break_row["datetime"]))
                if direction is not None and key in seen:
                    continue
                if direction is not None:
                    seen.add(key)
                    self._handle_break_setup(
                        all_rows=all_rows,
                        day_rows=day_rows,
                        break_index=int(break_index),
                        break_row=break_row,
                        zone=zone,
                        direction=direction,
                        levels=levels,
                        atr=atr,
                        htf_contexts=htf_contexts or {},
                        target_zones=zones,
                        signals=signals,
                        skipped=skipped,
                    )
                    continue

                continuation = self._trend_continuation_setup(zone, day_rows, int(break_index), day_trend)
                if continuation is not None:
                    setup_c, direction_c, entry_model_c = continuation
                    cont_key = (zone.zone_id, setup_c, str(break_row["datetime"]))
                    if cont_key not in seen:
                        seen.add(cont_key)
                        continuation_signal = self._build_signal(
                            direction=direction_c,
                            setup=setup_c,
                            all_rows=all_rows,
                            day_rows=day_rows,
                            row_index=int(break_index),
                            row=break_row,
                            break_row=break_row,
                            zone=zone,
                            levels=levels,
                            atr=atr,
                            entry_model=entry_model_c,
                            htf_context=(htf_contexts or {}).get(int(break_index), {}),
                            target_zones=zones,
                        )
                        self._append(continuation_signal, skipped, break_row, direction_c, setup_c, signals)

                reaction = self._reaction_setup(zone, previous_close, break_row)
                if reaction is None:
                    continue
                setup, reaction_direction, entry_model = reaction
                one_shot_key = (zone.zone_id, reaction_direction, setup)
                if self._is_one_shot_setup(setup) and one_shot_key in one_shot_taken:
                    continue
                reaction_key = (zone.zone_id, setup, str(break_row["datetime"]))
                if reaction_key in seen:
                    continue
                seen.add(reaction_key)
                entry_index = int(break_index)
                entry_row = break_row
                if self.cfg.smart_trade_reaction_requires_hold:
                    hold_index = self._reaction_hold_index(day_rows, int(break_index), zone, reaction_direction)
                    if hold_index is None:
                        skipped.append(self._skip(break_row, reaction_direction, setup, "Reaction did not hold on the next confirmation candle", {"zone": zone.to_dict(), "entry_model": entry_model}))
                        continue
                    entry_index = hold_index
                    entry_row = day_rows.iloc[hold_index]
                signal = self._build_signal(
                    direction=reaction_direction,
                    setup=setup,
                    all_rows=all_rows,
                    day_rows=day_rows,
                    row_index=entry_index,
                    row=entry_row,
                    break_row=break_row,
                    zone=zone,
                    levels=levels,
                    atr=atr,
                    entry_model=entry_model,
                    htf_context=(htf_contexts or {}).get(entry_index, {}),
                    target_zones=zones,
                )
                self._append(signal, skipped, entry_row, reaction_direction, setup, signals)
                if signal[0] is not None and self._is_one_shot_setup(setup):
                    one_shot_taken.add(one_shot_key)
        return self._dedupe(signals), skipped

    def generate_for_candle(
        self,
        candles_5m: pd.DataFrame,
        levels: LevelSet,
        trading_date: date,
        candle_time,
        htf_context: dict[str, Any] | None = None,
    ) -> tuple[list[SignalCandidate], list[SkippedSignal]]:
        signals: list[SignalCandidate] = []
        skipped: list[SkippedSignal] = []
        if not self.cfg.smart_trade_enabled:
            return signals, skipped

        all_rows = self._rows(candles_5m)
        day_rows = all_rows[all_rows["date"] == trading_date].reset_index(drop=True)
        if day_rows.empty:
            return signals, skipped
        current_time = pd.to_datetime(candle_time)
        matches = day_rows.index[day_rows["datetime"] == current_time].tolist()
        if not matches:
            return signals, skipped
        current_index = int(matches[-1])
        current_row = day_rows.iloc[current_index]
        if current_row["time"] < self.cfg.opening_range_end or current_row["time"] > self.cfg.no_fresh_trade_after:
            return signals, skipped

        seen: set[tuple[str, str, str]] = set()
        signals.extend(self._candle_break_confirmations(all_rows, day_rows, current_index, current_row, levels, htf_context or {}, skipped, seen))
        signals.extend(self._candle_retest_confirmations(all_rows, day_rows, current_index, current_row, levels, htf_context or {}, skipped, seen))
        signals.extend(self._candle_reaction_holds(all_rows, day_rows, current_index, current_row, levels, htf_context or {}, skipped, seen))
        signals.extend(self._candle_trend_continuations(all_rows, day_rows, current_index, current_row, levels, htf_context or {}, skipped, seen))
        return self._dedupe(signals), skipped

    def _candle_sweep_reclaim_displacements(
        self,
        all_rows: pd.DataFrame,
        day_rows: pd.DataFrame,
        current_index: int,
        current_row: pd.Series,
        levels: LevelSet,
        htf_context: dict[str, Any],
        skipped: list[SkippedSignal],
        seen: set[tuple[str, str, str]],
    ) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        zones, atr = self._event_snapshot(all_rows, day_rows, current_index)
        if not zones:
            return out
        for zone in zones:
            sweep_reclaim = self._sweep_reclaim_displacement_setup(zone, current_row, atr)
            if sweep_reclaim is None:
                continue
            setup, direction, entry_model = sweep_reclaim
            key = (zone.zone_id, setup, str(current_row["datetime"]))
            if key in seen:
                continue
            seen.add(key)
            signal = self._build_signal(
                direction=direction,
                setup=setup,
                all_rows=all_rows,
                day_rows=day_rows,
                row_index=current_index,
                row=current_row,
                break_row=current_row,
                zone=zone,
                levels=levels,
                atr=atr,
                entry_model=entry_model,
                htf_context=htf_context,
                target_zones=zones,
            )
            self._append(signal, skipped, current_row, direction, setup, out)
        return out

    def _candle_break_confirmations(
        self,
        all_rows: pd.DataFrame,
        day_rows: pd.DataFrame,
        current_index: int,
        current_row: pd.Series,
        levels: LevelSet,
        htf_context: dict[str, Any],
        skipped: list[SkippedSignal],
        seen: set[tuple[str, str, str]],
    ) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        start = max(0, current_index - self.cfg.smart_trade_confirmation_window_candles)
        for break_index in range(start, current_index):
            break_row = day_rows.iloc[break_index]
            zones, atr = self._event_snapshot(all_rows, day_rows, break_index)
            if not zones:
                continue
            previous_close = self._previous_close_before(all_rows, break_row["datetime"])
            if previous_close is None:
                continue
            for zone in zones:
                direction = self._break_direction(zone, previous_close, float(break_row["close"]))
                if direction is None or not self._confirms(current_row, zone, direction):
                    continue
                key = (zone.zone_id, "SMART_ZONE_BREAK_CONFIRMATION", str(current_row["datetime"]))
                if key in seen:
                    continue
                seen.add(key)
                signal = self._build_signal(
                    direction=direction,
                    setup="SMART_ZONE_BREAK_CONFIRMATION",
                    all_rows=all_rows,
                    day_rows=day_rows,
                    row_index=current_index,
                    row=current_row,
                    break_row=break_row,
                    zone=zone,
                    levels=levels,
                    atr=atr,
                    entry_model="break_confirmation",
                    htf_context=htf_context,
                    target_zones=zones,
                )
                self._append(signal, skipped, current_row, direction, "SMART_ZONE_BREAK_CONFIRMATION", out)
        return out

    def _candle_retest_confirmations(
        self,
        all_rows: pd.DataFrame,
        day_rows: pd.DataFrame,
        current_index: int,
        current_row: pd.Series,
        levels: LevelSet,
        htf_context: dict[str, Any],
        skipped: list[SkippedSignal],
        seen: set[tuple[str, str, str]],
    ) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        confirm_start = max(1, current_index - self.cfg.smart_trade_retest_window_candles)
        for confirm_index in range(confirm_start, current_index):
            confirm_row = day_rows.iloc[confirm_index]
            break_start = max(0, confirm_index - self.cfg.smart_trade_confirmation_window_candles)
            for break_index in range(break_start, confirm_index):
                break_row = day_rows.iloc[break_index]
                zones, atr = self._event_snapshot(all_rows, day_rows, break_index)
                if not zones:
                    continue
                previous_close = self._previous_close_before(all_rows, break_row["datetime"])
                if previous_close is None:
                    continue
                for zone in zones:
                    direction = self._break_direction(zone, previous_close, float(break_row["close"]))
                    touched = float(current_row["low"]) <= zone.high and float(current_row["high"]) >= zone.low
                    if direction is None or not self._confirms(confirm_row, zone, direction) or not touched or not self._confirms(current_row, zone, direction):
                        continue
                    key = (zone.zone_id, "SMART_ZONE_RETEST_CONFIRMATION", str(current_row["datetime"]))
                    if key in seen:
                        continue
                    seen.add(key)
                    signal = self._build_signal(
                        direction=direction,
                        setup="SMART_ZONE_RETEST_CONFIRMATION",
                        all_rows=all_rows,
                        day_rows=day_rows,
                        row_index=current_index,
                        row=current_row,
                        break_row=break_row,
                        zone=zone,
                        levels=levels,
                        atr=atr,
                        entry_model="break_confirm_retest",
                        htf_context=htf_context,
                        target_zones=zones,
                    )
                    self._append(signal, skipped, current_row, direction, "SMART_ZONE_RETEST_CONFIRMATION", out)
        return out

    def _candle_reaction_holds(
        self,
        all_rows: pd.DataFrame,
        day_rows: pd.DataFrame,
        current_index: int,
        current_row: pd.Series,
        levels: LevelSet,
        htf_context: dict[str, Any],
        skipped: list[SkippedSignal],
        seen: set[tuple[str, str, str]],
    ) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        reaction_index = current_index - 1
        if reaction_index < 0:
            return out
        reaction_row = day_rows.iloc[reaction_index]
        zones, atr = self._event_snapshot(all_rows, day_rows, reaction_index)
        if not zones:
            return out
        previous_close = self._previous_close_before(all_rows, reaction_row["datetime"])
        if previous_close is None:
            return out
        for zone in zones:
            reaction = self._reaction_setup(zone, previous_close, reaction_row)
            if reaction is None:
                continue
            setup, direction, entry_model = reaction
            if not self._confirms(current_row, zone, direction):
                continue
            key = (zone.zone_id, setup, str(current_row["datetime"]))
            if key in seen:
                continue
            seen.add(key)
            signal = self._build_signal(
                direction=direction,
                setup=setup,
                all_rows=all_rows,
                day_rows=day_rows,
                row_index=current_index,
                row=current_row,
                break_row=reaction_row,
                zone=zone,
                levels=levels,
                atr=atr,
                entry_model=entry_model,
                htf_context=htf_context,
                target_zones=zones,
            )
            self._append(signal, skipped, current_row, direction, setup, out)
        return out

    def _candle_trend_continuations(
        self,
        all_rows: pd.DataFrame,
        day_rows: pd.DataFrame,
        current_index: int,
        current_row: pd.Series,
        levels: LevelSet,
        htf_context: dict[str, Any],
        skipped: list[SkippedSignal],
        seen: set[tuple[str, str, str]],
    ) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        if not self.cfg.smart_trade_continuation_enabled:
            return out
        zones, atr = self._event_snapshot(all_rows, day_rows, current_index)
        if not zones:
            return out
        day_trend = self._day_trend(day_rows, current_index)
        for zone in zones:
            continuation = self._trend_continuation_setup(zone, day_rows, current_index, day_trend)
            if continuation is None:
                continue
            setup, direction, entry_model = continuation
            key = (zone.zone_id, setup, str(current_row["datetime"]))
            if key in seen:
                continue
            seen.add(key)
            signal = self._build_signal(
                direction=direction,
                setup=setup,
                all_rows=all_rows,
                day_rows=day_rows,
                row_index=current_index,
                row=current_row,
                break_row=current_row,
                zone=zone,
                levels=levels,
                atr=atr,
                entry_model=entry_model,
                htf_context=htf_context,
                target_zones=zones,
            )
            self._append(signal, skipped, current_row, direction, setup, out)
        return out

    def _handle_break_setup(
        self,
        *,
        all_rows: pd.DataFrame,
        day_rows: pd.DataFrame,
        break_index: int,
        break_row: pd.Series,
        zone: SmartZone,
        direction: str,
        levels: LevelSet,
        atr: float,
        htf_contexts: dict[int, dict[str, Any]],
        target_zones: list[SmartZone],
        signals: list[SignalCandidate],
        skipped: list[SkippedSignal],
    ) -> None:
        chase = self._break_chase(zone, direction, float(break_row["close"]))
        if chase > atr * self.cfg.smart_trade_max_chase_atr:
            skipped.append(self._skip(break_row, direction, "SMART_ZONE_BREAK_CONFIRMATION", "Break candle is too far from the zone", {"zone": zone.to_dict(), "chase_points": round(chase, 2), "atr": round(atr, 2)}))
            return

        confirm_index = self._confirmation_index(day_rows, break_index, zone, direction)
        if confirm_index is None:
            skipped.append(self._skip(break_row, direction, "SMART_ZONE_BREAK_CONFIRMATION", "No one-candle 5m confirmation after zone break", {"zone": zone.to_dict()}))
            return

        confirm_row = day_rows.iloc[confirm_index]
        signal = self._build_signal(
            direction=direction,
            setup="SMART_ZONE_BREAK_CONFIRMATION",
            all_rows=all_rows,
            day_rows=day_rows,
            row_index=int(confirm_index),
            row=confirm_row,
            break_row=break_row,
            zone=zone,
            levels=levels,
            atr=atr,
            entry_model="break_confirmation",
            htf_context=htf_contexts.get(int(confirm_index), {}),
            target_zones=target_zones,
        )
        self._append(signal, skipped, confirm_row, direction, "SMART_ZONE_BREAK_CONFIRMATION", signals)

        retest_index = self._retest_index(day_rows, int(confirm_index), zone, direction)
        if retest_index is not None:
            retest_row = day_rows.iloc[retest_index]
            retest_signal = self._build_signal(
                direction=direction,
                setup="SMART_ZONE_RETEST_CONFIRMATION",
                all_rows=all_rows,
                day_rows=day_rows,
                row_index=int(retest_index),
                row=retest_row,
                break_row=break_row,
                zone=zone,
                levels=levels,
                atr=atr,
                entry_model="break_confirm_retest",
                htf_context=htf_contexts.get(int(retest_index), {}),
                target_zones=target_zones,
            )
            self._append(retest_signal, skipped, retest_row, direction, "SMART_ZONE_RETEST_CONFIRMATION", signals)

    def _known_zones(self, all_rows: pd.DataFrame, current_price: float, as_of: Any, trading_date: date | None = None) -> list[SmartZone]:
        as_of_ts = pd.to_datetime(as_of)

        # Layer 1: zones from completed previous sessions (computed once, cached)
        prev_zones = self._previous_day_zones(all_rows, trading_date) if trading_date is not None else []

        # Layer 2: zones forming candle by candle from today's session
        intraday_zones: list[SmartZone] = []
        if trading_date is not None:
            today_rows = all_rows[(all_rows["date"] == trading_date) & (all_rows["datetime"] <= as_of_ts)]
            if not today_rows.empty:
                intraday_result = self.levels.calculate_smart_zones(today_rows, current_price=current_price, as_of=as_of)
                intraday_zones = [
                    zone for zone in intraday_result.zones
                    if zone.score >= self.cfg.smart_trade_min_zone_score
                    and zone.status != "broken"
                    and zone.break_count <= self.cfg.smart_max_allowed_breaks
                ]

        # Merge both layers, prev day zones first (higher priority), dedup by zone_id
        seen_ids: set[str] = set()
        combined: list[SmartZone] = []
        for zone in prev_zones + intraday_zones:
            if zone.zone_id not in seen_ids:
                seen_ids.add(zone.zone_id)
                combined.append(zone)
        return combined

    def _previous_day_zones(self, all_rows: pd.DataFrame, trading_date: date) -> list[SmartZone]:
        cached = self._prev_day_zones_cache.get(trading_date)
        if cached is not None:
            return cached
        prev_rows = all_rows[all_rows["date"] < trading_date]
        days = int(getattr(self.cfg, "smart_trade_zone_history_days", 0) or 0)
        if days > 0 and not prev_rows.empty:
            previous_dates = sorted({day for day in prev_rows["date"].unique() if day < trading_date})
            prev_rows = prev_rows[prev_rows["date"].isin(set(previous_dates[-days:]))]
        if prev_rows.empty:
            self._prev_day_zones_cache[trading_date] = []
            return []
        current_price = float(prev_rows.iloc[-1]["close"])
        result = self.levels.calculate_smart_zones(prev_rows, current_price=current_price)
        zones = [
            zone for zone in result.zones
            if zone.score >= self.cfg.smart_trade_min_zone_score
            and zone.status != "broken"
            and zone.break_count <= self.cfg.smart_max_allowed_breaks
        ]
        self._prev_day_zones_cache[trading_date] = zones
        return zones

    def _break_direction(self, zone: SmartZone, previous_close: float, close: float) -> str | None:
        if self._is_resistance(zone) and previous_close <= zone.high and close > zone.high:
            return "CE"
        if self._is_support(zone) and previous_close >= zone.low and close < zone.low:
            return "PE"
        return None

    def _reaction_setup(self, zone: SmartZone, previous_close: float, row: pd.Series) -> tuple[str, str, str] | None:
        touched = float(row["low"]) <= zone.high and float(row["high"]) >= zone.low
        if not touched:
            return None
        close = float(row["close"])
        open_price = float(row["open"])
        bullish = close > open_price
        bearish = close < open_price

        if self._is_support(zone):
            if previous_close >= zone.low and close > zone.high and bullish:
                return ("SMART_ZONE_SUPPORT_REACTION_CONFIRMATION", "CE", "support_reclaim_reaction")

        if self._is_resistance(zone):
            if previous_close <= zone.high and close < zone.low and bearish:
                return ("SMART_ZONE_RESISTANCE_REJECTION_CONFIRMATION", "PE", "resistance_rejection")
        return None

    def _trend_continuation_setup(
        self,
        zone: SmartZone,
        day_rows: pd.DataFrame,
        confirm_index: int,
        trend: str,
    ) -> tuple[str, str, str] | None:
        """Pullback continuation: in an established trend, price pulls back into a
        with-trend zone then resumes in the trend direction on the confirm candle."""
        if not self.cfg.smart_trade_continuation_enabled:
            return None
        if confirm_index < 1 or trend not in {"up", "down"}:
            return None
        confirm_row = day_rows.iloc[confirm_index]
        close = float(confirm_row["close"])
        open_price = float(confirm_row["open"])
        lookback = max(1, int(getattr(self.cfg, "smart_trade_continuation_pullback_lookback", 4) or 4))
        start = max(0, confirm_index - lookback)
        window = day_rows.iloc[start : confirm_index + 1]
        touched = bool(((window["low"] <= zone.high) & (window["high"] >= zone.low)).any())
        if not touched:
            return None
        if self._is_support(zone) and trend == "up" and close > open_price and close > zone.low:
            return ("SMART_ZONE_TREND_CONTINUATION", "CE", "trend_continuation")
        if self._is_resistance(zone) and trend == "down" and close < open_price and close < zone.high:
            return ("SMART_ZONE_TREND_CONTINUATION", "PE", "trend_continuation")
        return None

    def _day_trend(self, day_rows: pd.DataFrame, index: int) -> str:
        if index < 1 or day_rows.empty:
            return "range"
        candles = day_rows.set_index("datetime").sort_index()
        bounded = min(int(index), len(candles) - 1)
        return self.structure.trend(candles, bounded)

    def _sweep_reclaim_displacement_setup(self, zone: SmartZone, row: pd.Series, atr: float) -> tuple[str, str, str] | None:
        if not self._is_displacement_reclaim(row, atr):
            return None
        low = float(row["low"])
        high = float(row["high"])
        close = float(row["close"])
        open_price = float(row["open"])
        if self._is_support(zone) and low < zone.low and close > zone.high and close > open_price:
            return ("SMART_ZONE_SWEEP_RECLAIM_DISPLACEMENT", "CE", "sweep_reclaim_displacement")
        if self._is_resistance(zone) and high > zone.high and close < zone.low and close < open_price:
            return ("SMART_ZONE_SWEEP_RECLAIM_DISPLACEMENT", "PE", "sweep_reclaim_displacement")
        return None

    def _is_displacement_reclaim(self, row: pd.Series, atr: float) -> bool:
        candle_range = max(float(row["high"]) - float(row["low"]), 0.01)
        body = abs(float(row["close"]) - float(row["open"]))
        body_pct = body / candle_range
        min_body = float(getattr(self.cfg, "smart_trade_sweep_reclaim_min_body_pct", 0.55) or 0.55)
        min_range_atr = float(getattr(self.cfg, "smart_trade_sweep_reclaim_min_range_atr", 1.0) or 1.0)
        return body_pct >= min_body and candle_range >= max(float(atr), 1.0) * min_range_atr

    def _confirmation_index(self, rows: pd.DataFrame, break_index: int, zone: SmartZone, direction: str) -> int | None:
        end = min(len(rows), break_index + 1 + self.cfg.smart_trade_confirmation_window_candles)
        for index in range(break_index + 1, end):
            row = rows.iloc[index]
            if row["time"] > self.cfg.no_fresh_trade_after:
                return None
            if self._confirms(row, zone, direction):
                return index
        return None

    def _retest_index(self, rows: pd.DataFrame, confirm_index: int, zone: SmartZone, direction: str) -> int | None:
        end = min(len(rows), confirm_index + 1 + self.cfg.smart_trade_retest_window_candles)
        for index in range(confirm_index + 1, end):
            row = rows.iloc[index]
            if row["time"] > self.cfg.no_fresh_trade_after:
                return None
            touched = float(row["low"]) <= zone.high and float(row["high"]) >= zone.low
            if touched and self._confirms(row, zone, direction):
                return index
        return None

    def _reaction_hold_index(self, rows: pd.DataFrame, reaction_index: int, zone: SmartZone, direction: str) -> int | None:
        next_index = reaction_index + 1
        if next_index >= len(rows):
            return None
        row = rows.iloc[next_index]
        if row["time"] > self.cfg.no_fresh_trade_after:
            return None
        return next_index if self._confirms(row, zone, direction) else None

    def _build_signal(
        self,
        *,
        direction: str,
        setup: str,
        all_rows: pd.DataFrame,
        day_rows: pd.DataFrame,
        row_index: int,
        row: pd.Series,
        break_row: pd.Series,
        zone: SmartZone,
        levels: LevelSet,
        atr: float,
        entry_model: str,
        htf_context: dict[str, Any],
        target_zones: list[SmartZone],
    ) -> tuple[SignalCandidate | None, str | None, dict[str, Any]]:
        if not htf_context:
            htf_context = self._htf_context_for(all_rows, row["datetime"])
        enhancer_features = self._zone_enhancer_features(zone)
        entry = round(float(row["close"]), 2)
        buffer = self.cfg.sl_buffer_points + (atr * self.cfg.smart_trade_sl_atr_buffer)
        if direction == "CE":
            sl = round(min(float(zone.low), float(break_row["low"]), float(row["low"])) - buffer, 2)
        else:
            sl = round(max(float(zone.high), float(break_row["high"]), float(row["high"])) + buffer, 2)
        original_sl = sl
        sl = self._reduced_zone_sl(setup, direction, zone, entry, buffer, sl)
        risk = entry - sl if direction == "CE" else sl - entry
        if risk <= 0:
            return None, "Invalid smart-zone SL", {"zone": zone.to_dict(), "entry_model": entry_model}
        if risk > self.cfg.max_entry_sl_points:
            return None, "Smart-zone entry is too far from SL", {"zone": zone.to_dict(), "risk_points": round(risk, 2), "entry_model": entry_model}
        # Zone quality is already enforced uniformly upstream (every zone passed in
        # clears smart_trade_min_zone_score), so no per-setup zone-score gate here.
        target = self._target(entry, direction, target_zones, zone, levels, min_reward=risk * self.cfg.minimum_rr)
        if target is None:
            return None, "No smart-zone or liquidity target ahead", {"zone": zone.to_dict(), "entry_model": entry_model}
        reward = abs(float(target["price"]) - entry)
        rr = reward / risk if risk > 0 else 0
        if rr < self.cfg.minimum_rr:
            return None, f"RR below 1:{self.cfg.minimum_rr:g}", {"zone": zone.to_dict(), "target": target, "risk_points": round(risk, 2), "reward_points": round(reward, 2), "entry_model": entry_model}

        candles = day_rows.set_index("datetime").sort_index()
        disp = self.displacement.analyze(candles, row_index)
        structure = self.structure.structure_shift(candles, row_index)
        fvg_context = self.fvg.context(candles, row_index, direction)
        pd_context = self.premium_discount.context(levels, entry)
        # HTF bias is a single hard gate applied identically to every setup: never
        # trade directly against the higher-timeframe bias (neutral is allowed).
        # No per-setup overrides or escape hatches.
        if not self.htf_bias.allows(direction, htf_context):
            return None, "HTF bias filter blocked smart-zone setup", {
                "zone": zone.to_dict(),
                "htf_bias": htf_context,
                "entry_model": entry_model,
            }

        # Quality is one uniform confluence threshold for all setups. Weak setups
        # (e.g. a reaction with no structure, or a counter-PD entry) simply score
        # low here and fail the same gate, instead of bespoke per-setup blocks.
        score = self._score(setup, direction, row, zone, disp, structure, fvg_context, pd_context, htf_context, rr, risk, atr)
        if score < self.cfg.min_setup_score:
            return None, f"Smart-zone confluence score below {self.cfg.min_setup_score}", {
                "zone": zone.to_dict(),
                "score": score,
                "entry_model": entry_model,
            }

        entry_datetime = pd.to_datetime(row["datetime"]) + pd.Timedelta(minutes=5)
        features = {
            "date": str(row["date"]),
            "time": row["time"],
            "entry_price": entry,
            "SL_price": sl,
            "original_SL_price": original_sl,
            "target_price": round(float(target["price"]), 2),
            "RR": round(rr, 2),
            "entry_model": entry_model,
            "smart_zone_sl_model": "zone_inner_fraction" if sl != original_sl else "structural_extreme",
            "smart_trade_grade": self._grade(score),
            "smart_zone": zone.to_dict(),
            "smart_zone_score": zone.score,
            "smart_zone_status": zone.status,
            "smart_zone_type": zone.zone_type,
            "smart_zone_low": zone.low,
            "smart_zone_high": zone.high,
            **enhancer_features,
            "smart_zone_break_time": break_row["time"],
            "smart_zone_confirmation_time": row["time"],
            "smart_zone_break_close": float(break_row["close"]),
            "target_name": target["name"],
            "target_source": target.get("source"),
            "target_zone": target.get("zone"),
            "displacement_candle_body_percentage": disp.get("body_pct"),
            "displacement_range_vs_recent_average": disp.get("range_ratio"),
            "BOS_direction": structure.get("direction") if structure.get("is_bos") else None,
            "is_BOS": structure.get("is_bos"),
            "is_CHOCH": structure.get("is_choch"),
            "is_MSS": structure.get("is_mss"),
            "FVG_present": fvg_context.get("present"),
            "FVG_direction": fvg_context.get("direction"),
            "PD_zone": pd_context.get("zone"),
            "PD_valid": pd_context.get("valid"),
            "HTF_bias": htf_context.get("bias"),
            "HTF_bias_reason": htf_context.get("reason"),
            "reason_for_entry": {
                "zone": zone.to_dict(),
                "zone_enhancers": zone.enhancers,
                "break_candle_time": break_row["time"],
                "confirmation_candle_time": row["time"],
                "entry_model": entry_model,
                "target": target,
                "htf_bias": htf_context,
                "premium_discount": pd_context,
                "fair_value_gap": fvg_context,
                "structure_shift": structure,
                "score_model": entry_model,
            },
        }
        option_contract = select_option_contract(
            direction=direction,
            spot_price=entry,
            setup_score=score,
            features=features,
            option_snapshot=self.option_snapshot,
            cfg=self.cfg,
        )
        features["selected_option_contract"] = option_contract
        features["option_selection_status"] = (
            "live_chain_contract_underlying_backtest_pnl"
            if option_contract.get("source") == "fyers_option_chain"
            else "intended_contract_only_no_historical_option_chain"
        )
        return SignalCandidate(
            date=str(entry_datetime.date()),
            time=entry_datetime.strftime("%H:%M"),
            symbol=self.cfg.symbol,
            direction=direction,
            setup_type=setup,
            entry_index_price=entry,
            sl_index_price=sl,
            target_index_price=round(float(target["price"]), 2),
            risk_points=round(risk, 2),
            reward_points=round(reward, 2),
            risk_reward=round(rr, 2),
            setup_score=score,
            features=features,
            notes=[f"Smart zone {zone.zone_type} {zone.low}-{zone.high}", f"Target: {target['name']}"],
        ), None, features

    def _target(
        self,
        entry: float,
        direction: str,
        zones: list[SmartZone],
        broken_zone: SmartZone,
        levels: LevelSet,
        min_reward: float = 0.0,
    ) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for zone in zones:
            if zone.zone_id == broken_zone.zone_id:
                continue
            if direction == "CE" and self._is_resistance(zone) and zone.low > entry:
                candidates.append({"name": f"SMART_ZONE:{zone.zone_type}", "price": zone.low, "source": "smart_zone", "zone": zone.to_dict(), "distance": zone.low - entry})
            if direction == "PE" and self._is_support(zone) and zone.high < entry:
                candidates.append({"name": f"SMART_ZONE:{zone.zone_type}", "price": zone.high, "source": "smart_zone", "zone": zone.to_dict(), "distance": entry - zone.high})
        candidates.extend(self._classic_targets(levels, entry, direction))
        if not candidates:
            return None
        ordered = sorted(candidates, key=lambda item: float(item["distance"]))
        return next((item for item in ordered if float(item["distance"]) >= min_reward), ordered[0])

    def _classic_targets(self, levels: LevelSet, entry: float, direction: str) -> list[dict[str, Any]]:
        raw: list[tuple[str, float | None]] = [
            ("PDH", levels.pdh),
            ("PDL", levels.pdl),
            ("PDC", levels.pdc),
            ("ORH", levels.orh),
            ("ORL", levels.orl),
        ]
        raw.extend(("ROUND_NUMBER", price) for price in self.levels.round_levels(entry))
        out: list[dict[str, Any]] = []
        seen: set[tuple[str, float]] = set()
        for name, price in raw:
            if price is None:
                continue
            value = float(price)
            if not math.isfinite(value):
                continue
            if direction == "CE" and value <= entry:
                continue
            if direction == "PE" and value >= entry:
                continue
            key = (name, round(value, 2))
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "name": name,
                    "price": value,
                    "source": "classic_liquidity",
                    "distance": value - entry if direction == "CE" else entry - value,
                }
            )
        return out

    def _reduced_zone_sl(self, setup: str, direction: str, zone: SmartZone, entry: float, buffer: float, structural_sl: float) -> float:
        if setup not in {"SMART_ZONE_BREAK_CONFIRMATION", "SMART_ZONE_RETEST_CONFIRMATION"}:
            return structural_sl
        fraction = float(getattr(self.cfg, "smart_trade_sl_zone_inner_fraction", 0.0) or 0.0)
        if fraction <= 0:
            return structural_sl
        fraction = min(max(fraction, 0.0), 0.5)
        zone_width = max(float(zone.high) - float(zone.low), 0.0)
        if zone_width <= 0:
            return structural_sl
        if direction == "CE":
            candidate = round(float(zone.low) + (zone_width * fraction) - buffer, 2)
            if candidate >= entry:
                return structural_sl
            return max(float(structural_sl), candidate)
        candidate = round(float(zone.high) - (zone_width * fraction) + buffer, 2)
        if candidate <= entry:
            return structural_sl
        return min(float(structural_sl), candidate)

    def _score(
        self,
        setup: str,
        direction: str,
        row: pd.Series,
        zone: SmartZone,
        disp: dict[str, Any],
        structure: dict[str, Any],
        fvg_context: dict[str, Any],
        pd_context: dict[str, Any],
        htf_context: dict[str, Any],
        rr: float,
        risk: float,
        atr: float,
    ) -> int:
        """Equal-weighted confluence score: the fraction of independent structural
        confirmations that are present, scaled to 0-100. No hand-tuned weights or
        per-setup bonuses, so the same evidence is worth the same for every setup.

        ``risk``/``atr``/``setup`` are accepted for signature stability but are not
        used; risk geometry is already enforced by the RR and SL gates upstream.
        """
        expected = "bullish" if direction == "CE" else "bearish"
        pd_zone = pd_context.get("zone")
        confirm_in_direction = (
            float(row["close"]) > float(row["open"]) if direction == "CE" else float(row["close"]) < float(row["open"])
        )
        factors = [
            # 1. confirmation candle closes in the trade direction
            bool(confirm_in_direction),
            # 2. displacement is aligned with the trade
            disp.get("direction") == expected,
            # 3. market structure broke in the trade direction
            bool(structure.get("is_structure_break") and structure.get("direction") == expected),
            # 4. an unmitigated directional fair-value gap supports the move
            bool(
                fvg_context.get("present")
                and not fvg_context.get("fully_mitigated")
                and fvg_context.get("direction") == expected
            ),
            # 5. entering from the favourable side of premium/discount
            (direction == "CE" and pd_zone == "discount") or (direction == "PE" and pd_zone == "premium"),
            # 6. the zone is fresh (barely touched)
            zone.touch_count <= 1,
            # 7. reward-to-risk is generous
            rr >= 2.0,
            # 8. the zone itself is high quality
            float(zone.score) >= 70.0,
        ]
        satisfied = sum(1 for ok in factors if ok)
        return int(round(100.0 * satisfied / len(factors)))

    def _append(self, result, skipped, row, direction, setup, signals) -> None:
        signal, reason, context = result
        if signal:
            signals.append(signal)
        elif reason:
            skipped.append(self._skip(row, direction, setup, reason, context))

    @staticmethod
    def _is_one_shot_setup(setup: str) -> bool:
        return setup in {
            "SMART_ZONE_SUPPORT_REACTION_CONFIRMATION",
            "SMART_ZONE_RESISTANCE_REJECTION_CONFIRMATION",
        }

    @staticmethod
    def _dedupe(signals: list[SignalCandidate]) -> list[SignalCandidate]:
        out: list[SignalCandidate] = []
        seen: set[tuple[str, str, str, str, float]] = set()
        for signal in sorted(signals, key=lambda item: (item.date, item.time, -item.setup_score)):
            key = (signal.date, signal.time, signal.direction, signal.setup_type, round(signal.entry_index_price, 2))
            if key in seen:
                continue
            seen.add(key)
            out.append(signal)
        return out

    def _skip(self, row: pd.Series, direction: str, setup: str, reason: str, context: dict[str, Any]) -> SkippedSignal:
        context = self._skip_context_with_enhancers(context)
        return SkippedSignal(str(row["date"]), row["time"], direction, setup, reason, context)

    @staticmethod
    def _zone_enhancer_features(zone: SmartZone) -> dict[str, Any]:
        enhancers = zone.enhancers or {}
        points = {
            name: round(float(value.get("points") or 0), 2)
            for name, value in enhancers.items()
            if isinstance(value, dict) and "points" in value
        }
        return {
            "smart_zone_enhancers": enhancers,
            "smart_zone_enhancer_total": round(float(enhancers.get("total_points") or 0), 2),
            "smart_zone_enhancer_max": round(float(enhancers.get("max_points") or 0), 2),
            "smart_zone_enhancer_points": points,
        }

    @classmethod
    def _skip_context_with_enhancers(cls, context: dict[str, Any]) -> dict[str, Any]:
        payload = dict(context or {})
        zone = payload.get("zone")
        if not isinstance(zone, dict):
            return payload
        enhancers = zone.get("enhancers") or {}
        if not isinstance(enhancers, dict):
            return payload
        points = {
            name: round(float(value.get("points") or 0), 2)
            for name, value in enhancers.items()
            if isinstance(value, dict) and "points" in value
        }
        payload.setdefault("zone_enhancers", enhancers)
        payload.setdefault("zone_enhancer_total", round(float(enhancers.get("total_points") or 0), 2))
        payload.setdefault("zone_enhancer_max", round(float(enhancers.get("max_points") or 0), 2))
        payload.setdefault("zone_enhancer_points", points)
        return payload

    def _latest_atr(self, rows: pd.DataFrame) -> float:
        atr = self.levels.calculate_atr(rows)
        return max(float(atr.iloc[-1]) if not atr.empty else 0.0, 0.01)

    def _htf_context_for(self, rows: pd.DataFrame, as_of: Any) -> dict[str, Any]:
        timestamp = pd.to_datetime(as_of)
        cached = self._htf_context_cache.get(timestamp)
        if cached is not None:
            return cached
        columns = [column for column in ["datetime", "open", "high", "low", "close", "volume", "date", "time"] if column in rows.columns]
        frame = rows.loc[rows["datetime"] <= timestamp, columns].copy()
        if frame.empty:
            context = self.htf_bias.context(pd.DataFrame(), timestamp)
        else:
            frame = frame.set_index("datetime").sort_index()
            context = self.htf_bias.context(frame, timestamp)
        self._htf_context_cache[timestamp] = context
        return context

    def _event_snapshot(
        self,
        all_rows: pd.DataFrame,
        day_rows: pd.DataFrame,
        event_index: int,
    ) -> tuple[list[SmartZone], float]:
        if day_rows.empty:
            return [], 0.0
        opening_candidates = day_rows.index[day_rows["time"] >= self.cfg.opening_range_end].tolist()
        opening_index = int(opening_candidates[0]) if opening_candidates else 0
        refresh_every = max(int(self.cfg.smart_trade_zone_refresh_candles), 1)
        if event_index < opening_index:
            anchor_index = event_index
        else:
            anchor_index = opening_index + ((event_index - opening_index) // refresh_every) * refresh_every
        anchor_index = max(0, min(anchor_index, len(day_rows) - 1))
        as_of = pd.to_datetime(day_rows.iloc[anchor_index]["datetime"])
        cached = self._snapshot_cache.get(as_of)
        if cached is not None:
            return cached
        history = self._history_before(all_rows, as_of)
        if history.empty:
            snapshot = ([], 0.0)
        else:
            trading_date = day_rows.iloc[0]["date"] if not day_rows.empty else None
            zones = self._known_zones(all_rows, float(history.iloc[-1]["close"]), as_of, trading_date)
            snapshot = (zones, self._latest_atr(history))
        self._snapshot_cache[as_of] = snapshot
        return snapshot

    @staticmethod
    def _previous_close_before(rows: pd.DataFrame, timestamp: Any) -> float | None:
        history = rows[rows["datetime"] < pd.to_datetime(timestamp)]
        if history.empty:
            return None
        return float(history.iloc[-1]["close"])

    def _history_before(self, rows: pd.DataFrame, timestamp: Any) -> pd.DataFrame:
        as_of = pd.to_datetime(timestamp)
        history = rows[rows["datetime"] < as_of]
        days = int(getattr(self.cfg, "smart_trade_zone_history_days", 0) or 0)
        if days > 0 and not history.empty:
            current_date = as_of.date()
            previous_dates = sorted({day for day in history["date"].unique() if day < current_date})
            keep_dates = set(previous_dates[-days:])
            keep_dates.add(current_date)
            history = history[history["date"].isin(keep_dates)]
        return history

    @staticmethod
    def _confirms(row: pd.Series, zone: SmartZone, direction: str) -> bool:
        if direction == "CE":
            return float(row["close"]) > zone.high and float(row["close"]) > float(row["open"])
        return float(row["close"]) < zone.low and float(row["close"]) < float(row["open"])

    @staticmethod
    def _break_chase(zone: SmartZone, direction: str, close: float) -> float:
        if direction == "CE":
            return max(close - zone.high, 0.0)
        return max(zone.low - close, 0.0)

    @staticmethod
    def _is_support(zone: SmartZone) -> bool:
        return any(tag in SUPPORT_TYPES for tag in zone.zone_type.split("+"))

    @staticmethod
    def _is_resistance(zone: SmartZone) -> bool:
        return any(tag in RESISTANCE_TYPES for tag in zone.zone_type.split("+"))

    @staticmethod
    def _grade(score: int) -> str:
        if score >= 85:
            return "A+"
        if score >= 75:
            return "A"
        if score >= 60:
            return "B"
        return "skip"

    @staticmethod
    def _rows(candles: pd.DataFrame) -> pd.DataFrame:
        rows = candles.copy()
        if "datetime" not in rows.columns:
            rows = rows.reset_index()
            if "datetime" not in rows.columns:
                rows = rows.rename(columns={rows.columns[0]: "datetime"})
        rows["datetime"] = pd.to_datetime(rows["datetime"])
        for column in ["open", "high", "low", "close", "volume"]:
            if column in rows.columns:
                rows[column] = pd.to_numeric(rows[column], errors="coerce")
        if "volume" not in rows.columns:
            rows["volume"] = 0
        rows = rows.dropna(subset=["open", "high", "low", "close"]).sort_values("datetime").drop_duplicates("datetime")
        rows = rows.reset_index(drop=True)
        rows["global_index"] = rows.index
        rows["date"] = rows["datetime"].dt.date
        rows["time"] = rows["datetime"].dt.strftime("%H:%M")
        return rows
