from __future__ import annotations

import csv
import io
import json
import math
from datetime import date
from typing import Any

import pandas as pd

from app.config import StrategyConfig, config
from app.domain import LevelSet, SmartLevelResult, SmartZone


SUPPORT_TYPES = {
    "swing_low",
    "demand",
    "breakout_base",
    "gap_up",
    "equal_lows_liquidity",
}
RESISTANCE_TYPES = {
    "swing_high",
    "supply",
    "breakdown_base",
    "gap_down",
    "equal_highs_liquidity",
}
DECISION_TYPES = {
    "demand",
    "supply",
    "breakout_base",
    "breakdown_base",
}
LIQUIDITY_CONTEXT_TYPES = {
    "equal_lows_liquidity",
    "equal_highs_liquidity",
}


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
        rows = self._rows(candles)
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

    def calculate_smart_zones(
        self,
        candles_5m: pd.DataFrame,
        current_price: float | None = None,
        as_of: Any | None = None,
    ) -> SmartLevelResult:
        rows = self._session_rows(candles_5m)
        if as_of is not None and not rows.empty:
            rows = rows[rows["datetime"] <= pd.to_datetime(as_of)]
        if rows.empty:
            return SmartLevelResult(0.0, 0.0, [], [], [], [], [], [])

        rows = rows.copy().reset_index(drop=True)
        rows["atr"] = self.calculate_atr(rows)
        atr = self._latest_atr(rows)
        price = float(current_price if current_price is not None else rows.iloc[-1]["close"])
        raw_zones = self._candidate_zones(rows)
        scored = [self._score_zone(rows, zone, raw_zones) for zone in raw_zones]
        merged = self.merge_zones(scored, atr)
        filtered = self.filter_noisy_zones(merged, price, atr, rows)
        filtered = sorted(filtered, key=lambda zone: zone.score, reverse=True)
        return self._result(filtered, price, atr, rows)

    def calculate_atr(self, candles: pd.DataFrame, period: int | None = None) -> pd.Series:
        rows = self._rows(candles)
        if rows.empty:
            return pd.Series(dtype="float64")
        high = pd.to_numeric(rows["high"], errors="coerce")
        low = pd.to_numeric(rows["low"], errors="coerce")
        close = pd.to_numeric(rows["close"], errors="coerce")
        previous_close = close.shift(1)
        true_range = pd.concat(
            [
                high - low,
                (high - previous_close).abs(),
                (low - previous_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return true_range.rolling(period or self.cfg.smart_atr_period, min_periods=1).mean()

    def merge_zones(self, zones: list[SmartZone], atr: float | None = None, *, strict: bool = False) -> list[SmartZone]:
        if not zones:
            return []
        atr_value = float(atr or 0)
        tolerance = self._cluster_tolerance(atr_value, strict)
        max_cluster_span = self.cfg.smart_max_zone_width_points if strict else max(self.cfg.smart_max_zone_width_points, atr_value * 5)
        clusters: list[list[SmartZone]] = []
        for zone in sorted(zones, key=lambda item: (item.low, item.high, -item.score)):
            matching_indexes = [
                index
                for index, cluster in enumerate(clusters)
                if self._cluster_can_accept(cluster, zone, tolerance, max_cluster_span)
            ]
            if matching_indexes:
                target = matching_indexes[0]
                clusters[target].append(zone)
                for index in reversed(matching_indexes[1:]):
                    clusters[target].extend(clusters.pop(index))
            else:
                clusters.append([zone])
            clusters = self._merge_compatible_clusters(clusters, tolerance, max_cluster_span)
        merged = [self._merge_cluster(cluster, atr_value) for cluster in clusters]
        return self._dedupe_merged_zones(merged, atr_value, tolerance, max_cluster_span)

    def filter_noisy_zones(
        self,
        zones: list[SmartZone],
        current_price: float,
        atr: float | None = None,
        rows: pd.DataFrame | None = None,
    ) -> list[SmartZone]:
        atr_value = max(float(atr or 0), 1.0)
        max_distance = atr_value * self.cfg.smart_max_distance_from_current_price_atr
        latest_time = None if rows is None or rows.empty else rows.iloc[-1]["datetime"]
        filtered: list[SmartZone] = []
        for zone in zones:
            width = zone.high - zone.low
            temp_strong_move_zone = self._is_temp_strong_move_zone(zone)
            too_old = False
            if latest_time is not None:
                reference = zone.last_touched_at or zone.created_at
                too_old = self._trading_day_age(reference, latest_time) > self.cfg.smart_max_age_days_without_touch
            if zone.score < self.cfg.smart_min_zone_score and not temp_strong_move_zone:
                continue
            if zone.break_count > self.cfg.smart_max_allowed_breaks:
                continue
            if zone.reaction_count <= 0 and zone.reaction_score < 40 and not temp_strong_move_zone:
                continue
            if abs(zone.midpoint - current_price) > max_distance:
                continue
            if too_old:
                continue
            if width > max(self.cfg.smart_max_zone_width_points, atr_value * 5):
                continue
            if width < self.cfg.smart_min_zone_width_points and zone.reaction_count <= 0:
                continue
            filtered.append(zone)
        return filtered

    def smart_zones_json(self, result: SmartLevelResult) -> str:
        return json.dumps(result.to_dict(), indent=2, default=str)

    def smart_zones_csv(self, result: SmartLevelResult) -> str:
        output = io.StringIO()
        fields = [
            "zone_id",
            "zone_type",
            "low",
            "high",
            "midpoint",
            "created_at",
            "last_touched_at",
            "touch_count",
            "reaction_count",
            "break_count",
            "score",
            "status",
            "enhancers",
            "notes",
        ]
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for zone in result.zones:
            row = zone.to_dict()
            row["enhancers"] = json.dumps(zone.enhancers, default=str)
            row["notes"] = "; ".join(zone.notes)
            writer.writerow({field: row.get(field) for field in fields})
        return output.getvalue()

    def _candidate_zones(self, rows: pd.DataFrame) -> list[dict[str, Any]]:
        zones: list[dict[str, Any]] = []
        zones.extend(self._decision_zones(rows))
        zones.extend(self._sweep_reclaim_zones(rows))
        zones.extend(self._swing_zones(rows))
        zones.extend(self._base_zones(rows))
        zones.extend(self._daily_gap_zones(rows))
        zones.extend(self._liquidity_zones(rows))
        return zones

    def _swing_zones(self, rows: pd.DataFrame) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        swings = self.detect_swings(rows)
        for swing in swings["highs"]:
            index = int(swing["index"])
            if not self._is_significant_swing(rows, index, "bearish"):
                continue
            confirmed_index = min(index + self.cfg.swing_right, len(rows) - 1)
            price = float(swing["price"])
            width = self._zone_width(rows, index)
            out.append(self._raw_zone("swing_high", price - width, price, confirmed_index, "bearish", ["confirmed swing high with displacement/rejection"]))
        for swing in swings["lows"]:
            index = int(swing["index"])
            if not self._is_significant_swing(rows, index, "bullish"):
                continue
            confirmed_index = min(index + self.cfg.swing_right, len(rows) - 1)
            price = float(swing["price"])
            width = self._zone_width(rows, index)
            out.append(self._raw_zone("swing_low", price, price + width, confirmed_index, "bullish", ["confirmed swing low with displacement/rejection"]))
        return out

    def _base_zones(self, rows: pd.DataFrame) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for i in range(1, len(rows) - 2):
            base = rows.iloc[i]
            atr = max(float(base.get("atr") or 0), 1.0)
            next_window = rows.iloc[i + 1 : min(i + 4, len(rows))]
            if next_window.empty:
                continue
            bullish_threshold = self.cfg.smart_min_reaction_atr * atr
            bearish_threshold = self.cfg.smart_min_reaction_atr * atr
            bullish_confirm = next_window[next_window["high"] - float(base["high"]) >= bullish_threshold]
            bearish_confirm = next_window[float(base["low"]) - next_window["low"] >= bearish_threshold]
            body = abs(float(base["close"]) - float(base["open"]))
            candle_range = max(float(base["high"]) - float(base["low"]), 0.01)
            is_base = body / candle_range <= 0.45 and candle_range <= atr * 1.35
            if not is_base:
                continue
            low, high = self._expanded_zone(float(base["low"]), float(base["high"]), i, rows)
            if not bullish_confirm.empty:
                confirmed = bullish_confirm.iloc[0]
                if not self._breaks_structure(rows, int(confirmed.name), "bullish"):
                    continue
                zone_type = "breakout_base" if float(confirmed["close"]) > float(base["high"]) else "demand"
                out.append(self._raw_zone(zone_type, low, high, int(confirmed.name), "bullish", ["base before fast bullish move"], base_candle_count=1))
            if not bearish_confirm.empty:
                confirmed = bearish_confirm.iloc[0]
                if not self._breaks_structure(rows, int(confirmed.name), "bearish"):
                    continue
                zone_type = "breakdown_base" if float(confirmed["close"]) < float(base["low"]) else "supply"
                out.append(self._raw_zone(zone_type, low, high, int(confirmed.name), "bearish", ["base before fast bearish move"], base_candle_count=1))
        return out

    def _decision_zones(self, rows: pd.DataFrame) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        max_base = max(1, int(self.cfg.smart_quality_max_base_candles))
        for impulse_index in range(max_base, len(rows)):
            impulse = rows.iloc[impulse_index]
            direction = self._impulse_direction(rows, impulse_index)
            if direction is None:
                continue
            base_slice = self._origin_base(rows, impulse_index, direction, max_base)
            if base_slice.empty:
                continue
            base_low = float(base_slice["low"].min())
            base_high = float(base_slice["high"].max())
            origin_index = int(base_slice.index[-1])
            low, high = self._refined_origin_zone(rows, base_slice, direction)
            if direction == "bullish":
                zone_type = "breakout_base" if self._breaks_structure(rows, impulse_index, "bullish") else "demand"
                note = "5m demand origin: tight base before bullish displacement"
            else:
                zone_type = "breakdown_base" if self._breaks_structure(rows, impulse_index, "bearish") else "supply"
                note = "5m supply origin: tight base before bearish displacement"
            if high <= low:
                low, high = self._expanded_zone(base_low, base_high, origin_index, rows)
            out.append(self._raw_zone(zone_type, low, high, impulse_index, direction, [note], base_candle_count=len(base_slice)))
        return out

    def _sweep_reclaim_zones(self, rows: pd.DataFrame) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        swings = self.detect_swings(rows)
        for swing in swings["lows"]:
            swing_index = int(swing["index"])
            swing_price = float(swing["price"])
            for index in range(swing_index + self.cfg.swing_right + 1, min(len(rows), swing_index + 18)):
                row = rows.iloc[index]
                atr = max(float(row.get("atr") or self._latest_atr(rows)), 1.0)
                swept = float(row["low"]) < swing_price - (atr * self.cfg.smart_quality_sweep_reclaim_atr)
                candle_range = max(float(row["high"]) - float(row["low"]), 0.01)
                close_location = (float(row["close"]) - float(row["low"])) / candle_range
                reclaimed = float(row["close"]) > swing_price and close_location >= 0.55
                if swept and reclaimed:
                    low, high = self._rejection_candle_zone(rows, index, "bullish")
                    out.append(self._raw_zone("swing_low", low, high, index, "bullish", ["liquidity sweep below swing low and reclaim"]))
                    break
        for swing in swings["highs"]:
            swing_index = int(swing["index"])
            swing_price = float(swing["price"])
            for index in range(swing_index + self.cfg.swing_right + 1, min(len(rows), swing_index + 18)):
                row = rows.iloc[index]
                atr = max(float(row.get("atr") or self._latest_atr(rows)), 1.0)
                swept = float(row["high"]) > swing_price + (atr * self.cfg.smart_quality_sweep_reclaim_atr)
                candle_range = max(float(row["high"]) - float(row["low"]), 0.01)
                close_location = (float(row["high"]) - float(row["close"])) / candle_range
                reclaimed = float(row["close"]) < swing_price and close_location >= 0.55
                if swept and reclaimed:
                    low, high = self._rejection_candle_zone(rows, index, "bearish")
                    out.append(self._raw_zone("swing_high", low, high, index, "bearish", ["liquidity sweep above swing high and rejection"]))
                    break
        return out

    def _daily_gap_zones(self, rows: pd.DataFrame) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for current_date in sorted(rows["date"].unique())[1:]:
            today = rows[rows["date"] == current_date]
            previous = rows[rows["date"] < current_date]
            if today.empty or previous.empty:
                continue
            previous_close = float(previous.iloc[-1]["close"])
            first = today.iloc[0]
            open_price = float(first["open"])
            atr = max(float(first.get("atr") or 0), 1.0)
            if abs(open_price - previous_close) < atr * 0.4:
                continue
            index = int(first.name)
            if open_price > previous_close:
                out.append(self._raw_zone("gap_up", previous_close, open_price, index, "bullish", ["daily gap up zone"]))
            else:
                out.append(self._raw_zone("gap_down", open_price, previous_close, index, "bearish", ["daily gap down zone"]))
        return out

    def _liquidity_zones(self, rows: pd.DataFrame) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        swings = self.detect_swings(rows)
        out.extend(self._equal_price_clusters(rows, swings["highs"], "equal_highs_liquidity", "bearish"))
        out.extend(self._equal_price_clusters(rows, swings["lows"], "equal_lows_liquidity", "bullish"))
        return out

    def _equal_price_clusters(
        self,
        rows: pd.DataFrame,
        swings: list[dict[str, Any]],
        zone_type: str,
        direction: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if len(swings) < 2:
            return out
        atr = self._latest_atr(rows)
        tolerance = max(atr * self.cfg.smart_cluster_atr_multiplier, self.cfg.smart_min_zone_width_points / 2)
        unused = swings[:]
        while unused:
            anchor = unused.pop(0)
            cluster = [anchor]
            remaining = []
            for swing in unused:
                if abs(float(swing["price"]) - float(anchor["price"])) <= tolerance:
                    cluster.append(swing)
                else:
                    remaining.append(swing)
            unused = remaining
            if len(cluster) < 2:
                continue
            prices = [float(item["price"]) for item in cluster]
            index = min(max(int(item["index"]) + self.cfg.swing_right for item in cluster), len(rows) - 1)
            width = self._zone_width(rows, index) / 2
            out.append(
                self._raw_zone(
                    zone_type,
                    min(prices) - width,
                    max(prices) + width,
                    index,
                    direction,
                    [f"{len(cluster)} near-equal swing points"],
                )
            )
        return out

    def _impulse_direction(self, rows: pd.DataFrame, index: int) -> str | None:
        row = rows.iloc[index]
        atr = max(float(row.get("atr") or self._latest_atr(rows)), 1.0)
        body = abs(float(row["close"]) - float(row["open"]))
        candle_range = max(float(row["high"]) - float(row["low"]), 0.01)
        body_pct = body / candle_range
        if body_pct < self.cfg.smart_quality_min_body_pct:
            return None
        previous = rows.iloc[max(0, index - self.cfg.smart_quality_structure_lookback) : index]
        if previous.empty:
            return None
        if float(row["close"]) > float(row["open"]):
            displacement = float(row["close"]) - float(previous["high"].max())
            if displacement >= atr * self.cfg.smart_quality_displacement_atr or self._breaks_structure(rows, index, "bullish"):
                return "bullish"
        if float(row["close"]) < float(row["open"]):
            displacement = float(previous["low"].min()) - float(row["close"])
            if displacement >= atr * self.cfg.smart_quality_displacement_atr or self._breaks_structure(rows, index, "bearish"):
                return "bearish"
        return None

    def _origin_base(self, rows: pd.DataFrame, impulse_index: int, direction: str, max_base: int) -> pd.DataFrame:
        indexes: list[int] = []
        for index in range(impulse_index - 1, max(-1, impulse_index - max_base - 1), -1):
            row = rows.iloc[index]
            atr = max(float(row.get("atr") or self._latest_atr(rows)), 1.0)
            body_pct = self._candle_body_pct(row)
            candle_range = float(row["high"]) - float(row["low"])
            is_tight = candle_range <= atr * 1.25
            is_pause = body_pct <= 0.55 or is_tight
            is_opposing = (direction == "bullish" and float(row["close"]) <= float(row["open"])) or (
                direction == "bearish" and float(row["close"]) >= float(row["open"])
            )
            if is_pause or is_opposing:
                indexes.append(index)
                continue
            break
        if not indexes:
            return pd.DataFrame()
        return rows.iloc[sorted(indexes)]

    def _refined_origin_zone(self, rows: pd.DataFrame, base_slice: pd.DataFrame, direction: str) -> tuple[float, float]:
        if base_slice.empty:
            return 0.0, 0.0
        if direction == "bullish":
            candidates = base_slice[base_slice["close"] <= base_slice["open"]]
            source = candidates.iloc[-1] if not candidates.empty else base_slice.iloc[-1]
            low = float(source["low"])
            high = max(float(source["open"]), float(source["close"]))
        else:
            candidates = base_slice[base_slice["close"] >= base_slice["open"]]
            source = candidates.iloc[-1] if not candidates.empty else base_slice.iloc[-1]
            low = min(float(source["open"]), float(source["close"]))
            high = float(source["high"])
        index = int(source.name)
        min_width = self.cfg.smart_min_zone_width_points
        if high - low < min_width:
            midpoint = (low + high) / 2
            half = min(self._zone_width(rows, index), min_width) / 2
            low, high = midpoint - half, midpoint + half
        return round(low, 2), round(high, 2)

    def _rejection_candle_zone(self, rows: pd.DataFrame, index: int, direction: str) -> tuple[float, float]:
        row = rows.iloc[index]
        if direction == "bullish":
            low = float(row["low"])
            high = min(max(float(row["open"]), float(row["close"])), float(row["high"]))
        else:
            low = max(min(float(row["open"]), float(row["close"])), float(row["low"]))
            high = float(row["high"])
        if high - low < self.cfg.smart_min_zone_width_points:
            midpoint = (low + high) / 2
            half = self._zone_width(rows, index) / 2
            low, high = midpoint - half, midpoint + half
        return round(low, 2), round(high, 2)

    def _is_significant_swing(self, rows: pd.DataFrame, index: int, direction: str) -> bool:
        after = rows.iloc[index + 1 : min(len(rows), index + 8)]
        if after.empty:
            return False
        row = rows.iloc[index]
        atr = max(float(row.get("atr") or self._latest_atr(rows)), 1.0)
        required = atr * self.cfg.smart_quality_swing_reaction_atr
        if direction == "bullish":
            reaction = float(after["high"].max()) - float(row["low"])
            rejection = self._lower_wick_pct(row) >= 0.35 and float(row["close"]) > float(row["open"])
            return reaction >= required or rejection or self._breaks_structure(rows, min(index + len(after), len(rows) - 1), "bullish")
        reaction = float(row["high"]) - float(after["low"].min())
        rejection = self._upper_wick_pct(row) >= 0.35 and float(row["close"]) < float(row["open"])
        return reaction >= required or rejection or self._breaks_structure(rows, min(index + len(after), len(rows) - 1), "bearish")

    def _breaks_structure(self, rows: pd.DataFrame, index: int, direction: str) -> bool:
        lookback = max(2, int(self.cfg.smart_quality_structure_lookback))
        previous = rows.iloc[max(0, index - lookback) : index]
        if previous.empty:
            return False
        row = rows.iloc[index]
        if direction == "bullish":
            return float(row["close"]) > float(previous["high"].max())
        return float(row["close"]) < float(previous["low"].min())

    @staticmethod
    def _candle_body_pct(row: pd.Series) -> float:
        candle_range = max(float(row["high"]) - float(row["low"]), 0.01)
        return abs(float(row["close"]) - float(row["open"])) / candle_range

    @staticmethod
    def _lower_wick_pct(row: pd.Series) -> float:
        candle_range = max(float(row["high"]) - float(row["low"]), 0.01)
        lower = min(float(row["open"]), float(row["close"])) - float(row["low"])
        return max(lower, 0.0) / candle_range

    @staticmethod
    def _upper_wick_pct(row: pd.Series) -> float:
        candle_range = max(float(row["high"]) - float(row["low"]), 0.01)
        upper = float(row["high"]) - max(float(row["open"]), float(row["close"]))
        return max(upper, 0.0) / candle_range

    def _score_zone(self, rows: pd.DataFrame, raw: dict[str, Any], all_raw: list[dict[str, Any]]) -> SmartZone:
        index = int(raw["index"])
        direction = str(raw["direction"])
        low = round(float(raw["low"]), 2)
        high = round(float(raw["high"]), 2)
        midpoint = round((low + high) / 2, 2)
        created_at = rows.iloc[index]["datetime"]
        after = rows.iloc[index + 1 :]
        atr = max(float(rows.iloc[index].get("atr") or self._latest_atr(rows)), 1.0)
        touch_count, reaction_count, break_count, last_touched_at, crossed_count = self._zone_behavior(after, low, high, direction, atr)
        reaction_score = self._reaction_score(after, low, high, direction, atr)
        speed_score = self._speed_score(after, low, high, direction, atr)
        reaction_move_points = self._reaction_move_points(after, low, high, direction)
        temp_strong_move_zone = self._is_temp_strong_move_points(reaction_move_points)
        if temp_strong_move_zone:
            reaction_score = max(reaction_score, 100.0)
        touch_quality_score = self._touch_quality_score(touch_count, reaction_count, break_count, crossed_count)
        freshness_score = max(0.0, 100.0 - (touch_count * 18.0) - (break_count * 25.0))
        recency_score = self._recency_score(last_touched_at or created_at, rows.iloc[-1]["datetime"])
        htf_visibility_score = self._htf_visibility_score(raw, high - low, atr)
        volume_score = self._volume_score(rows, index)
        gap_overlap_score = self._overlap_score(raw, all_raw, {"gap_up", "gap_down"})
        liquidity_sweep_score = self._liquidity_sweep_score(rows, low, high, direction, raw)
        enhancers = self._zone_enhancers(
            raw=raw,
            all_raw=all_raw,
            low=low,
            high=high,
            direction=direction,
            touch_count=touch_count,
            reaction_move_points=reaction_move_points,
            atr=atr,
            htf_visibility_score=htf_visibility_score,
        )
        noise_penalty = self._noise_penalty(high - low, atr, break_count, crossed_count, reaction_score)
        score = self._final_score(
            reaction_score=reaction_score,
            speed_score=speed_score,
            touch_quality_score=touch_quality_score,
            freshness_score=freshness_score,
            recency_score=recency_score,
            htf_visibility_score=htf_visibility_score,
            volume_score=volume_score,
            gap_overlap_score=gap_overlap_score,
            liquidity_sweep_score=liquidity_sweep_score,
            noise_penalty=noise_penalty,
        )
        notes = list(raw.get("notes") or [])
        if temp_strong_move_zone:
            score = max(score, float(self.cfg.smart_temp_strong_move_min_score))
            notes.append(f"TEMP strong move zone: price moved {round(reaction_move_points, 2)} points away")
        return SmartZone(
            zone_id=self._zone_id(str(raw["zone_type"]), low, high, created_at),
            zone_type=str(raw["zone_type"]),
            low=low,
            high=high,
            midpoint=midpoint,
            created_at=created_at,
            last_touched_at=last_touched_at,
            touch_count=touch_count,
            reaction_count=reaction_count,
            break_count=break_count,
            score=score,
            freshness_score=round(freshness_score, 2),
            recency_score=round(recency_score, 2),
            reaction_score=round(reaction_score, 2),
            speed_score=round(speed_score, 2),
            touch_quality_score=round(touch_quality_score, 2),
            htf_visibility_score=round(htf_visibility_score, 2),
            volume_score=round(volume_score, 2),
            gap_overlap_score=round(gap_overlap_score, 2),
            liquidity_sweep_score=round(liquidity_sweep_score, 2),
            noise_penalty=round(noise_penalty, 2),
            status=self._status(touch_count, break_count),
            notes=notes,
            enhancers=enhancers,
        )

    def _zone_behavior(
        self,
        rows: pd.DataFrame,
        low: float,
        high: float,
        direction: str,
        atr: float,
    ) -> tuple[int, int, int, Any | None, int]:
        touch_count = 0
        reaction_count = 0
        break_count = 0
        crossed_count = 0
        last_touched_at = None
        for _, row in rows.iterrows():
            candle_high = float(row["high"])
            candle_low = float(row["low"])
            close = float(row["close"])
            touched = candle_low <= high and candle_high >= low
            if touched:
                touch_count += 1
                last_touched_at = row["datetime"]
                if direction == "bullish" and close > high:
                    reaction_count += 1
                elif direction == "bearish" and close < low:
                    reaction_count += 1
            if direction == "bullish":
                if close < low:
                    break_count += 1
                if candle_high > high and candle_low < low:
                    crossed_count += 1
            else:
                if close > high:
                    break_count += 1
                if candle_high > high and candle_low < low:
                    crossed_count += 1
        return touch_count, reaction_count, break_count, last_touched_at, crossed_count

    def _reaction_score(self, rows: pd.DataFrame, low: float, high: float, direction: str, atr: float) -> float:
        if rows.empty:
            return 0.0
        move = self._reaction_move_points(rows, low, high, direction)
        return self._clamp(move / max(self.cfg.smart_min_reaction_atr * atr, 0.01) * 100.0)

    def _reaction_move_points(self, rows: pd.DataFrame, low: float, high: float, direction: str) -> float:
        if rows.empty:
            return 0.0
        window = rows.head(12)
        if direction == "bullish":
            move = float(window["high"].max()) - high
        else:
            move = low - float(window["low"].min())
        return max(float(move), 0.0)

    def _zone_enhancers(
        self,
        raw: dict[str, Any],
        all_raw: list[dict[str, Any]],
        low: float,
        high: float,
        direction: str,
        touch_count: int,
        reaction_move_points: float,
        atr: float,
        htf_visibility_score: float,
    ) -> dict[str, Any]:
        base_candle_count = int(raw.get("base_candle_count") or 1)
        enhancers = {
            "move_strength": self._move_strength_enhancer(reaction_move_points, atr),
            "base_candle_count": self._base_candle_count_enhancer(base_candle_count),
            "freshness": self._freshness_enhancer(touch_count),
            "risk_reward_space": self._risk_reward_space_enhancer(raw, all_raw, low, high, direction),
            "original_zone": self._original_zone_enhancer(raw, all_raw, low, high),
            "htf_overlap": self._htf_overlap_enhancer(htf_visibility_score),
        }
        total = sum(float(item["points"]) for item in enhancers.values())
        enhancers["total_points"] = round(total, 2)
        enhancers["max_points"] = 14.0
        return enhancers

    @staticmethod
    def _move_strength_enhancer(reaction_move_points: float, atr: float) -> dict[str, Any]:
        ratio = float(reaction_move_points) / max(float(atr), 0.01)
        if ratio >= 3.0:
            points = 3.0
        elif ratio >= 2.0:
            points = 2.0
        elif ratio >= 1.0:
            points = 1.0
        else:
            points = 0.0
        return {"points": points, "move_points": round(float(reaction_move_points), 2), "move_atr_ratio": round(ratio, 2)}

    @staticmethod
    def _base_candle_count_enhancer(base_candle_count: int) -> dict[str, Any]:
        if base_candle_count <= 3:
            points = 2.0
        elif base_candle_count <= 5:
            points = 1.0
        else:
            points = 0.0
        return {"points": points, "base_candle_count": int(base_candle_count)}

    @staticmethod
    def _freshness_enhancer(touch_count: int) -> dict[str, Any]:
        if touch_count <= 0:
            points = 3.0
        elif touch_count == 1:
            points = 1.5
        else:
            points = 0.0
        return {"points": points, "touch_count": int(touch_count)}

    def _risk_reward_space_enhancer(
        self,
        raw: dict[str, Any],
        all_raw: list[dict[str, Any]],
        low: float,
        high: float,
        direction: str,
    ) -> dict[str, Any]:
        width = max(float(high) - float(low), 0.01)
        distance = self._nearest_opposing_raw_distance(raw, all_raw, low, high, direction)
        ratio = distance / width if distance is not None else 0.0
        if ratio >= 3.0:
            points = 2.0
        elif ratio >= 2.0:
            points = 1.0
        else:
            points = 0.0
        return {"points": points, "space_to_opposing_zone": round(distance or 0.0, 2), "space_width_ratio": round(ratio, 2)}

    def _original_zone_enhancer(self, raw: dict[str, Any], all_raw: list[dict[str, Any]], low: float, high: float) -> dict[str, Any]:
        raw_index = int(raw["index"])
        prior_overlap = any(
            other is not raw
            and int(other.get("index") or 0) < raw_index
            and self._overlaps(low, high, float(other["low"]), float(other["high"]))
            for other in all_raw
        )
        return {"points": 0.0 if prior_overlap else 2.0, "prior_overlap": bool(prior_overlap)}

    @staticmethod
    def _htf_overlap_enhancer(htf_visibility_score: float) -> dict[str, Any]:
        if htf_visibility_score >= 85:
            points = 2.0
        elif htf_visibility_score >= 70:
            points = 1.0
        else:
            points = 0.0
        return {"points": points, "htf_visibility_score": round(float(htf_visibility_score), 2)}

    def _nearest_opposing_raw_distance(
        self,
        raw: dict[str, Any],
        all_raw: list[dict[str, Any]],
        low: float,
        high: float,
        direction: str,
    ) -> float | None:
        distances: list[float] = []
        raw_type = str(raw["zone_type"])
        raw_support = any(tag in SUPPORT_TYPES for tag in raw_type.split("+"))
        for other in all_raw:
            if other is raw:
                continue
            other_type = str(other["zone_type"])
            other_support = any(tag in SUPPORT_TYPES for tag in other_type.split("+"))
            other_resistance = any(tag in RESISTANCE_TYPES for tag in other_type.split("+"))
            other_low = float(other["low"])
            other_high = float(other["high"])
            if direction == "bullish" and raw_support and other_resistance and other_low > high:
                distances.append(other_low - high)
            elif direction == "bearish" and not raw_support and other_support and other_high < low:
                distances.append(low - other_high)
        return min(distances) if distances else None

    def _speed_score(self, rows: pd.DataFrame, low: float, high: float, direction: str, atr: float) -> float:
        if rows.empty:
            return 0.0
        window = rows.head(4)
        if direction == "bullish":
            move = float(window["high"].max()) - high
        else:
            move = low - float(window["low"].min())
        return self._clamp(move / max(atr * len(window), 0.01) * 100.0)

    def _touch_quality_score(self, touch_count: int, reaction_count: int, break_count: int, crossed_count: int) -> float:
        if touch_count <= 0:
            return 65.0
        respect_ratio = reaction_count / max(touch_count, 1)
        score = respect_ratio * 100.0
        score -= break_count * 18.0
        score -= crossed_count * 12.0
        if 1 <= touch_count <= 3 and reaction_count:
            score += 12.0
        return self._clamp(score)

    def _recency_score(self, reference: Any, latest: Any) -> float:
        age = self._trading_day_age(reference, latest)
        if age <= self.cfg.smart_recent_trading_days:
            return 100.0
        return self._clamp(100.0 - ((age - self.cfg.smart_recent_trading_days) * 4.0))

    def _htf_visibility_score(self, raw: dict[str, Any], width: float, atr: float) -> float:
        zone_type = str(raw["zone_type"])
        if "gap" in zone_type:
            return 100.0
        if "liquidity" in zone_type:
            return 85.0
        if "swing" in zone_type:
            return 75.0
        if width >= atr:
            return 70.0
        return 50.0

    def _volume_score(self, rows: pd.DataFrame, index: int) -> float:
        if "volume" not in rows.columns:
            return 50.0
        volume = pd.to_numeric(rows["volume"], errors="coerce").fillna(0)
        if float(volume.max()) <= 0:
            return 50.0
        median = float(volume.iloc[max(0, index - 20) : index + 1].median() or 0)
        current = float(volume.iloc[index])
        if median <= 0:
            return 50.0
        return self._clamp((current / median) * 50.0)

    def _overlap_score(self, raw: dict[str, Any], all_raw: list[dict[str, Any]], types: set[str]) -> float:
        for other in all_raw:
            if other is raw or str(other["zone_type"]) not in types:
                continue
            if self._overlaps(float(raw["low"]), float(raw["high"]), float(other["low"]), float(other["high"])):
                return 100.0
        return 0.0

    def _liquidity_sweep_score(
        self,
        rows: pd.DataFrame,
        low: float,
        high: float,
        direction: str,
        raw: dict[str, Any],
    ) -> float:
        if "liquidity" in str(raw["zone_type"]):
            return 100.0
        after = rows.iloc[int(raw["index"]) + 1 :]
        if after.empty:
            return 0.0
        if direction == "bullish":
            swept = (after["low"] < low) & (after["close"] > low)
        else:
            swept = (after["high"] > high) & (after["close"] < high)
        return 80.0 if bool(swept.any()) else 0.0

    def _noise_penalty(
        self,
        width: float,
        atr: float,
        break_count: int,
        crossed_count: int,
        reaction_score: float,
    ) -> float:
        penalty = break_count * 10.0 + crossed_count * 8.0
        if width > self.cfg.smart_max_zone_width_points or width > atr * 6:
            penalty += 18.0
        if width < self.cfg.smart_min_zone_width_points and reaction_score < 80:
            penalty += 12.0
        if reaction_score < 35:
            penalty += 20.0
        return penalty

    def _final_score(self, **scores: float) -> float:
        weights = self.cfg.smart_level_weights
        total = sum(float(scores.get(name, 0.0)) * float(weight) for name, weight in weights.items())
        total -= float(scores.get("noise_penalty", 0.0))
        return round(self._clamp(total), 2)

    def _is_temp_strong_move_points(self, reaction_move_points: float) -> bool:
        return (
            bool(getattr(self.cfg, "smart_temp_strong_move_zone_enabled", False))
            and float(reaction_move_points) >= float(getattr(self.cfg, "smart_temp_strong_move_points", 100.0))
        )

    def _is_temp_strong_move_zone(self, zone: SmartZone) -> bool:
        return bool(getattr(self.cfg, "smart_temp_strong_move_zone_enabled", False)) and any(
            str(note).startswith("TEMP strong move zone:") for note in zone.notes
        )

    def _result(self, zones: list[SmartZone], current_price: float, atr: float, rows: pd.DataFrame) -> SmartLevelResult:
        supports = [
            zone
            for zone in zones
            if self._is_support_zone(zone) and zone.high <= current_price
        ]
        resistances = [
            zone
            for zone in zones
            if self._is_resistance_zone(zone) and zone.low >= current_price
        ]
        supports = sorted(supports, key=lambda zone: current_price - zone.high)[:3]
        resistances = sorted(resistances, key=lambda zone: zone.low - current_price)[:3]
        latest = rows.iloc[-1]["datetime"]
        recent = [
            zone
            for zone in zones
            if zone.last_touched_at is not None
            and self._trading_day_age(zone.last_touched_at, latest) <= self.cfg.smart_recent_trading_days
        ]
        fresh = [zone for zone in zones if zone.status == "fresh" and zone.touch_count == 0]
        return SmartLevelResult(
            current_price=current_price,
            atr=atr,
            zones=zones,
            nearest_support_demand=supports,
            nearest_resistance_supply=resistances,
            strongest_zones=zones[:10],
            recently_touched_zones=recent,
            fresh_untested_zones=fresh,
        )

    def _merge_cluster(self, zones: list[SmartZone], atr: float) -> SmartZone:
        if len(zones) == 1:
            return zones[0]
        parent = max(zones, key=self._parent_rank)
        others = [zone for zone in zones if zone is not parent]
        max_expansion = max(float(atr) * 0.25, self.cfg.smart_min_zone_width_points * 0.5)
        low, high = self._merged_cluster_bounds(zones, parent, max_expansion)
        zone_types = parent.zone_type
        notes = [*parent.notes]
        for zone in others:
            zone_types = self._merge_tags(zone_types, zone.zone_type)
            for note in zone.notes:
                if note not in notes:
                    notes.append(note)
            notes.append(f"absorbed {zone.zone_id}")
        absorbed_touch_count = sum(zone.touch_count for zone in others)
        absorbed_reaction_count = sum(zone.reaction_count for zone in others)
        absorbed_break_count = sum(zone.break_count for zone in others)
        notes.append(
            f"absorbed_metadata touches={absorbed_touch_count} reactions={absorbed_reaction_count} breaks={absorbed_break_count}"
        )
        notes.append(f"cluster_quality={self._cluster_quality(zones, parent, low, high)}")
        enhancers = self._merge_enhancers(zones)
        merged_scores = {
            "reaction_score": max(zone.reaction_score for zone in zones),
            "speed_score": max(zone.speed_score for zone in zones),
            "touch_quality_score": max(zone.touch_quality_score for zone in zones),
            "freshness_score": max(zone.freshness_score for zone in zones),
            "recency_score": max(zone.recency_score for zone in zones),
            "htf_visibility_score": max(zone.htf_visibility_score for zone in zones),
            "volume_score": max(zone.volume_score for zone in zones),
            "gap_overlap_score": max(zone.gap_overlap_score for zone in zones),
            "liquidity_sweep_score": max(zone.liquidity_sweep_score for zone in zones),
            "noise_penalty": min(zone.noise_penalty for zone in zones),
        }
        score = self._final_score(**merged_scores)
        if any(self._is_temp_strong_move_zone(zone) for zone in zones):
            score = max(score, float(self.cfg.smart_temp_strong_move_min_score))
        return SmartZone(
            zone_id=parent.zone_id,
            zone_type=zone_types,
            low=low,
            high=high,
            midpoint=round((low + high) / 2, 2),
            created_at=min(zone.created_at for zone in zones),
            last_touched_at=max(
                [zone.last_touched_at for zone in zones if zone.last_touched_at is not None],
                default=None,
            ),
            touch_count=parent.touch_count,
            reaction_count=parent.reaction_count,
            break_count=parent.break_count,
            score=score,
            freshness_score=round(merged_scores["freshness_score"], 2),
            recency_score=round(merged_scores["recency_score"], 2),
            reaction_score=round(merged_scores["reaction_score"], 2),
            speed_score=round(merged_scores["speed_score"], 2),
            touch_quality_score=round(merged_scores["touch_quality_score"], 2),
            htf_visibility_score=round(merged_scores["htf_visibility_score"], 2),
            volume_score=round(merged_scores["volume_score"], 2),
            gap_overlap_score=round(merged_scores["gap_overlap_score"], 2),
            liquidity_sweep_score=round(merged_scores["liquidity_sweep_score"], 2),
            noise_penalty=round(merged_scores["noise_penalty"], 2),
            status=parent.status,
            notes=notes,
            enhancers=enhancers,
        )

    @staticmethod
    def _merge_enhancers(zones: list[SmartZone]) -> dict[str, Any]:
        if not zones:
            return {}
        keys = [
            "move_strength",
            "base_candle_count",
            "freshness",
            "risk_reward_space",
            "original_zone",
            "htf_overlap",
        ]
        merged: dict[str, Any] = {}
        for key in keys:
            candidates = [zone.enhancers.get(key) for zone in zones if zone.enhancers.get(key)]
            if candidates:
                merged[key] = max(candidates, key=lambda item: float(item.get("points") or 0))
        merged["total_points"] = round(sum(float(merged.get(key, {}).get("points") or 0) for key in keys), 2)
        merged["max_points"] = 14.0
        return merged

    @staticmethod
    def _parent_rank(zone: SmartZone) -> tuple[float, float, float, float, float, float, float]:
        width = max(zone.high - zone.low, 0.01)
        return (
            1.0 if LevelEngine._has_any_zone_type(zone, DECISION_TYPES) else 0.0,
            float(zone.score),
            float(zone.reaction_score),
            float(zone.freshness_score),
            -float(zone.break_count),
            -width,
            float(zone.recency_score),
        )

    def _merged_cluster_bounds(
        self,
        zones: list[SmartZone],
        parent: SmartZone,
        max_expansion: float,
    ) -> tuple[float, float]:
        if self._has_any_zone_type(parent, DECISION_TYPES):
            decision_zones = [zone for zone in zones if self._has_any_zone_type(zone, DECISION_TYPES)]
            boundary_decision_zones = [
                zone
                for zone in decision_zones
                if zone is parent or self._is_boundary_quality_decision_zone(zone)
            ]
            context_zones = [zone for zone in zones if zone not in decision_zones]
            low = min(zone.low for zone in boundary_decision_zones)
            high = max(zone.high for zone in boundary_decision_zones)
            if len(boundary_decision_zones) > 1:
                low = max(low, parent.low - max_expansion)
                high = min(high, parent.high + max_expansion)
            for zone in context_zones:
                low, high = self._decision_context_bounds(low, high, zone, parent, max_expansion)
            return round(low, 2), round(high, 2)

        low = max(min(zone.low for zone in zones), parent.low - max_expansion)
        high = min(max(zone.high for zone in zones), parent.high + max_expansion)
        return round(low, 2), round(high, 2)

    def _decision_context_bounds(
        self,
        low: float,
        high: float,
        zone: SmartZone,
        parent: SmartZone,
        max_expansion: float,
    ) -> tuple[float, float]:
        if self._has_any_zone_type(zone, LIQUIDITY_CONTEXT_TYPES):
            return low, high
        near_or_overlapping = zone.high >= parent.low - max_expansion and zone.low <= parent.high + max_expansion
        if not near_or_overlapping:
            return low, high
        if self._is_support_zone(parent) and self._has_any_zone_type(zone, {"swing_low"}):
            return min(low, zone.low), high
        if self._is_resistance_zone(parent) and self._has_any_zone_type(zone, {"swing_high"}):
            return low, max(high, zone.high)
        zone_width = max(zone.high - zone.low, 0.01)
        parent_width = max(parent.high - parent.low, 0.01)
        if zone_width > parent_width * 1.35:
            return low, high
        return (
            min(low, max(zone.low, parent.low - max_expansion)),
            max(high, min(zone.high, parent.high + max_expansion)),
        )

    def _is_boundary_quality_decision_zone(self, zone: SmartZone) -> bool:
        return (
            zone.score >= self.cfg.smart_min_zone_score
            and zone.break_count <= self.cfg.smart_max_allowed_breaks
            and zone.status != "broken"
        )

    def _cluster_quality(self, zones: list[SmartZone], parent: SmartZone, low: float, high: float) -> str:
        has_support = any(self._is_support_zone(zone) for zone in zones)
        has_resistance = any(self._is_resistance_zone(zone) for zone in zones)
        width = high - low
        parent_width = parent.high - parent.low
        if has_support and has_resistance:
            return "conflicted"
        if width > max(parent_width * 1.6, self.cfg.smart_max_zone_width_points):
            return "wide"
        if len(zones) >= 3:
            return "high_confluence"
        return "clean"

    def _cluster_tolerance(self, atr: float, strict: bool) -> float:
        tolerance = max(float(atr) * self.cfg.smart_cluster_atr_multiplier, 0.0)
        if not strict:
            return tolerance
        tight_cap = self.cfg.smart_max_zone_width_points * 0.25
        tight_floor = self.cfg.smart_min_zone_width_points * 0.25
        return max(min(tolerance, tight_cap), tight_floor)

    def _merge_compatible_clusters(self, clusters: list[list[SmartZone]], tolerance: float, max_cluster_span: float) -> list[list[SmartZone]]:
        merged = True
        while merged:
            merged = False
            for left_index in range(len(clusters)):
                if merged:
                    break
                for right_index in range(left_index + 1, len(clusters)):
                    if self._clusters_can_merge(clusters[left_index], clusters[right_index], tolerance, max_cluster_span):
                        clusters[left_index].extend(clusters.pop(right_index))
                        merged = True
                        break
        return clusters

    def _cluster_can_accept(self, cluster: list[SmartZone], zone: SmartZone, tolerance: float, max_cluster_span: float) -> bool:
        cluster_low = min(item.low for item in cluster)
        cluster_high = max(item.high for item in cluster)
        overlaps = zone.low <= cluster_high + tolerance and zone.high >= cluster_low - tolerance
        combined_span = max(cluster_high, zone.high) - min(cluster_low, zone.low)
        compatible = any(self._can_merge_zones(existing, zone) for existing in cluster)
        return overlaps and compatible and combined_span <= max_cluster_span

    def _dedupe_merged_zones(
        self,
        zones: list[SmartZone],
        atr: float,
        tolerance: float,
        max_cluster_span: float,
    ) -> list[SmartZone]:
        out: list[SmartZone] = []
        for zone in sorted(zones, key=lambda item: (item.low, item.high, -item.score)):
            match_index = None
            for index, existing in enumerate(out):
                combined_span = max(existing.high, zone.high) - min(existing.low, zone.low)
                overlaps = zone.low <= existing.high + tolerance and zone.high >= existing.low - tolerance
                if overlaps and combined_span <= max_cluster_span and self._can_merge_zones(existing, zone):
                    match_index = index
                    break
            if match_index is None:
                out.append(zone)
            else:
                out[match_index] = self._merge_cluster([out[match_index], zone], atr)
        return out

    def _clusters_can_merge(self, first: list[SmartZone], second: list[SmartZone], tolerance: float, max_cluster_span: float) -> bool:
        first_low = min(zone.low for zone in first)
        first_high = max(zone.high for zone in first)
        second_low = min(zone.low for zone in second)
        second_high = max(zone.high for zone in second)
        overlaps = first_low <= second_high + tolerance and second_low <= first_high + tolerance
        combined_span = max(first_high, second_high) - min(first_low, second_low)
        compatible = any(self._can_merge_zones(left, right) for left in first for right in second)
        return overlaps and compatible and combined_span <= max_cluster_span

    def _raw_zone(
        self,
        zone_type: str,
        low: float,
        high: float,
        index: int,
        direction: str,
        notes: list[str],
        **metadata: Any,
    ) -> dict[str, Any]:
        low, high = sorted((float(low), float(high)))
        payload = {
            "zone_type": zone_type,
            "low": round(low, 2),
            "high": round(high, 2),
            "index": int(index),
            "direction": direction,
            "notes": notes,
        }
        payload.update(metadata)
        return payload

    def _expanded_zone(self, low: float, high: float, index: int, rows: pd.DataFrame) -> tuple[float, float]:
        width = max(high - low, self._zone_width(rows, index))
        midpoint = (low + high) / 2
        return midpoint - width / 2, midpoint + width / 2

    def _zone_width(self, rows: pd.DataFrame, index: int) -> float:
        atr = max(float(rows.iloc[index].get("atr") or self._latest_atr(rows)), 1.0)
        return float(
            min(
                max(atr * self.cfg.smart_zone_atr_multiplier, self.cfg.smart_min_zone_width_points),
                self.cfg.smart_max_zone_width_points,
            )
        )

    def _latest_atr(self, rows: pd.DataFrame) -> float:
        if rows.empty:
            return 0.0
        if "atr" in rows.columns:
            value = pd.to_numeric(rows["atr"], errors="coerce").dropna()
            if not value.empty:
                return max(float(value.iloc[-1]), 0.01)
        atr = self.calculate_atr(rows)
        return max(float(atr.iloc[-1]) if not atr.empty else 0.0, 0.01)

    def _rows(self, candles: pd.DataFrame) -> pd.DataFrame:
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
        rows = rows.dropna(subset=["open", "high", "low", "close"])
        rows = rows.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
        rows["date"] = rows["datetime"].dt.date
        rows["time"] = rows["datetime"].dt.strftime("%H:%M")
        return rows

    def _session_rows(self, candles: pd.DataFrame) -> pd.DataFrame:
        rows = self._rows(candles)
        if rows.empty:
            return rows
        return rows[(rows["time"] >= "09:15") & (rows["time"] <= "15:30")].reset_index(drop=True)

    @staticmethod
    def _zone_id(zone_type: str, low: float, high: float, created_at: Any) -> str:
        created = pd.to_datetime(created_at).strftime("%Y%m%d%H%M")
        return f"{zone_type}:{created}:{round(low, 2)}:{round(high, 2)}"

    @staticmethod
    def _status(touch_count: int, break_count: int) -> str:
        if break_count > 3:
            return "broken"
        if break_count > 0:
            return "weakened"
        if touch_count == 0:
            return "fresh"
        if touch_count <= 2:
            return "tested"
        return "active"

    @staticmethod
    def _overlaps(first_low: float, first_high: float, second_low: float, second_high: float) -> bool:
        return first_low <= second_high and second_low <= first_high

    @staticmethod
    def _merge_tags(first: str, second: str) -> str:
        tags: list[str] = []
        for value in [*first.split("+"), *second.split("+")]:
            if value and value not in tags:
                tags.append(value)
        return "+".join(tags)

    @staticmethod
    def _is_support_zone(zone: SmartZone) -> bool:
        return any(tag in SUPPORT_TYPES for tag in zone.zone_type.split("+"))

    @staticmethod
    def _is_resistance_zone(zone: SmartZone) -> bool:
        return any(tag in RESISTANCE_TYPES for tag in zone.zone_type.split("+"))

    @staticmethod
    def _has_any_zone_type(zone: SmartZone, zone_types: set[str]) -> bool:
        return any(tag in zone_types for tag in zone.zone_type.split("+"))

    @classmethod
    def _can_merge_zones(cls, first: SmartZone, second: SmartZone) -> bool:
        first_support = cls._is_support_zone(first)
        second_support = cls._is_support_zone(second)
        first_resistance = cls._is_resistance_zone(first)
        second_resistance = cls._is_resistance_zone(second)
        if first_support and second_resistance:
            return False
        if first_resistance and second_support:
            return False
        return True

    @staticmethod
    def _trading_day_age(start: Any, end: Any) -> int:
        start_date = pd.to_datetime(start).date()
        end_date = pd.to_datetime(end).date()
        return max((end_date - start_date).days, 0)

    @staticmethod
    def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
        if not math.isfinite(float(value)):
            return low
        return min(max(float(value), low), high)


def is_bullish_rejection(candle, zone) -> bool:
    return False


def is_bearish_rejection(candle, zone) -> bool:
    return False


def is_liquidity_sweep(candles, zone, direction) -> bool:
    return False


def is_market_structure_shift(candles, direction) -> bool:
    return False
