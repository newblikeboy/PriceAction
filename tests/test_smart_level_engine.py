from __future__ import annotations

import json

import pandas as pd

from app.config import StrategyConfig
from app.data_loader import DataLoader
from app.domain import SmartZone
from app.engines.levels import LevelEngine


def test_data_loader_reports_missing_5m_session_candles() -> None:
    loader = DataLoader()
    frame = pd.DataFrame(
        [
            {"datetime": "2024-01-01 09:15", "open": 100, "high": 110, "low": 95, "close": 105},
            {"datetime": "2024-01-01 09:25", "open": 105, "high": 112, "low": 101, "close": 108},
        ]
    )

    candles = loader.validate_candles(frame, "5m")
    missing = loader.missing_candle_times(candles)

    assert pd.Timestamp("2024-01-01 09:20") in missing


def test_atr_calculation_uses_true_range() -> None:
    engine = LevelEngine()
    candles = pd.DataFrame(
        [
            {"datetime": "2024-01-01 09:15", "open": 100, "high": 110, "low": 95, "close": 105},
            {"datetime": "2024-01-01 09:20", "open": 120, "high": 125, "low": 118, "close": 121},
        ]
    )

    atr = engine.calculate_atr(candles, period=14)

    assert round(float(atr.iloc[0]), 2) == 15.0
    assert round(float(atr.iloc[1]), 2) == 17.5


def test_swing_detection_keeps_existing_shape() -> None:
    engine = LevelEngine(StrategyConfig(swing_left=1, swing_right=1))
    candles = pd.DataFrame(
        [
            {"datetime": "2024-01-01 09:15", "open": 100, "high": 105, "low": 99, "close": 101},
            {"datetime": "2024-01-01 09:20", "open": 101, "high": 115, "low": 100, "close": 110},
            {"datetime": "2024-01-01 09:25", "open": 110, "high": 111, "low": 95, "close": 98},
            {"datetime": "2024-01-01 09:30", "open": 98, "high": 106, "low": 96, "close": 104},
        ]
    )

    swings = engine.detect_swings(candles)

    assert swings["highs"][0]["price"] == 115.0
    assert swings["lows"][0]["price"] == 95.0


def test_smart_zone_creation_scoring_and_exports() -> None:
    engine = LevelEngine(_test_config())
    candles = _sample_5m_candles()

    result = engine.calculate_smart_zones(candles, current_price=22555)

    assert result.zones
    assert all(zone.low < zone.high for zone in result.zones)
    assert all(0 <= zone.score <= 100 for zone in result.zones)
    assert all("move_strength" in zone.enhancers for zone in result.zones)
    assert result.strongest_zones == result.zones[:10]

    payload = json.loads(engine.smart_zones_json(result))
    csv_payload = engine.smart_zones_csv(result)

    assert "nearest_support_demand" in payload
    assert "enhancers" in payload["zones"][0]
    assert "zone_id,zone_type" in csv_payload


def test_zone_merging_absorbs_overlapping_metadata() -> None:
    engine = LevelEngine(_test_config())
    first = _zone("demand", 100, 120, score=70, touches=1)
    second = _zone("gap_up", 115, 130, score=65, touches=2)

    merged = engine.merge_zones([first, second], atr=10)

    assert len(merged) == 1
    assert merged[0].low == 100
    assert 120 < merged[0].high < 130
    assert merged[0].touch_count == 1
    assert "demand" in merged[0].zone_type
    assert "gap_up" in merged[0].zone_type
    assert any("absorbed_metadata" in note for note in merged[0].notes)
    assert any("cluster_quality=" in note for note in merged[0].notes)


def test_zone_merging_does_not_absorb_wide_overlap_chain() -> None:
    engine = LevelEngine(_test_config())
    first = _zone("equal_lows_liquidity", 100, 125, score=90, touches=0)
    bridge = _zone("swing_low", 122, 150, score=70, touches=1)
    distant = _zone("breakout_base", 148, 205, score=85, touches=1)

    merged = engine.merge_zones([first, bridge, distant], atr=10)

    assert len(merged) == 2
    assert any(zone.zone_type.startswith("equal_lows_liquidity") for zone in merged)
    assert max(zone.high - zone.low for zone in merged) < 90


def test_decision_zone_keeps_bounds_when_absorbing_liquidity_context() -> None:
    engine = LevelEngine(_test_config())
    liquidity = _zone("equal_lows_liquidity", 23304.83, 23346.52, score=94, touches=0)
    decision = _zone("breakout_base", 23313.90, 23346.00, score=88, touches=1)

    merged = engine.merge_zones([liquidity, decision], atr=20)

    assert len(merged) == 1
    assert merged[0].low == 23313.90
    assert merged[0].high == 23346.00
    assert "breakout_base" in merged[0].zone_type
    assert "equal_lows_liquidity" in merged[0].zone_type


def test_decision_zone_uses_same_side_swing_as_protective_edge() -> None:
    engine = LevelEngine(_test_config())
    decision = _zone("breakout_base", 23329.07, 23344.70, score=88, touches=1)
    swing = _zone("swing_low", 23313.90, 23328.90, score=71, touches=3)
    broad_sweep = _zone("swing_low", 23321.60, 23388.95, score=83, touches=0)

    merged = engine.merge_zones([decision, swing, broad_sweep], atr=23.56)

    assert len(merged) == 1
    assert merged[0].low == 23313.90
    assert merged[0].high == 23344.70
    assert "breakout_base" in merged[0].zone_type
    assert "swing_low" in merged[0].zone_type


def test_weak_decision_zone_does_not_widen_clean_decision_cluster() -> None:
    engine = LevelEngine(_test_config())
    clean = _zone("breakout_base", 23329.07, 23344.70, score=88, touches=1)
    weak = _zone("breakout_base", 23344.95, 23359.95, score=0, touches=12)

    merged = engine.merge_zones([clean, weak], atr=23.56)

    assert len(merged) == 1
    assert merged[0].low == 23329.07
    assert merged[0].high == 23344.70


def test_noise_filter_removes_weak_and_distant_zones() -> None:
    engine = LevelEngine(StrategyConfig(smart_min_zone_score=55, smart_max_distance_from_current_price_atr=20))
    strong = _zone("demand", 100, 120, score=70, touches=1, reactions=1)
    weak = _zone("demand", 130, 145, score=20, touches=1, reactions=1)
    distant = _zone("supply", 1000, 1020, score=80, touches=1, reactions=1)

    filtered = engine.filter_noisy_zones([strong, weak, distant], current_price=140, atr=10)

    assert filtered == [strong]


def test_noise_filter_has_no_strong_move_escape_hatch() -> None:
    # The TEMP strong-move override was removed: a weak-score zone is filtered out
    # uniformly, regardless of how far price later travelled away from it.
    engine = LevelEngine(StrategyConfig(smart_min_zone_score=55))
    weak_but_big_move = _zone("demand", 100, 120, score=20, touches=0, reactions=0)
    weak_but_big_move.notes.append("price moved 120.0 points away")

    filtered = engine.filter_noisy_zones([weak_but_big_move], current_price=130, atr=10)

    assert filtered == []


def test_quality_zone_uses_origin_base_before_displacement() -> None:
    engine = LevelEngine(_quality_config())
    candles = _quality_displacement_candles()

    result = engine.calculate_smart_zones(candles, current_price=22645)

    origin_zones = [
        zone
        for zone in result.zones
        if "breakout_base" in zone.zone_type or "demand" in zone.zone_type
    ]
    assert origin_zones
    best = origin_zones[0]
    assert best.low <= 22500
    assert best.high < 22545
    assert any("5m demand origin" in note for note in best.notes)


def test_quality_zone_detects_liquidity_sweep_reclaim() -> None:
    engine = LevelEngine(_quality_config())
    candles = _sweep_reclaim_candles()

    result = engine.calculate_smart_zones(candles, current_price=22575)

    sweep_zones = [
        zone
        for zone in result.zones
        if any("liquidity sweep below swing low" in note for note in zone.notes)
    ]
    assert sweep_zones
    assert sweep_zones[0].low < 22490
    assert sweep_zones[0].high > sweep_zones[0].low


def _test_config() -> StrategyConfig:
    return StrategyConfig(
        swing_left=1,
        swing_right=1,
        smart_min_zone_score=10,
        smart_max_distance_from_current_price_atr=200,
        smart_max_age_days_without_touch=365,
        smart_min_reaction_atr=0.5,
        smart_min_zone_width_points=5,
        smart_max_zone_width_points=40,
    )


def _quality_config() -> StrategyConfig:
    return StrategyConfig(
        swing_left=1,
        swing_right=1,
        smart_min_zone_score=5,
        smart_max_distance_from_current_price_atr=200,
        smart_max_age_days_without_touch=365,
        smart_min_reaction_atr=0.5,
        smart_min_zone_width_points=5,
        smart_max_zone_width_points=50,
        smart_quality_displacement_atr=0.8,
        smart_quality_structure_lookback=3,
        smart_quality_max_base_candles=3,
        smart_quality_min_body_pct=0.45,
        smart_quality_swing_reaction_atr=0.7,
    )


def _sample_5m_candles() -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-01 09:15")
    prices = [
        (22500, 22510, 22490, 22500),
        (22500, 22508, 22482, 22488),
        (22488, 22502, 22480, 22496),
        (22496, 22565, 22494, 22555),
        (22555, 22610, 22550, 22600),
        (22600, 22615, 22570, 22582),
        (22582, 22620, 22576, 22605),
        (22605, 22608, 22530, 22545),
        (22545, 22555, 22510, 22518),
        (22518, 22550, 22515, 22542),
        (22542, 22570, 22538, 22560),
        (22560, 22575, 22540, 22555),
    ]
    for open_, high, low, close in prices:
        rows.append(
            {
                "datetime": timestamp,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1000,
            }
        )
        timestamp += pd.Timedelta(minutes=5)
    frame = pd.DataFrame(rows).set_index("datetime")
    frame["date"] = frame.index.date
    frame["time"] = frame.index.strftime("%H:%M")
    return frame


def _quality_displacement_candles() -> pd.DataFrame:
    prices = [
        (22530, 22535, 22515, 22520),
        (22520, 22525, 22500, 22508),
        (22508, 22518, 22495, 22502),
        (22502, 22512, 22498, 22507),
        (22507, 22518, 22500, 22510),
        (22510, 22620, 22508, 22605),
        (22605, 22655, 22595, 22645),
        (22645, 22660, 22625, 22635),
    ]
    return _frame_from_prices(prices)


def _sweep_reclaim_candles() -> pd.DataFrame:
    prices = [
        (22540, 22550, 22520, 22528),
        (22528, 22535, 22495, 22502),
        (22502, 22545, 22498, 22536),
        (22536, 22552, 22518, 22525),
        (22525, 22534, 22488, 22530),
        (22530, 22590, 22528, 22578),
        (22578, 22605, 22565, 22592),
    ]
    return _frame_from_prices(prices)


def _frame_from_prices(prices: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-01 09:15")
    for open_, high, low, close in prices:
        rows.append(
            {
                "datetime": timestamp,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1000,
            }
        )
        timestamp += pd.Timedelta(minutes=5)
    frame = pd.DataFrame(rows).set_index("datetime")
    frame["date"] = frame.index.date
    frame["time"] = frame.index.strftime("%H:%M")
    return frame


def _zone(
    zone_type: str,
    low: float,
    high: float,
    *,
    score: float,
    touches: int,
    reactions: int = 1,
) -> SmartZone:
    created_at = pd.Timestamp("2024-01-01 09:15")
    return SmartZone(
        zone_id=f"{zone_type}:{low}:{high}",
        zone_type=zone_type,
        low=low,
        high=high,
        midpoint=(low + high) / 2,
        created_at=created_at,
        last_touched_at=created_at,
        touch_count=touches,
        reaction_count=reactions,
        break_count=0,
        score=score,
        freshness_score=80,
        recency_score=100,
        reaction_score=80,
        speed_score=70,
        touch_quality_score=80,
        htf_visibility_score=70,
        volume_score=50,
        gap_overlap_score=0,
        liquidity_sweep_score=0,
        noise_penalty=0,
        status="tested",
        notes=[],
    )
