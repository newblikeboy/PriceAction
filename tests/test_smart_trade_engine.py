from __future__ import annotations

from dataclasses import replace

import pandas as pd

from app.config import StrategyConfig
from app.domain import LevelSet, SmartZone
from app.engines.signals import SignalEngine
from app.engines.smart_trades import SmartTradeEngine


class FixedZoneSmartTradeEngine(SmartTradeEngine):
    def __init__(self, cfg: StrategyConfig, zones: list[SmartZone]) -> None:
        super().__init__(cfg)
        self.fixed_zones = zones

    def _known_zones(self, history: pd.DataFrame, current_price: float, as_of, trading_date=None) -> list[SmartZone]:
        return self.fixed_zones


def test_trade_zone_history_lookback_is_two_days() -> None:
    assert StrategyConfig().smart_trade_zone_history_days == 2


def test_smart_trade_history_uses_previous_two_trading_days() -> None:
    engine = SmartTradeEngine(_test_config())
    rows = _history_rows(
        [
            "2024-01-04 09:15",
            "2024-01-05 09:15",
            "2024-01-08 09:15",
            "2024-01-09 09:15",
        ]
    )

    history = engine._history_before(rows, pd.Timestamp("2024-01-09 09:20"))

    assert [str(day) for day in sorted(history["date"].unique())] == [
        "2024-01-05",
        "2024-01-08",
        "2024-01-09",
    ]


def test_smart_trade_requires_break_plus_confirmation() -> None:
    zone = _zone("swing_high", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _breakout_candles(include_retest=False)
    levels = _levels()

    signals, skipped = engine.generate_for_day(candles, levels, pd.Timestamp("2024-01-01").date())

    assert not [item for item in skipped if item.skip_reason == "No one-candle 5m confirmation after zone break"]
    signal = next(item for item in signals if item.setup_type == "SMART_ZONE_BREAK_CONFIRMATION")
    assert signal.direction == "CE"
    assert signal.time == "10:40"
    assert signal.entry_index_price == 126.0
    assert zone.low < signal.sl_index_price < signal.entry_index_price
    assert signal.features["original_SL_price"] < zone.low
    assert signal.features["smart_zone_sl_model"] == "zone_inner_fraction"
    assert signal.target_index_price == 190.0
    assert signal.features["smart_trade_grade"] in {"A+", "A", "B"}
    assert signal.features["entry_model"] == "break_confirmation"
    assert "smart_zone_enhancer_total" in signal.features
    assert "smart_zone_enhancer_points" in signal.features


def test_smart_trade_detects_retest_confirmation() -> None:
    zone = _zone("swing_high", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _breakout_candles(include_retest=True)
    levels = _levels()

    signals, _ = engine.generate_for_day(candles, levels, pd.Timestamp("2024-01-01").date())

    retest = next(item for item in signals if item.setup_type == "SMART_ZONE_RETEST_CONFIRMATION")
    # break-confirmation must also be present; both are scored by the same uniform
    # confluence (no retest bonus), so we no longer assert one outscores the other.
    assert any(item.setup_type == "SMART_ZONE_BREAK_CONFIRMATION" for item in signals)
    assert retest.time == "10:50"
    assert retest.features["entry_model"] == "break_confirm_retest"
    assert 0 <= retest.setup_score <= 100


def test_smart_trade_does_not_emit_removed_flip_retest_setup() -> None:
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _support_flipped_resistance_candles()
    levels = _short_favorable_levels()

    signals, _ = engine.generate_for_day(candles, levels, pd.Timestamp("2024-01-01").date())

    assert not [item for item in signals if item.setup_type == "SMART_ZONE_FLIP_RETEST_CONFIRMATION"]


def test_htf_bias_hard_gate_blocks_trade_against_bias() -> None:
    # HTF bias is now a single uniform hard gate (no per-setup override hatch):
    # a CE setup with an opposing (bearish) HTF bias must be blocked.
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _breakout_candles(include_retest=False)
    levels = _levels()
    all_rows = engine._rows(candles)
    day_rows = all_rows[all_rows["date"] == pd.Timestamp("2024-01-01").date()].reset_index(drop=True)
    row_index = len(day_rows) - 1
    row = day_rows.iloc[row_index]

    signal, reason, _ = engine._build_signal(
        direction="CE",
        setup="SMART_ZONE_BREAK_CONFIRMATION",
        all_rows=all_rows,
        day_rows=day_rows,
        row_index=row_index,
        row=row,
        break_row=row,
        zone=zone,
        levels=levels,
        atr=10,
        entry_model="break_confirmation",
        htf_context={"enabled": True, "bias": "bearish", "reason": "opposing HTF"},
        target_zones=[zone],
    )

    assert signal is None
    assert reason == "HTF bias filter blocked smart-zone setup"


def test_smart_trade_does_not_emit_removed_sweep_reclaim_setup() -> None:
    zone = _zone("breakout_base+swing_low", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _sweep_reclaim_displacement_candles()
    levels = _wide_levels()

    signals, _ = engine.generate_for_day(candles, levels, pd.Timestamp("2024-01-01").date())

    assert not [item for item in signals if item.setup_type == "SMART_ZONE_SWEEP_RECLAIM_DISPLACEMENT"]


def test_trend_continuation_enabled_by_default() -> None:
    cfg = StrategyConfig()

    assert cfg.smart_trade_continuation_enabled is True
    assert cfg.smart_trade_continuation_pullback_lookback == 4


def test_trend_continuation_detects_bullish_pullback() -> None:
    engine = SmartTradeEngine(_test_config())
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    rows = [
        _candle(pd.Timestamp("2024-01-01 09:15"), 122, 126, 121, 124),
        _candle(pd.Timestamp("2024-01-01 09:20"), 105, 120, 103, 118),
    ]
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    day_rows = engine._rows(frame)
    confirm_index = len(day_rows) - 1

    assert engine._trend_continuation_setup(zone, day_rows, confirm_index, "up") == (
        "SMART_ZONE_TREND_CONTINUATION",
        "CE",
        "trend_continuation",
    )
    # No detection when there is no established trend or when disabled.
    assert engine._trend_continuation_setup(zone, day_rows, confirm_index, "range") is None
    disabled = SmartTradeEngine(_test_config_with(smart_trade_continuation_enabled=False))
    assert disabled._trend_continuation_setup(zone, day_rows, confirm_index, "up") is None


def test_trend_continuation_emits_with_aligned_htf() -> None:
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _breakout_candles(include_retest=False)
    levels = _levels()
    all_rows = engine._rows(candles)
    day_rows = all_rows[all_rows["date"] == pd.Timestamp("2024-01-01").date()].reset_index(drop=True)
    row_index = len(day_rows) - 1
    row = day_rows.iloc[row_index]

    signal, reason, _ = engine._build_signal(
        direction="CE",
        setup="SMART_ZONE_TREND_CONTINUATION",
        all_rows=all_rows,
        day_rows=day_rows,
        row_index=row_index,
        row=row,
        break_row=row,
        zone=zone,
        levels=levels,
        atr=10,
        entry_model="trend_continuation",
        htf_context=_aligned_htf("bullish"),
        target_zones=[zone],
    )

    assert reason is None
    assert signal is not None
    assert signal.setup_type == "SMART_ZONE_TREND_CONTINUATION"
    assert signal.direction == "CE"
    assert signal.features["entry_model"] == "trend_continuation"


def test_signal_engine_is_smart_zone_only() -> None:
    cfg = StrategyConfig(
        smart_trade_enabled=False,
        min_setup_score=1,
        htf_bias_filter_enabled=False,
        premium_discount_filter_enabled=False,
    )
    engine = SignalEngine(cfg)
    candles = _breakout_candles(include_retest=False)
    levels = _levels()

    signals, skipped = engine.generate_for_day(candles, levels, pd.Timestamp("2024-01-01").date())

    assert signals == []
    assert skipped == []


def test_confluence_score_is_normalized_fraction() -> None:
    # The score is the fraction of 8 equal-weighted structural confirmations,
    # scaled to 0-100. All present -> 100; none present -> 0; no hand-tuned weights.
    engine = SmartTradeEngine(_test_config())

    strong_zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    bull_row = pd.Series({"open": 100.0, "high": 110.0, "low": 99.0, "close": 109.0})
    all_true = engine._score(
        "SMART_ZONE_BREAK_CONFIRMATION",
        "CE",
        bull_row,
        strong_zone,
        {"direction": "bullish"},
        {"is_structure_break": True, "direction": "bullish"},
        {"present": True, "fully_mitigated": False, "direction": "bullish"},
        {"zone": "discount"},
        {"bias": "bullish"},
        3.0,
        10.0,
        10.0,
    )
    assert all_true == 100

    weak_zone = _zone("swing_high", 100, 120, score=10)
    weak_zone.touch_count = 5
    bear_row = pd.Series({"open": 109.0, "high": 110.0, "low": 99.0, "close": 100.0})
    none_true = engine._score(
        "SMART_ZONE_BREAK_CONFIRMATION",
        "CE",
        bear_row,
        weak_zone,
        {"direction": "bearish"},
        {"is_structure_break": False, "direction": None},
        {"present": False},
        {"zone": "premium"},
        {"bias": "bearish"},
        1.0,
        10.0,
        10.0,
    )
    assert none_true == 0


def _test_config() -> StrategyConfig:
    return StrategyConfig(
        min_setup_score=55,
        minimum_rr=1.5,
        smart_trade_min_zone_score=50,
        smart_trade_confirmation_window_candles=2,
        smart_trade_retest_window_candles=4,
        smart_trade_sl_atr_buffer=0.0,
        htf_bias_filter_enabled=True,
        htf_bias_allow_neutral=True,
    )


def _test_config_with(**overrides) -> StrategyConfig:
    base = _test_config()
    return replace(base, **overrides)


def _aligned_htf(bias: str) -> dict:
    return {
        "enabled": True,
        "bias": bias,
        "reason": "15m and 60m aligned",
        "15m": {"bias": bias},
        "60m": {"bias": bias},
    }


def _history_rows(timestamps: list[str]) -> pd.DataFrame:
    rows = [
        _candle(pd.Timestamp(timestamp), 100.0, 105.0, 95.0, 101.0)
        for timestamp in timestamps
    ]
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    return frame


def _breakout_candles(*, include_retest: bool) -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-01 09:15")
    price = 110.0
    for _ in range(15):
        rows.append(_candle(timestamp, price, price + 3, price - 3, price + 0.5))
        price += 0.2
        timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 118, 125, 117, 123))
    timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 123, 128, 122, 126))
    timestamp += pd.Timedelta(minutes=5)
    if include_retest:
        rows.append(_candle(timestamp, 125, 127, 118, 124))
        timestamp += pd.Timedelta(minutes=5)
        rows.append(_candle(timestamp, 121, 129, 119, 127))
    else:
        rows.append(_candle(timestamp, 126, 130, 124, 128))
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    return frame


def _support_reaction_candles() -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-01 09:15")
    price = 128.0
    for _ in range(17):
        rows.append(_candle(timestamp, price, price + 3, price - 3, price + 0.2))
        price += 0.1
        timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 112, 128, 108, 126))
    timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 126, 130, 124, 128))
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    return frame


def _support_flipped_resistance_candles() -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-01 09:15")
    price = 92.0
    for _ in range(17):
        rows.append(_candle(timestamp, price, price + 3, price - 3, price - 0.1))
        price -= 0.05
        timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 112, 118, 92, 94))
    timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 94, 96, 90, 92))
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    return frame


def _resistance_rejection_candles() -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-01 09:15")
    price = 94.0
    for _ in range(17):
        rows.append(_candle(timestamp, price, price + 3, price - 3, price - 0.1))
        price -= 0.05
        timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 116, 118, 94, 96))
    timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 96, 98, 90, 92))
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    return frame


def _resistance_flipped_support_candles() -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-01 09:15")
    price = 116.0
    for _ in range(17):
        rows.append(_candle(timestamp, price, price + 3, price - 3, price + 0.1))
        price += 0.05
        timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 118, 130, 112, 126))
    timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 126, 132, 118, 128))
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    return frame


def _sweep_reclaim_displacement_candles() -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-01 09:15")
    price = 114.0
    for _ in range(17):
        rows.append(_candle(timestamp, price, price + 3, price - 3, price + 0.1))
        price += 0.05
        timestamp += pd.Timedelta(minutes=5)
    rows.append(_candle(timestamp, 104, 132, 95, 130))
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    return frame


def _candle(timestamp: pd.Timestamp, open_: float, high: float, low: float, close: float) -> dict:
    return {
        "datetime": timestamp,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1000,
    }


def _levels() -> LevelSet:
    return LevelSet(
        trading_date=pd.Timestamp("2024-01-01").date(),
        pdh=190.0,
        pdl=90.0,
        pdc=110.0,
        orh=115.0,
        orl=105.0,
        day_high=190.0,
        day_low=100.0,
    )


def _wide_levels() -> LevelSet:
    return LevelSet(
        trading_date=pd.Timestamp("2024-01-01").date(),
        pdh=190.0,
        pdl=40.0,
        pdc=110.0,
        orh=130.0,
        orl=90.0,
        day_high=200.0,
        day_low=80.0,
    )


def _short_favorable_levels() -> LevelSet:
    return LevelSet(
        trading_date=pd.Timestamp("2024-01-01").date(),
        pdh=150.0,
        pdl=40.0,
        pdc=110.0,
        orh=130.0,
        orl=80.0,
        day_high=110.0,
        day_low=40.0,
    )


def _short_premium_levels() -> LevelSet:
    return LevelSet(
        trading_date=pd.Timestamp("2024-01-01").date(),
        pdh=130.0,
        pdl=40.0,
        pdc=110.0,
        orh=130.0,
        orl=80.0,
        day_high=130.0,
        day_low=40.0,
    )


def _invalid_pd_levels() -> LevelSet:
    return LevelSet(
        trading_date=pd.Timestamp("2024-01-01").date(),
        pdh=300.0,
        pdl=40.0,
        pdc=110.0,
        orh=300.0,
        orl=80.0,
        swing_highs=[{"price": 121.0}],
        swing_lows=[{"price": 120.0}],
        day_high=300.0,
        day_low=40.0,
    )


def _zone(zone_type: str, low: float, high: float, *, score: float) -> SmartZone:
    created_at = pd.Timestamp("2024-01-01 09:30")
    return SmartZone(
        zone_id=f"{zone_type}:{low}:{high}",
        zone_type=zone_type,
        low=low,
        high=high,
        midpoint=(low + high) / 2,
        created_at=created_at,
        last_touched_at=created_at,
        touch_count=1,
        reaction_count=1,
        break_count=0,
        score=score,
        freshness_score=80,
        recency_score=100,
        reaction_score=90,
        speed_score=80,
        touch_quality_score=85,
        htf_visibility_score=75,
        volume_score=50,
        gap_overlap_score=0,
        liquidity_sweep_score=0,
        noise_penalty=0,
        status="tested",
        notes=["test zone"],
    )
