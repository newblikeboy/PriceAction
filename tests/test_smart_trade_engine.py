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


def test_temporary_freshness_filter_is_enabled_by_default() -> None:
    cfg = StrategyConfig()

    assert cfg.smart_temp_freshness_filter_enabled is True
    assert cfg.smart_temp_min_freshness_enhancer == 1.5
    assert "SMART_ZONE_RETEST_CONFIRMATION" in cfg.smart_temp_freshness_filter_setups
    assert "SMART_ZONE_REJECTION_OVERRIDE" not in cfg.smart_temp_freshness_filter_setups


def test_temporary_freshness_filter_blocks_only_configured_setups() -> None:
    engine = SmartTradeEngine(_test_config())
    zone = _zone("swing_high", 100, 120, score=90)
    zone.enhancers = {"freshness": {"points": 1.0}, "total_points": 10.0, "max_points": 14.0}

    assert engine._freshness_filter_reason("SMART_ZONE_RETEST_CONFIRMATION", zone) == "Temporary freshness filter blocked smart-zone setup"
    assert engine._freshness_filter_reason("SMART_ZONE_REJECTION_OVERRIDE", zone) is None
    assert engine._freshness_filter_reason("SMART_ZONE_SWEEP_RECLAIM_DISPLACEMENT", zone) is None


def test_temporary_freshness_filter_allows_unknown_or_fresh_zones() -> None:
    engine = SmartTradeEngine(_test_config())
    unknown_zone = _zone("swing_high", 100, 120, score=90)
    fresh_zone = _zone("swing_high", 100, 120, score=90)
    fresh_zone.enhancers = {"freshness": {"points": 1.5}, "total_points": 10.0, "max_points": 14.0}

    assert engine._freshness_filter_reason("SMART_ZONE_RETEST_CONFIRMATION", unknown_zone) is None
    assert engine._freshness_filter_reason("SMART_ZONE_RETEST_CONFIRMATION", fresh_zone) is None


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
    immediate = next(item for item in signals if item.setup_type == "SMART_ZONE_BREAK_CONFIRMATION")
    assert retest.time == "10:50"
    assert retest.features["entry_model"] == "break_confirm_retest"
    assert retest.setup_score >= immediate.setup_score


def test_smart_trade_blocks_support_reaction_without_structure_or_fvg() -> None:
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _support_reaction_candles()
    levels = _wide_levels()

    signals, skipped = engine.generate_for_day(candles, levels, pd.Timestamp("2024-01-01").date())

    assert not [item for item in signals if item.setup_type == "SMART_ZONE_SUPPORT_REACTION_CONFIRMATION"]
    assert [item for item in skipped if item.skip_reason == "Reaction lacks structure shift or unmitigated directional FVG"]


def test_smart_trade_does_not_emit_removed_flip_retest_setup() -> None:
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _support_flipped_resistance_candles()
    levels = _short_favorable_levels()

    signals, _ = engine.generate_for_day(candles, levels, pd.Timestamp("2024-01-01").date())

    assert not [item for item in signals if item.setup_type == "SMART_ZONE_FLIP_RETEST_CONFIRMATION"]


def test_smart_trade_blocks_reaction_without_structure_or_fvg_before_pd() -> None:
    zone = _zone("breakdown_base+swing_high", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _resistance_rejection_candles()
    levels = _invalid_pd_levels()
    all_rows = engine._rows(candles)
    day_rows = all_rows[all_rows["date"] == pd.Timestamp("2024-01-01").date()].reset_index(drop=True)
    row_index = len(day_rows) - 1
    row = day_rows.iloc[row_index]

    signal, reason, context = engine._build_signal(
        direction="PE",
        setup="SMART_ZONE_RESISTANCE_REJECTION_CONFIRMATION",
        all_rows=all_rows,
        day_rows=day_rows,
        row_index=row_index,
        row=row,
        break_row=row,
        zone=zone,
        levels=levels,
        atr=10,
        entry_model="resistance_rejection",
        htf_context={"enabled": True, "bias": "bearish", "reason": "test"},
        target_zones=[zone],
    )

    assert signal is None
    assert reason == "Reaction lacks structure shift or unmitigated directional FVG"
    assert context["fair_value_gap"]["present"] is False


def test_smart_trade_blocks_counter_pd_break_without_structure_confirmation() -> None:
    zone = _zone("breakdown_base+swing_high", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _resistance_flipped_support_candles()
    levels = _short_premium_levels()
    all_rows = engine._rows(candles)
    day_rows = all_rows[all_rows["date"] == pd.Timestamp("2024-01-01").date()].reset_index(drop=True)
    row_index = len(day_rows) - 1
    row = day_rows.iloc[row_index]

    signal, reason, context = engine._build_signal(
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
        htf_context={"enabled": True, "bias": "bullish", "reason": "test"},
        target_zones=[zone],
    )

    assert signal is None
    assert reason == "Counter-PD zone break needs structure confirmation"
    assert context["premium_discount"]["zone"] == "premium"


def test_smart_trade_blocks_compressed_forward_space() -> None:
    zone = _zone("breakout_base+swing_low", 100, 120, score=90)
    zone.enhancers = {"risk_reward_space": {"space_width_ratio": 0.07}}
    engine = SmartTradeEngine(_test_config())

    assert engine._forward_space_filter_reason("SMART_ZONE_BREAK_CONFIRMATION", zone) == "Forward space to opposing zone is too compressed"


def test_smart_trade_blocks_low_score_retest() -> None:
    zone = _zone("swing_low", 100, 120, score=74)
    engine = SmartTradeEngine(_test_config())
    candles = _resistance_flipped_support_candles()
    levels = _short_premium_levels()
    all_rows = engine._rows(candles)
    day_rows = all_rows[all_rows["date"] == pd.Timestamp("2024-01-01").date()].reset_index(drop=True)
    row_index = len(day_rows) - 1
    row = day_rows.iloc[row_index]

    signal, reason, context = engine._build_signal(
        direction="PE",
        setup="SMART_ZONE_RETEST_CONFIRMATION",
        all_rows=all_rows,
        day_rows=day_rows,
        row_index=row_index,
        row=row,
        break_row=row,
        zone=zone,
        levels=levels,
        atr=10,
        entry_model="break_confirm_retest",
        htf_context={"enabled": True, "bias": "bearish", "reason": "test"},
        target_zones=[zone],
    )

    assert signal is None
    assert reason == "Smart-zone retest setup score below retest threshold"
    assert context["required_score"] == 75


def test_smart_trade_blocks_weak_resistance_rejection_before_htf_override() -> None:
    zone = _zone("breakdown_base+swing_high", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _resistance_rejection_candles()
    levels = _invalid_pd_levels()
    bullish_htf = {"enabled": True, "bias": "bullish", "reason": "test opposing HTF"}
    all_rows = engine._rows(candles)
    day_rows = all_rows[all_rows["date"] == pd.Timestamp("2024-01-01").date()].reset_index(drop=True)
    row_index = len(day_rows) - 1
    row = day_rows.iloc[row_index]

    signal, reason, context = engine._build_signal(
        direction="PE",
        setup="SMART_ZONE_RESISTANCE_REJECTION_CONFIRMATION",
        all_rows=all_rows,
        day_rows=day_rows,
        row_index=row_index,
        row=row,
        break_row=row,
        zone=zone,
        levels=levels,
        atr=10,
        entry_model="resistance_rejection",
        htf_context=bullish_htf,
        target_zones=[zone],
    )

    assert signal is None
    assert reason == "Reaction lacks structure shift or unmitigated directional FVG"
    assert context["fair_value_gap"]["present"] is False


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
    assert cfg.smart_trade_continuation_min_zone_score == 65.0


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


def test_trend_continuation_rejects_limp_resume_candle() -> None:
    # Filter B: a doji/limp resume candle (body% below threshold) must not qualify.
    engine = SmartTradeEngine(_test_config())
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    rows = [
        _candle(pd.Timestamp("2024-01-01 09:15"), 122, 126, 121, 124),
        _candle(pd.Timestamp("2024-01-01 09:20"), 110, 122, 103, 112),  # body 2 / range 19 = 0.11
    ]
    frame = pd.DataFrame(rows)
    frame["date"] = frame["datetime"].dt.date
    frame["time"] = frame["datetime"].dt.strftime("%H:%M")
    day_rows = engine._rows(frame)
    confirm_index = len(day_rows) - 1

    assert engine._trend_continuation_setup(zone, day_rows, confirm_index, "up") is None


def test_trend_continuation_requires_aligned_htf() -> None:
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
        htf_context={"enabled": True, "bias": "neutral", "reason": "test"},
        target_zones=[zone],
    )

    assert signal is None
    assert reason == "Trend continuation needs aligned HTF bias"


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


def test_trend_continuation_blocks_counter_premium_discount() -> None:
    # Filter A: a CE continuation in premium (counter-PD) must be blocked.
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _breakout_candles(include_retest=False)
    levels = _short_premium_levels()
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

    assert signal is None
    assert reason == "Trend continuation is against premium/discount context"


def test_trend_continuation_requires_both_htf_timeframes() -> None:
    # Filter C: combined bias bullish but 15m neutral must be blocked.
    zone = _zone("swing_low+breakout_base", 100, 120, score=90)
    engine = FixedZoneSmartTradeEngine(_test_config(), [zone])
    candles = _breakout_candles(include_retest=False)
    levels = _levels()
    all_rows = engine._rows(candles)
    day_rows = all_rows[all_rows["date"] == pd.Timestamp("2024-01-01").date()].reset_index(drop=True)
    row_index = len(day_rows) - 1
    row = day_rows.iloc[row_index]

    half_aligned = {
        "enabled": True,
        "bias": "bullish",
        "reason": "60m directional, 15m neutral",
        "15m": {"bias": "neutral"},
        "60m": {"bias": "bullish"},
    }
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
        htf_context=half_aligned,
        target_zones=[zone],
    )

    assert signal is None
    assert reason == "Trend continuation needs 15m and 60m HTF both aligned"


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
