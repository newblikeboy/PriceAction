from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from app.config import StrategyConfig
from app.domain import LevelSet, SignalCandidate
from app.engines.signals import SignalEngine


def signal(entry_time: str, setup: str = "SMART_ZONE_SUPPORT_REACTION_CONFIRMATION") -> SignalCandidate:
    return SignalCandidate(
        date="2026-01-02",
        time=entry_time,
        symbol="NIFTY",
        direction="CE",
        setup_type=setup,
        entry_index_price=24000.0,
        sl_index_price=23950.0,
        target_index_price=24100.0,
        risk_points=50.0,
        reward_points=100.0,
        risk_reward=2.0,
        setup_score=80,
        features={"time": entry_time},
    )


@pytest.mark.parametrize("entry_time", ["14:00", "14:05", "15:00"])
def test_all_fresh_entries_are_blocked_from_1400(entry_time: str) -> None:
    engine = SignalEngine(StrategyConfig())

    allowed, skipped = engine._apply_entry_time_filters([signal(entry_time)], [])

    assert allowed == []
    assert skipped[0].skip_reason == "Fresh entry blocked from session cutoff"
    assert skipped[0].context["cutoff"] == "14:00"


def test_entry_before_1400_remains_allowed() -> None:
    engine = SignalEngine(StrategyConfig())
    candidate = signal("13:59")

    allowed, skipped = engine._apply_entry_time_filters([candidate], [])

    assert allowed == [candidate]
    assert skipped == []


@pytest.mark.parametrize("entry_time", ["11:00", "11:05", "12:55", "12:59"])
def test_break_confirmation_is_blocked_during_midday_window(entry_time: str) -> None:
    engine = SignalEngine(StrategyConfig())

    allowed, skipped = engine._apply_entry_time_filters(
        [signal(entry_time, "SMART_ZONE_BREAK_CONFIRMATION")],
        [],
    )

    assert allowed == []
    assert skipped[0].skip_reason == "Break confirmation blocked during midday window"
    assert skipped[0].context["blocked_from"] == "11:00"
    assert skipped[0].context["blocked_until"] == "13:00"


@pytest.mark.parametrize("entry_time", ["10:59", "13:00"])
def test_break_confirmation_outside_midday_window_remains_allowed(entry_time: str) -> None:
    engine = SignalEngine(StrategyConfig())
    candidate = signal(entry_time, "SMART_ZONE_BREAK_CONFIRMATION")

    allowed, skipped = engine._apply_entry_time_filters([candidate], [])

    assert allowed == [candidate]
    assert skipped == []


def test_midday_filter_does_not_block_other_setup_types() -> None:
    engine = SignalEngine(StrategyConfig())
    candidate = signal("11:30", "SMART_ZONE_RETEST_CONFIRMATION")

    allowed, skipped = engine._apply_entry_time_filters([candidate], [])

    assert allowed == [candidate]
    assert skipped == []


class StubSmartTrades:
    def __init__(self, candidate: SignalCandidate) -> None:
        self.option_snapshot = None
        self.candidate = candidate

    def generate_for_day(self, *_args):
        return [self.candidate], []

    def generate_for_candle(self, *_args):
        return [self.candidate], []

    def generate_for_candle_rows(self, *_args):
        return [self.candidate], []


def test_every_signal_engine_entry_point_applies_time_filters() -> None:
    engine = SignalEngine(StrategyConfig())
    engine.smart_trades = StubSmartTrades(signal("14:00"))  # type: ignore[assignment]
    levels = LevelSet(
        trading_date=date(2026, 1, 2),
        pdh=24100.0,
        pdl=23900.0,
        pdc=24000.0,
        orh=24050.0,
        orl=23950.0,
    )
    candles = pd.DataFrame()

    results = [
        engine.generate_for_day(candles, levels, date(2026, 1, 2)),
        engine.generate_for_candle(candles, levels, date(2026, 1, 2), pd.Timestamp("2026-01-02 13:55")),
        engine.generate_for_candle_rows(candles, levels, date(2026, 1, 2), pd.Timestamp("2026-01-02 13:55")),
    ]

    assert all(not allowed for allowed, _skipped in results)
    assert all(skipped[0].skip_reason == "Fresh entry blocked from session cutoff" for _allowed, skipped in results)
