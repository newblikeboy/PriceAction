from __future__ import annotations

import pandas as pd

from app.replay import ReplayBarSession


def test_replay_uses_previous_trading_days_as_hidden_context() -> None:
    candles = _multi_day_candles()

    session = ReplayBarSession(
        symbol="NIFTY",
        start_date="2024-01-08",
        end_date="2024-01-09",
        candles_5m=candles,
        context_trading_days=2,
    )
    payload = session.payload()

    assert session.candles_5m.index[0] == pd.Timestamp("2024-01-04 09:15")
    assert session.visible_candles().index[0] == pd.Timestamp("2024-01-08 09:15")
    assert payload["context_candles"] == 2
    assert payload["visible_candles"] == 1
    assert payload["total_candles"] == 2

    next_payload = session.next()

    assert next_payload["current_time"] == "2024-01-09 09:15:00"
    assert len(next_payload["candles_delta"]) == 1


def test_replay_zone_history_rolls_to_previous_two_trading_days() -> None:
    candles = _multi_day_candles()

    session = ReplayBarSession(
        symbol="NIFTY",
        start_date="2024-01-08",
        end_date="2024-01-09",
        candles_5m=candles,
        context_trading_days=4,
    )
    session.next()

    history = session._zone_history(session.engine_candles(), pd.Timestamp("2024-01-09 09:15"))

    assert [str(day) for day in sorted(history["date"].unique())] == [
        "2024-01-05",
        "2024-01-08",
        "2024-01-09",
    ]


def test_replay_multistep_uses_compact_intermediate_frames() -> None:
    candles = _intraday_candles(4)

    session = ReplayBarSession(
        symbol="NIFTY",
        start_date="2024-01-08",
        end_date="2024-01-08",
        candles_5m=candles,
        context_trading_days=0,
    )
    payload = session.next(count=3)

    assert len(payload["frames"]) == 3
    assert payload["frames"][0]["compact"] is True
    assert payload["frames"][1]["compact"] is True
    assert "compact" not in payload["frames"][2]
    assert "zones" in payload["frames"][2]
    assert payload["current_time"] == "2024-01-08 09:30:00"


def test_replay_zone_anchor_starts_at_0915_then_advances_after_six_candles() -> None:
    session = ReplayBarSession(
        symbol="NIFTY",
        start_date="2024-01-08",
        end_date="2024-01-08",
        candles_5m=_intraday_candles(12),
        context_trading_days=0,
    )

    assert session.candles_5m.index[session._zone_anchor_index()] == pd.Timestamp("2024-01-08 09:15")

    session.current_index = session.replay_start_index + 4
    assert session.candles_5m.index[session._zone_anchor_index()] == pd.Timestamp("2024-01-08 09:15")

    session.current_index = session.replay_start_index + 5
    assert session.candles_5m.index[session._zone_anchor_index()] == pd.Timestamp("2024-01-08 09:40")

    session.current_index = session.replay_start_index + 11
    assert session.candles_5m.index[session._zone_anchor_index()] == pd.Timestamp("2024-01-08 10:10")


def _multi_day_candles() -> pd.DataFrame:
    rows = []
    for day in pd.to_datetime(
        [
            "2024-01-01 09:15",
            "2024-01-02 09:15",
            "2024-01-03 09:15",
            "2024-01-04 09:15",
            "2024-01-05 09:15",
            "2024-01-08 09:15",
            "2024-01-09 09:15",
        ]
    ):
        rows.append(
            {
                "datetime": day,
                "open": 100.0,
                "high": 105.0,
                "low": 95.0,
                "close": 101.0,
                "volume": 1000,
            }
        )
    frame = pd.DataFrame(rows).set_index("datetime")
    frame["date"] = frame.index.date
    frame["time"] = frame.index.strftime("%H:%M")
    return frame


def _intraday_candles(count: int) -> pd.DataFrame:
    rows = []
    timestamp = pd.Timestamp("2024-01-08 09:15")
    for index in range(count):
        rows.append(
            {
                "datetime": timestamp,
                "open": 100.0 + index,
                "high": 105.0 + index,
                "low": 95.0 + index,
                "close": 101.0 + index,
                "volume": 1000,
            }
        )
        timestamp += pd.Timedelta(minutes=5)
    frame = pd.DataFrame(rows).set_index("datetime")
    frame["date"] = frame.index.date
    frame["time"] = frame.index.strftime("%H:%M")
    return frame
