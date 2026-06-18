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
