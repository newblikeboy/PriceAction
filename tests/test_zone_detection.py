from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from app.zone_detection import ZoneDetectionSession


def test_zone_detection_anchor_zones_use_previous_two_trading_days() -> None:
    session = ZoneDetectionSession(
        symbol="NIFTY",
        start_date="2024-01-04",
        end_date="2024-01-09",
        candles_5m=_multi_day_candles(),
    )
    captured_dates: list[str] = []

    class FakeLevels:
        def calculate_smart_zones(self, rows, current_price=None):
            captured_dates.extend(str(day) for day in sorted(rows["date"].unique()))
            return SimpleNamespace(zones=[])

    session.levels = FakeLevels()

    assert session._get_anchor_zones() == []
    assert captured_dates == ["2024-01-05", "2024-01-08"]


def _multi_day_candles() -> pd.DataFrame:
    rows = []
    for timestamp in pd.to_datetime(
        [
            "2024-01-04 09:15",
            "2024-01-05 09:15",
            "2024-01-08 09:15",
            "2024-01-09 09:15",
        ]
    ):
        rows.append(
            {
                "datetime": timestamp,
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
