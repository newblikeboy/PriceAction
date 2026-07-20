from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from app import main


class ChartWindowDatabase:
    def __init__(self) -> None:
        self.load_chart_args: dict | None = None

    def latest_candle_dates(self, timeframe: str, symbol: str, end_date: str, limit: int) -> list[date]:
        assert timeframe == "5m"
        assert symbol == "NIFTY"
        assert limit == 3
        return [date(2026, 7, 16), date(2026, 7, 17), date(2026, 7, 20)]

    def load_chart_candles(self, timeframe: str, symbol: str, start_date: str, end_date: str) -> list[dict]:
        self.load_chart_args = {
            "timeframe": timeframe,
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
        }
        return [
            {"datetime": datetime(2026, 7, 16, 15, 25), "open": 1, "high": 2, "low": 1, "close": 2},
            {"datetime": datetime(2026, 7, 17, 15, 25), "open": 2, "high": 3, "low": 2, "close": 3},
            {"datetime": datetime(2026, 7, 20, 15, 25), "open": 3, "high": 4, "low": 3, "close": 4},
        ]

    def list_trades_between(self, *args, **kwargs) -> list[dict]:
        return []

    def load_candles(self, *args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame()


def test_admin_chart_uses_latest_trading_sessions(monkeypatch) -> None:
    database = ChartWindowDatabase()
    monkeypatch.setattr(main, "get_db", lambda: database)
    main.invalidate_chart_cache()

    payload = main.cached_admin_chart_base("5m", "NIFTY", 3)

    assert database.load_chart_args is not None
    assert database.load_chart_args["start_date"] == "2026-07-16"
    assert database.load_chart_args["end_date"] == "2026-07-20"
    assert payload["date_window"] == "trading_sessions"
    assert payload["trading_dates"] == ["2026-07-16", "2026-07-17", "2026-07-20"]
    assert payload["counts"]["candles"] == 3
