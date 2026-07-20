from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from app.domain import SignalCandidate
from app import main


class LiveTradeDatabase:
    def __init__(self, open_trade: dict | None = None) -> None:
        self.open_trade = open_trade
        self.closed: dict | None = None
        self.inserted_trades: list[dict] = []
        self.inserted_skipped: list[dict] = []

    def list_open_trades(self, symbol: str, limit: int) -> list[dict]:
        return [self.open_trade] if self.open_trade is not None and self.closed is None else []

    def update_trade_option_mark(self, *args, **kwargs) -> None:
        return None

    def update_trade_protection(self, *args, **kwargs) -> None:
        return None

    def close_trade(self, trade_id: int, updates: dict) -> None:
        self.closed = updates

    def load_candles(self, *args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame([{"close": 100.0}])

    def list_trades_between(self, *args, **kwargs) -> list[dict]:
        return [
            {
                "result": "LOSS",
                "features_json": '{"smart_zone": {"zone_id": "failed-zone"}}',
            }
        ]

    def insert_trade_if_absent(self, trade: dict) -> int:
        self.inserted_trades.append(trade)
        return len(self.inserted_trades)

    def insert_skipped_if_absent(self, skipped: dict) -> int:
        self.inserted_skipped.append(skipped)
        return len(self.inserted_skipped)


def test_live_monitor_closes_at_near_target(monkeypatch) -> None:
    database = LiveTradeDatabase(
        {
            "id": 1,
            "direction": "CE",
            "entry_index_price": 100.0,
            "underlying_entry_price": 100.0,
            "sl_index_price": 90.0,
            "target_index_price": 130.0,
            "risk_points": 10.0,
            "reward_points": 30.0,
            "max_favorable_excursion": 0.0,
            "max_adverse_excursion": 0.0,
            "features_json": "{}",
            "option_symbol": None,
        }
    )
    monkeypatch.setattr(main, "get_db", lambda: database)
    monkeypatch.setattr(main, "dispatch_angel_exit_async", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "runtime_cache_clear", lambda *args, **kwargs: None)

    main.update_live_open_trades(128.5, datetime(2024, 1, 1, 10, 0))

    assert database.closed is not None
    assert database.closed["exit_reason"] == "NEAR_TARGET_EXIT"
    assert database.closed["exit_index_price"] == 128.5


def test_live_signal_evaluation_blocks_failed_zone(monkeypatch) -> None:
    database = LiveTradeDatabase()
    break_signal = _signal("SMART_ZONE_BREAK_CONFIRMATION", "failed-zone")
    retest_signal = _signal("SMART_ZONE_RETEST_CONFIRMATION", "failed-zone")

    class FakeLevelEngine:
        def calculate(self, candles, trading_date):
            return object()

    class FakeSignalEngine:
        def generate_for_candle(self, candles, levels, trading_date, candle_time):
            return [break_signal, retest_signal], []

    monkeypatch.setattr(main, "get_db", lambda: database)
    monkeypatch.setattr(main, "LevelEngine", FakeLevelEngine)
    monkeypatch.setattr(main, "SignalEngine", FakeSignalEngine)
    monkeypatch.setattr(main, "attach_live_option_pricing", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "dispatch_angel_entry_async", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "start_live_trade_pnl_monitor", lambda: None)
    monkeypatch.setattr(main, "runtime_cache_clear", lambda *args, **kwargs: None)
    candle_time = datetime(2024, 1, 1, 10, 0)
    main._live_signal_evaluated_5m.discard(main.chart_time(candle_time))

    main.evaluate_closed_live_5m_candle({"datetime": candle_time})

    assert [trade["setup_type"] for trade in database.inserted_trades] == [
        "SMART_ZONE_RETEST_CONFIRMATION"
    ]
    assert len(database.inserted_skipped) == 1
    assert database.inserted_skipped[0]["potential_setup"] == "SMART_ZONE_BREAK_CONFIRMATION"
    assert database.inserted_skipped[0]["skip_reason"] == "Zone blocked after same-day losing trade"


def test_socket_watchdog_restarts_disconnected_socket_after_grace() -> None:
    started_at = datetime.now(main.IST).replace(tzinfo=None) - timedelta(
        seconds=main.FYERS_SOCKET_CONNECT_GRACE_SECONDS + 5
    )

    needs_restart, reason = main.socket_status_needs_restart(
        {
            "running": True,
            "connected": False,
            "symbols": [main.FYERS_NIFTY_INDEX],
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "latest_prices": {},
        }
    )

    assert needs_restart is True
    assert reason == "socket is not connected"


def test_socket_watchdog_restarts_stale_tick_stream() -> None:
    received_at = datetime.now(main.IST) - timedelta(seconds=main.FYERS_SOCKET_STALE_TICK_SECONDS + 5)

    needs_restart, reason = main.socket_status_needs_restart(
        {
            "running": True,
            "connected": True,
            "symbols": [main.FYERS_NIFTY_INDEX],
            "started_at": datetime.now(main.IST).strftime("%Y-%m-%d %H:%M:%S"),
            "latest_prices": {
                main.FYERS_NIFTY_INDEX: {
                    "price": 25000.0,
                    "received_at": received_at.isoformat(timespec="seconds"),
                }
            },
        }
    )

    assert needs_restart is True
    assert reason and reason.startswith("socket tick stale")


def _signal(setup_type: str, zone_id: str) -> SignalCandidate:
    return SignalCandidate(
        date="2024-01-01",
        time="10:05",
        symbol="NIFTY",
        direction="CE",
        setup_type=setup_type,
        entry_index_price=100.0,
        sl_index_price=90.0,
        target_index_price=130.0,
        risk_points=10.0,
        reward_points=30.0,
        risk_reward=3.0,
        setup_score=80,
        features={"smart_zone": {"zone_id": zone_id}},
    )
