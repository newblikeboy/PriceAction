from __future__ import annotations

import time
from datetime import date
from typing import Any

import pytest

from app.angel_execution import AngelExecutionManager


class FakeDatabase:
    def __init__(self, sessions: list[dict[str, Any]] | None = None) -> None:
        self.sessions = sessions or []
        self.api_hits: list[dict[str, Any]] = []
        self.entries: list[dict[str, Any]] = []
        self.exits: list[dict[str, Any]] = []
        self.open_orders: list[dict[str, Any]] = []

    def execution_lot_size(self, fallback: int) -> int:
        return fallback

    def list_connected_angel_sessions(self) -> list[dict[str, Any]]:
        return self.sessions

    def save_angel_api_hit(self, **values: Any) -> None:
        self.api_hits.append(values)

    def save_angel_live_entry(self, **values: Any) -> None:
        self.entries.append(values)

    def list_angel_live_orders(self, paper_trade_id: int) -> list[dict[str, Any]]:
        return [row for row in self.open_orders if row["paper_trade_id"] == paper_trade_id]

    def list_open_angel_live_orders(self, paper_trade_id: int) -> list[dict[str, Any]]:
        return [row for row in self.open_orders if row["paper_trade_id"] == paper_trade_id]

    def get_user_angel_session(self, username: str) -> dict[str, Any] | None:
        return next((row for row in self.sessions if row["username"] == username), None)

    def save_angel_live_exit(self, **values: Any) -> None:
        self.exits.append(values)


def manager(monkeypatch: pytest.MonkeyPatch, tmp_path: Any, database: FakeDatabase) -> AngelExecutionManager:
    monkeypatch.setenv("ANGEL_LIVE_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("ANGEL_FUTURE_CONTRACT_CACHE", str(tmp_path / "future-contracts.json"))
    return AngelExecutionManager(database)  # type: ignore[arg-type]


def future_session(mode: str = "FUTURE", lot_count: int = 2) -> dict[str, Any]:
    return {
        "username": "trader@example.com",
        "api_key": "key",
        "access_token": "token",
        "execution_instrument": mode,
        "lot_count": lot_count,
    }


def set_future_contracts(execution: AngelExecutionManager) -> None:
    execution._future_contracts = [
        {
            "symbol": "NIFTY27JUL99FUT",
            "token": "12345",
            "exchange": "NFO",
            "expiry": "2099-07-27",
            "lot_size": 65,
        }
    ]
    execution._future_contracts_loaded_at = time.time()


@pytest.mark.parametrize(("direction", "expected_side"), [("CE", "BUY"), ("PE", "SELL")])
def test_future_entry_uses_signal_direction_and_contract_lot_size(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    direction: str,
    expected_side: str,
) -> None:
    database = FakeDatabase([future_session()])
    execution = manager(monkeypatch, tmp_path, database)
    set_future_contracts(execution)
    sent_payloads: list[dict[str, Any]] = []

    def post(_url: str, payload: dict[str, Any], _headers: dict[str, str]) -> tuple[dict[str, Any], int, bool]:
        sent_payloads.append(payload)
        return {"status": True, "data": {"orderid": "ENTRY-1"}}, 200, True

    execution._post = post  # type: ignore[method-assign]

    result = execution.dispatch_entry(41, {"direction": direction})

    assert result["ok"] is True
    assert sent_payloads[0]["tradingsymbol"] == "NIFTY27JUL99FUT"
    assert sent_payloads[0]["transactiontype"] == expected_side
    assert sent_payloads[0]["quantity"] == "130"
    assert database.entries[0]["entry_side"] == expected_side
    assert database.entries[0]["quantity"] == 130


def test_future_exit_reverses_sell_entry_to_buy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    database = FakeDatabase([future_session()])
    database.open_orders = [
        {
            "id": 9,
            "username": "trader@example.com",
            "paper_trade_id": 41,
            "symbol": "NIFTY27JUL99FUT",
            "token": "12345",
            "exchange": "NFO",
            "quantity": 130,
            "entry_side": "SELL",
        }
    ]
    execution = manager(monkeypatch, tmp_path, database)
    sent_payloads: list[dict[str, Any]] = []

    def post(_url: str, payload: dict[str, Any], _headers: dict[str, str]) -> tuple[dict[str, Any], int, bool]:
        sent_payloads.append(payload)
        return {"status": True, "data": {"orderid": "EXIT-1"}}, 200, True

    execution._post = post  # type: ignore[method-assign]

    result = execution.dispatch_exit(41, "SL")

    assert result["ok"] is True
    assert sent_payloads[0]["transactiontype"] == "BUY"
    assert database.exits[0]["exit_order_id"] == "EXIT-1"


def test_duplicate_future_entry_is_not_sent_again(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    database = FakeDatabase([future_session()])
    database.open_orders = [
        {
            "id": 9,
            "username": "trader@example.com",
            "paper_trade_id": 41,
            "status": "OPEN",
            "entry_order_id": "ENTRY-1",
        }
    ]
    execution = manager(monkeypatch, tmp_path, database)
    set_future_contracts(execution)
    execution._post = lambda *_args: pytest.fail("duplicate order reached Angel API")  # type: ignore[method-assign]

    result = execution.dispatch_entry(41, {"direction": "CE"})

    assert result["ok"] is True
    assert result["results"][0]["skipped"] is True
    assert database.entries == []


def test_future_contract_resolution_selects_nearest_unexpired_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    execution = manager(monkeypatch, tmp_path, FakeDatabase())
    rows = [
        {
            "token": "far",
            "symbol": "NIFTY25AUG26FUT",
            "name": "NIFTY",
            "expiry": "25AUG2026",
            "lotsize": "65",
            "instrumenttype": "FUTIDX",
            "exch_seg": "NFO",
        },
        {
            "token": "expired",
            "symbol": "NIFTY30JUN26FUT",
            "name": "NIFTY",
            "expiry": "30JUN2026",
            "lotsize": "65",
            "instrumenttype": "FUTIDX",
            "exch_seg": "NFO",
        },
        {
            "token": "near",
            "symbol": "NIFTY28JUL26FUT",
            "name": "NIFTY",
            "expiry": "28JUL2026",
            "lotsize": "65",
            "instrumenttype": "FUTIDX",
            "exch_seg": "NFO",
        },
    ]
    contracts = [execution._normalise_future_contract(row) for row in rows]

    selected = execution._pick_future_contract(
        [row for row in contracts if row is not None],
        date(2026, 7, 1),
    )

    assert selected is not None
    assert selected["symbol"] == "NIFTY28JUL26FUT"
    assert selected["token"] == "near"
    assert selected["lot_size"] == 65


def test_option_mode_remains_buy_to_open(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    database = FakeDatabase([future_session(mode="OPTION", lot_count=1)])
    execution = manager(monkeypatch, tmp_path, database)
    sent_payloads: list[dict[str, Any]] = []
    execution._post = lambda _url, payload, _headers: (
        sent_payloads.append(payload) or {"status": True, "data": {"orderid": "OPT-1"}},
        200,
        True,
    )  # type: ignore[method-assign]
    trade = {
        "direction": "PE",
        "features": {
            "selected_option_contract": {
                "angel_symbol": "NIFTY26JUL24000PE",
                "angel_token": "987",
                "angel_exchange": "NFO",
                "lot_size": 75,
            }
        },
    }

    result = execution.dispatch_entry(42, trade)

    assert result["ok"] is True
    assert sent_payloads[0]["transactiontype"] == "BUY"
    assert sent_payloads[0]["quantity"] == "75"
