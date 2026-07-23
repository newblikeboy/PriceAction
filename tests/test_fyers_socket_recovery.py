import sys
import types
import time

from app.fyers_integration import FyersSocketSession


class FakeLoader:
    def __init__(self):
        self.refresh_calls = 0
        self.auth = {
            "client_id": "ABC123-100",
            "access_token": "old-token",
        }

    def load_fyers_auth(self):
        return self.auth

    def refresh_fyers_access_token_with_totp(self):
        self.refresh_calls += 1
        self.auth["access_token"] = "refreshed-token"
        return {"access_token": "refreshed-token"}


class FakeSocket:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        self.connect_count = 0
        self.subscribe_calls = []
        self.on_error = kwargs.get("on_error")
        self.on_close = kwargs.get("on_close")
        self.on_connect = kwargs.get("on_connect")
        self.on_message = kwargs.get("on_message")

    def connect(self):
        self.connect_count += 1
        if self.connect_count == 1:
            self.on_error("socket Error : connection to remote host was lost")
        else:
            self.on_connect()
            self.on_message({"s": "ok", "type": "cn", "symbol": "NSE:NIFTY50-INDEX", "ltp": 22000.0})

    def subscribe(self, symbols=None, data_type=None):
        self.subscribe_calls.append((symbols, data_type))

    def keep_running(self):
        return None

    def close_connection(self):
        self.closed = True


def test_fyers_socket_session_refreshes_and_restarts_after_socket_drop(monkeypatch):
    import app.fyers_integration as fyers_integration

    monkeypatch.setattr(fyers_integration, "require_nse_market_hours", lambda: None)

    fake_loader = FakeLoader()
    session = FyersSocketSession(loader=fake_loader, max_ticks=10)

    fyers_api = types.ModuleType("fyers_apiv3")
    fyers_ws = types.ModuleType("fyers_apiv3.FyersWebsocket")
    fyers_ws.data_ws = types.SimpleNamespace(FyersDataSocket=FakeSocket)
    fyers_api.FyersWebsocket = fyers_ws

    monkeypatch.setitem(sys.modules, "fyers_apiv3", fyers_api)
    monkeypatch.setitem(sys.modules, "fyers_apiv3.FyersWebsocket", fyers_ws)

    session.start(["NSE:NIFTY50-INDEX"], data_type="SymbolUpdate")
    deadline = time.time() + 1.0
    while time.time() < deadline:
        if fake_loader.refresh_calls >= 1:
            break
        time.sleep(0.05)

    assert fake_loader.refresh_calls == 1
    assert session.status()["running"] is True
