from __future__ import annotations

import threading
import time as clock
from collections import deque
from collections.abc import Callable
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from app.data_loader import DataLoader


TickHandler = Callable[[dict[str, Any]], None]
QuoteHandler = Callable[[float], None]
IST = ZoneInfo("Asia/Kolkata")
NSE_SOCKET_START = time(9, 15)
NSE_SOCKET_END = time(15, 30)


def nse_market_hours_status(now: datetime | None = None) -> dict[str, Any]:
    current = now.astimezone(IST) if now else datetime.now(IST)
    is_weekday = current.weekday() < 5
    current_time = current.time()
    is_open = is_weekday and NSE_SOCKET_START <= current_time <= NSE_SOCKET_END
    if not is_weekday:
        reason = "NSE market is closed on weekends"
    elif current_time < NSE_SOCKET_START:
        reason = "NSE market has not opened yet"
    elif current_time > NSE_SOCKET_END:
        reason = "NSE market is closed for the day"
    else:
        reason = "NSE market is open"
    return {
        "is_open": is_open,
        "reason": reason,
        "now_ist": current.strftime("%Y-%m-%d %H:%M:%S"),
        "start_ist": NSE_SOCKET_START.strftime("%H:%M"),
        "end_ist": NSE_SOCKET_END.strftime("%H:%M"),
    }


def require_nse_market_hours() -> None:
    status = nse_market_hours_status()
    if not status["is_open"]:
        raise RuntimeError(
            f"Fyers socket can run only during NSE market hours "
            f"({status['start_ist']} to {status['end_ist']} IST). "
            f"{status['reason']}. Current IST: {status['now_ist']}"
        )


class FyersMarketDataSocket:
    """Fyers live market-data socket wrapper.

    This adapter intentionally exposes market data only. Do not add order
    placement methods to V1.
    """

    def __init__(self, loader: DataLoader | None = None) -> None:
        self.loader = loader or DataLoader()

    def connect(self, symbols: list[str], on_tick: TickHandler) -> None:
        require_nse_market_hours()
        auth = self.loader.load_fyers_auth()
        if not auth:
            raise RuntimeError("Fyers access token is not configured")
        try:
            from fyers_apiv3.FyersWebsocket import data_ws
        except ImportError as exc:
            raise RuntimeError("Install fyers-apiv3 to use the Fyers data socket") from exc

        access_token = f"{auth['client_id']}:{auth['access_token']}"

        def onmessage(message: dict[str, Any]) -> None:
            on_tick(message)

        def onopen() -> None:
            socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
            socket.keep_running()

        socket = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=onopen,
            on_message=onmessage,
            on_error=lambda message: on_tick({"type": "error", "message": message}),
            on_close=lambda message: on_tick({"type": "closed", "message": message}),
        )
        socket.connect()


class FyersSocketSession:
    """Admin socket session for observing ticks without trading execution."""

    def __init__(self, loader: DataLoader | None = None, max_ticks: int = 100, on_price: TickHandler | None = None) -> None:
        self.loader = loader or DataLoader()
        self.max_ticks = max_ticks
        self.on_price = on_price
        self.symbols: list[str] = []
        self.data_type = "SymbolUpdate"
        self.started_at: str | None = None
        self.error: str | None = None
        self._socket = None
        self._thread: threading.Thread | None = None
        self._guard_stop = threading.Event()
        self._guard_thread: threading.Thread | None = None
        self._ticks: deque[dict[str, Any]] = deque(maxlen=max_ticks)
        self._latest_prices: dict[str, dict[str, Any]] = {}
        self._connected = False
        self._lock = threading.Lock()

    def start(self, symbols: list[str], data_type: str = "SymbolUpdate") -> None:
        self.stop()
        require_nse_market_hours()
        auth = self.loader.load_fyers_auth()
        if not auth:
            raise RuntimeError("Fyers access token is not configured")
        try:
            from fyers_apiv3.FyersWebsocket import data_ws
        except ImportError as exc:
            raise RuntimeError("Install fyers-apiv3 to use the Fyers data socket") from exc

        self.symbols = symbols
        self.data_type = data_type
        self.started_at = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self.error = None
        with self._lock:
            self._latest_prices = {}
            self._connected = False
        access_token = f"{auth['client_id']}:{auth['access_token']}"

        def onmessage(message: dict[str, Any]) -> None:
            received_at = datetime.now(IST).isoformat(timespec="seconds")
            with self._lock:
                self._ticks.appendleft({"received_at": received_at, "message": message})
                if isinstance(message, dict) and message.get("s") == "ok" and message.get("type") in {"cn", "ful"}:
                    self._connected = True
                symbol = str(message.get("symbol") or "") if isinstance(message, dict) else ""
                price = _extract_tick_price(message)
                if symbol and price is not None:
                    self._connected = True
                    self._latest_prices[symbol] = {
                        "price": price,
                        "received_at": received_at,
                        "message": message,
                    }
            if symbol and price is not None and self.on_price:
                self.on_price(message)

        def onerror(message: Any) -> None:
            self.error = str(message)
            with self._lock:
                self._connected = False
            onmessage({"type": "error", "message": message})

        def onclose(message: Any) -> None:
            with self._lock:
                self._connected = False
            onmessage({"type": "closed", "message": message})

        def onopen() -> None:
            self._socket.subscribe(symbols=symbols, data_type=data_type)
            self._socket.keep_running()

        self._socket = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=onopen,
            on_message=onmessage,
            on_error=onerror,
            on_close=onclose,
        )
        self._thread = threading.Thread(target=self._socket.connect, daemon=True)
        self._thread.start()
        self._guard_stop.clear()
        self._guard_thread = threading.Thread(target=self._market_hours_guard, daemon=True)
        self._guard_thread.start()

    def stop(self) -> None:
        self._guard_stop.set()
        if self._socket:
            try:
                self._socket.close_connection()
            except Exception as exc:
                self.error = str(exc)
        self._socket = None
        self._thread = None
        self._guard_thread = None
        with self._lock:
            self._connected = False

    def _market_hours_guard(self) -> None:
        while not self._guard_stop.wait(10):
            status = nse_market_hours_status()
            if status["is_open"]:
                continue
            self.error = f"Socket stopped automatically: {status['reason']} at {status['now_ist']} IST"
            try:
                if self._socket:
                    self._socket.close_connection()
            except Exception as exc:
                self.error = str(exc)
            self._socket = None
            self._thread = None
            with self._lock:
                self._connected = False
            self._guard_stop.set()
            return

    def latest_price(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            latest = self._latest_prices.get(symbol)
            return dict(latest) if latest else None

    def status(self) -> dict[str, Any]:
        with self._lock:
            ticks = list(self._ticks)
            latest_prices = {symbol: dict(value) for symbol, value in self._latest_prices.items()}
            connected = self._connected
        return {
            "running": bool(self._socket),
            "connected": connected,
            "market_hours": nse_market_hours_status(),
            "symbols": self.symbols,
            "data_type": self.data_type,
            "started_at": self.started_at,
            "error": self.error,
            "ticks": ticks,
            "latest_prices": latest_prices,
        }


class FyersQuotePoller:
    """Polls Fyers quotes every few seconds for paper-trade monitoring."""

    def __init__(
        self,
        symbol: str,
        interval_seconds: int = 2,
        loader: DataLoader | None = None,
        duration_seconds: int | None = None,
    ) -> None:
        self.symbol = symbol
        self.interval_seconds = interval_seconds
        self.loader = loader or DataLoader()
        self.duration_seconds = duration_seconds
        self.started_at: str | None = None
        self.finished_at: str | None = None
        self.last_price: float | None = None
        self.last_error: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, on_quote: QuoteHandler) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self.started_at = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self.finished_at = None
        self.last_error = None
        self._thread = threading.Thread(target=self._run, args=(on_quote,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def status(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval_seconds": self.interval_seconds,
            "duration_seconds": self.duration_seconds,
            "running": self.is_running(),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "last_price": self.last_price,
            "last_error": self.last_error,
        }

    def _run(self, on_quote: QuoteHandler) -> None:
        deadline = clock.monotonic() + self.duration_seconds if self.duration_seconds else None
        while not self._stop.is_set():
            try:
                price = self.loader.fetch_fyers_quote(self.symbol)
                self.last_price = float(price)
                self.last_error = None
                on_quote(price)
            except Exception as exc:
                self.last_error = str(exc)
            if deadline is not None:
                remaining = deadline - clock.monotonic()
                if remaining <= 0:
                    break
                wait_seconds = min(float(self.interval_seconds), remaining)
            else:
                wait_seconds = float(self.interval_seconds)
            self._stop.wait(wait_seconds)
        self.finished_at = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _extract_tick_price(message: Any) -> float | None:
    if not isinstance(message, dict):
        return None
    for key in ("ltp", "lp", "last_price"):
        value = message.get(key)
        if value is not None:
            return float(value)
    nested = message.get("v")
    if isinstance(nested, dict):
        for key in ("lp", "ltp", "last_price"):
            value = nested.get(key)
            if value is not None:
                return float(value)
    return None
