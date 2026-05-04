from __future__ import annotations

import base64
import hmac
import json
import math
import os
import threading
import time
from copy import deepcopy
from hashlib import sha256
from functools import lru_cache
from datetime import datetime, timedelta
from queue import Queue
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

from app.config import BASE_DIR
from app.data_loader import DataLoader
from app.engines.levels import LevelEngine
from app.engines.signals import SignalEngine
from app.fyers_integration import FyersQuotePoller, FyersSocketSession, nse_market_hours_status
from app.options_pricing import select_option_contract, to_float, to_int
from app.paper_trading import PaperTradeEngine
from app.services import StrategyService
from app.storage.database import Database


app = FastAPI(title="Nifty Price Action Paper Trading AI")
app.add_middleware(GZipMiddleware, minimum_size=1000)
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

_db: Database | None = None
_loader: DataLoader | None = None
_service: StrategyService | None = None
_socket_session: FyersSocketSession | None = None
SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-only-change-me")
_backtest_job_lock = threading.Lock()
_backtest_job: dict[str, Any] = {}
IST = ZoneInfo("Asia/Kolkata")
FYERS_NIFTY_INDEX = "NSE:NIFTY50-INDEX"
_chart_cache_lock = threading.Lock()
_chart_cache: dict[tuple[str, str, int], dict[str, Any]] = {}
CHART_CACHE_TTL_SECONDS = 120
_chart_warm_lock = threading.Lock()
_chart_warm_running = False
_runtime_cache_lock = threading.Lock()
_runtime_cache: dict[str, tuple[float, Any]] = {}
_live_socket_lock = threading.Lock()
_live_candle_lock = threading.Lock()
_live_candles: dict[tuple[str, int], dict[str, Any]] = {}
_live_candle_last_error: str | None = None
_live_candle_persist_queue: Queue[dict[str, Any] | None] = Queue()
_live_candle_persist_started = False
_live_candle_persist_lock = threading.Lock()
_live_signal_lock = threading.Lock()
_live_signal_evaluated_5m: set[int] = set()
_live_completed_candles_enqueued: set[tuple[str, int]] = set()
_live_signal_last_error: str | None = None
_live_trade_monitor_lock = threading.Lock()
_live_trade_monitor_poller: FyersQuotePoller | None = None
_live_trade_monitor_status: dict[str, Any] = {}
LIVE_DB_SYMBOL = "NIFTY"
LIVE_TRADE_MONITOR_SECONDS = 30
LIVE_TRADE_MONITOR_INTERVAL_SECONDS = 2


@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/static/"):
        response.headers.setdefault("Cache-Control", "public, max-age=86400")
    return response


class SignupPayload(BaseModel):
    full_name: str = ""
    email: str
    mobile_number: str = ""
    password: str
    confirm_password: str


class LoginPayload(BaseModel):
    email: str | None = None
    username: str | None = None
    password: str
    role: str | None = "user"


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db


def get_loader() -> DataLoader:
    global _loader
    if _loader is None:
        _loader = DataLoader()
    return _loader


def get_service() -> StrategyService:
    global _service
    if _service is None:
        _service = StrategyService(get_db())
    return _service


def get_socket_session() -> FyersSocketSession:
    global _socket_session
    if _socket_session is None:
        _socket_session = FyersSocketSession(get_loader(), on_price=process_fyers_tick)
    return _socket_session


def runtime_cache_get(key: str, ttl_seconds: float, factory, *, copy_value: bool = True):
    now = time.monotonic()
    with _runtime_cache_lock:
        cached = _runtime_cache.get(key)
        if cached and cached[0] > now:
            return deepcopy(cached[1]) if copy_value else cached[1]
    value = factory()
    with _runtime_cache_lock:
        _runtime_cache[key] = (now + ttl_seconds, deepcopy(value) if copy_value else value)
    return deepcopy(value) if copy_value else value


def runtime_cache_clear(prefix: str | None = None) -> None:
    with _runtime_cache_lock:
        if prefix is None:
            _runtime_cache.clear()
            return
        for key in [key for key in _runtime_cache if key.startswith(prefix)]:
            _runtime_cache.pop(key, None)


def cached_user(username: str) -> dict[str, Any] | None:
    user = runtime_cache_get(f"user:{username}", 20, lambda: get_db().get_user(username), copy_value=True)
    return dict(user) if user else None


def cached_trades(limit: int) -> list[dict[str, Any]]:
    trades = runtime_cache_get(f"trades:{limit}", 5, lambda: get_db().list_trades(limit), copy_value=True)
    return [enrich_trade_points(dict(trade)) for trade in trades]


def cached_skipped(limit: int) -> list[dict[str, Any]]:
    return runtime_cache_get(f"skipped:{limit}", 5, lambda: get_db().list_skipped(limit), copy_value=True)


def cached_candle_counts(symbol: str = "NIFTY") -> dict[str, int]:
    return runtime_cache_get(f"candle-counts:{symbol}", 30, lambda: get_db().candle_counts(symbol), copy_value=True)


def cached_fyers_quote(symbol: str) -> float:
    return float(runtime_cache_get(f"fyers-quote:{symbol}", 3, lambda: get_loader().fetch_fyers_quote(symbol), copy_value=False))


def cached_fyers_quote_details(symbol: str) -> dict[str, Any] | None:
    return runtime_cache_get(f"fyers-quote-details:{symbol}", 2, lambda: get_loader().fetch_fyers_quote_details(symbol), copy_value=True)


def cached_option_snapshot() -> dict[str, Any] | None:
    return runtime_cache_get("fyers-option-snapshot:NIFTY", 10, lambda: get_loader().fetch_fyers_option_snapshot(FYERS_NIFTY_INDEX, 11), copy_value=True)


def live_socket_quote(symbol: str) -> dict[str, Any]:
    market_hours = nse_market_hours_status()
    if not market_hours["is_open"]:
        return {
            "enabled": False,
            "running": False,
            "connected": False,
            "price": None,
            "message": market_hours["reason"],
        }
    try:
        session = get_socket_session()
        with _live_socket_lock:
            status = session.status()
            if not status.get("running") or symbol not in (status.get("symbols") or []):
                session.start([symbol], data_type="SymbolUpdate")
                status = session.status()
        latest = session.latest_price(symbol)
        status = session.status()
        return {
            "enabled": True,
            "running": bool(status.get("running")),
            "connected": bool(status.get("connected")),
            "price": round(float(latest["price"]), 2) if latest and latest.get("price") is not None else None,
            "received_at": latest.get("received_at") if latest else None,
            "message": "FYERS socket tick received" if latest else "FYERS socket connected; waiting for first tick",
        }
    except Exception as exc:
        return {
            "enabled": False,
            "running": False,
            "connected": False,
            "price": None,
            "message": str(exc),
        }


def sign_payload(payload: dict[str, Any]) -> str:
    raw = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    sig = hmac.new(SESSION_SECRET.encode(), raw.encode(), sha256).hexdigest()
    return f"{raw}.{sig}"


def read_session(request: Request) -> dict[str, Any] | None:
    value = request.cookies.get("session")
    if not value or "." not in value:
        return None
    raw, sig = value.rsplit(".", 1)
    expected = hmac.new(SESSION_SECRET.encode(), raw.encode(), sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        return json.loads(base64.urlsafe_b64decode(raw.encode()).decode())
    except (json.JSONDecodeError, ValueError):
        return None


def current_user(request: Request) -> dict[str, Any]:
    session = read_session(request)
    if not session:
        raise HTTPException(status_code=401, detail="Login required")
    user = cached_user(session["username"])
    if not user:
        raise HTTPException(status_code=401, detail="Login required")
    return dict(user)


def require_admin(user: dict[str, Any] = Depends(current_user)) -> dict[str, Any]:
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return user


def require_user(user: dict[str, Any] = Depends(current_user)) -> dict[str, Any]:
    if user["role"] not in {"user", "admin"}:
        raise HTTPException(status_code=403, detail="User role required")
    return user


def trade_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [trade for trade in trades if str(trade.get("status") or "").upper() == "CLOSED"]
    wins = [trade for trade in closed if str(trade.get("result") or "").upper() == "WIN"]
    losses = [trade for trade in closed if str(trade.get("result") or "").upper() == "LOSS"]
    total_points = sum(float(trade.get("points") or 0) for trade in closed)
    win_rate = round((len(wins) / len(closed) * 100), 2) if closed else 0
    return {
        "total_trades": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "total_points": round(total_points, 2),
        "win_rate": win_rate,
    }


def trade_points(trade: dict[str, Any]) -> float | None:
    option_points = trade.get("option_points")
    if option_points is not None:
        try:
            return round(float(option_points), 2)
        except (TypeError, ValueError):
            pass
    features = json_payload(trade.get("features_json"))
    feature_points = features.get("points") if isinstance(features, dict) else None
    if feature_points is not None:
        try:
            return round(float(feature_points), 2)
        except (TypeError, ValueError):
            pass
    if str(trade.get("status") or "").upper() == "OPEN":
        try:
            option_entry = float(trade.get("option_entry_ltp") or 0)
            option_mark = float(trade.get("option_mark_ltp") or 0)
        except (TypeError, ValueError):
            option_entry = 0.0
            option_mark = 0.0
        if option_entry > 0.0 and option_mark > 0.0:
            return round(option_mark - option_entry, 2)
        underlying_points = trade.get("underlying_points")
        if underlying_points is not None:
            try:
                return round(float(underlying_points), 2)
            except (TypeError, ValueError):
                pass
        try:
            underlying_entry = float(trade.get("underlying_entry_price") or trade.get("entry_index_price") or 0)
            underlying_mark = float(trade.get("underlying_exit_price") or 0)
        except (TypeError, ValueError):
            underlying_entry = 0.0
            underlying_mark = 0.0
        if underlying_entry > 0.0 and underlying_mark > 0.0:
            direction = str(trade.get("direction") or "").upper()
            return round(underlying_mark - underlying_entry, 2) if direction == "CE" else round(underlying_entry - underlying_mark, 2)
    if trade.get("exit_index_price") is None:
        return None
    try:
        entry = float(trade.get("entry_index_price") or 0)
        exit_price = float(trade.get("exit_index_price") or 0)
    except (TypeError, ValueError):
        return None
    direction = str(trade.get("direction") or "").upper()
    if direction == "CE":
        return round(exit_price - entry, 2)
    if direction == "PE":
        return round(entry - exit_price, 2)
    return round(exit_price - entry, 2)


def enrich_trade_points(trade: dict[str, Any]) -> dict[str, Any]:
    points = trade_points(trade)
    trade["points"] = points
    return trade


def json_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            data = json.loads(value)
            return dict(data) if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def active_trade_text(trades: list[dict[str, Any]]) -> str:
    for trade in trades:
        if str(trade.get("status") or "").upper() == "OPEN":
            option_symbol = str(trade.get("option_symbol") or "").strip()
            option_entry = trade.get("option_entry_ltp")
            option_mark = trade.get("option_mark_ltp")
            points = trade.get("points")
            mark_text = f", mark {option_mark}" if option_mark else ""
            pnl_text = f", P&L {points} pts" if points is not None else ""
            if option_symbol and option_entry:
                return (
                    f"{trade.get('direction')} {trade.get('setup_type')} "
                    f"from {trade.get('entry_time')} @ {option_entry} ({option_symbol}{mark_text}{pnl_text})"
                )
            return (
                f"{trade.get('direction')} {trade.get('setup_type')} "
                f"from {trade.get('entry_time')} @ {trade.get('entry_index_price')}"
            )
    return "No active paper trade."


def public_user(user: dict[str, Any]) -> dict[str, Any]:
    username = str(user.get("username") or "").strip()
    return {
        "username": username,
        "email": username,
        "full_name": username.split("@", 1)[0] if "@" in username else username,
        "role": user.get("role"),
    }


def wants_json(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    requested_with = request.headers.get("x-requested-with", "")
    return "application/json" in accept or requested_with == "fetch"


def public_backtest_run(run: dict[str, Any] | None) -> dict[str, Any] | None:
    if not run:
        return None
    summary = run.get("summary") or {}
    return {
        "id": run.get("id"),
        "symbol": run.get("symbol"),
        "start_date": str(run.get("start_date")) if run.get("start_date") else None,
        "end_date": str(run.get("end_date")) if run.get("end_date") else None,
        "status": str(run.get("status") or "").lower(),
        "progress_pct": float(run.get("progress_pct") or 0),
        "current_step": run.get("current_step") or "",
        "trades_count": int(run.get("trades_count") or 0),
        "skipped_count": int(run.get("skipped_count") or 0),
        "summary": summary,
        "error_message": run.get("error_message") or "",
        "started_at": str(run.get("started_at")) if run.get("started_at") else None,
        "completed_at": str(run.get("completed_at")) if run.get("completed_at") else None,
    }


def current_backtest_payload() -> dict[str, Any]:
    with _backtest_job_lock:
        active_job = dict(_backtest_job) if _backtest_job.get("status") == "running" else None
    if active_job:
        return {"active": True, "latest": active_job}
    return {"active": False, "latest": public_backtest_run(get_db().latest_backtest_run())}


def decode_jwt_payload(token: str | None) -> dict[str, Any]:
    if not token or token.count(".") < 2:
        return {}
    try:
        payload = token.split(".", 2)[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
    except Exception:
        return {}


def token_expiry_status(token: str | None) -> dict[str, Any]:
    payload = decode_jwt_payload(token)
    exp = payload.get("exp")
    if not exp:
        return {
            "status": "warn",
            "label": "Unknown",
            "message": "Token expiry could not be read. Re-auth if FYERS market data stops.",
            "expires_at": None,
        }
    expires_at = datetime.fromtimestamp(int(exp), tz=IST)
    now = datetime.now(IST)
    minutes_left = int((expires_at - now).total_seconds() // 60)
    if minutes_left <= 0:
        return {
            "status": "bad",
            "label": "Expired",
            "message": f"Expired at {expires_at.strftime('%Y-%m-%d %H:%M IST')}. Open FYERS Auth and exchange a new code.",
            "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S IST"),
        }
    if minutes_left < 60:
        label = f"{minutes_left}m left"
        status = "warn"
    else:
        label = f"{round(minutes_left / 60, 1)}h left"
        status = "ok"
    return {
        "status": status,
        "label": label,
        "message": f"Expires at {expires_at.strftime('%Y-%m-%d %H:%M IST')}.",
        "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S IST"),
    }


def next_nse_close(now: datetime | None = None) -> datetime:
    current = now or datetime.now(IST)
    candidate = current.replace(hour=15, minute=30, second=0, microsecond=0)
    if current.weekday() < 5 and current <= candidate:
        return candidate
    candidate = (current + timedelta(days=1)).replace(hour=15, minute=30, second=0, microsecond=0)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def token_covers_next_market_close(token: str | None) -> dict[str, Any]:
    payload = decode_jwt_payload(token)
    exp = payload.get("exp")
    if not exp:
        return {"ok": False, "message": "Token expiry could not be read."}
    expires_at = datetime.fromtimestamp(int(exp), tz=IST)
    required_until = next_nse_close()
    if expires_at < required_until:
        return {
            "ok": False,
            "message": (
                f"Token expires at {expires_at.strftime('%Y-%m-%d %H:%M IST')}, "
                f"before next NSE close {required_until.strftime('%Y-%m-%d %H:%M IST')}."
            ),
        }
    return {
        "ok": True,
        "message": f"Token covers next NSE close {required_until.strftime('%Y-%m-%d %H:%M IST')}.",
    }


def preflight_item(name: str, status: str, message: str, action: str = "") -> dict[str, str]:
    return {"name": name, "status": status, "message": message, "action": action}


def admin_preflight() -> dict[str, Any]:
    items: list[dict[str, str]] = []
    overall = "ok"

    def add(item: dict[str, str]) -> None:
        nonlocal overall
        items.append(item)
        if item["status"] == "bad":
            overall = "bad"
        elif item["status"] == "warn" and overall != "bad":
            overall = "warn"

    try:
        db = get_db()
        with db.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 AS ok")
                cursor.fetchone()
        add(preflight_item("Database", "ok", "MySQL is connected.", ""))
        try:
            counts = cached_candle_counts("NIFTY")
            missing = [tf for tf, count in counts.items() if int(count or 0) <= 0]
            if missing:
                add(preflight_item("Candle Data", "bad", f"Missing candles for: {', '.join(missing)}.", "Run FYERS backfill."))
            else:
                add(preflight_item("Candle Data", "ok", f"Loaded 1m={counts['1m']}, 5m={counts['5m']}, 15m={counts['15m']}.", ""))
        except Exception as exc:
            add(preflight_item("Candle Data", "warn", f"Could not verify candle counts: {exc}", "Check MySQL tables."))
    except Exception as exc:
        add(preflight_item("Database", "bad", f"MySQL connection failed: {exc}", "Fix MYSQL_URI / Aiven connectivity."))

    loader = get_loader()
    app_credentials = loader.fyers_app_credentials()
    missing_credentials = [
        key.upper()
        for key in ("client_id", "secret_key", "redirect_uri")
        if not app_credentials.get(key)
    ]
    if missing_credentials:
        add(preflight_item("FYERS App Config", "bad", f"Missing {', '.join(missing_credentials)}.", "Set FYERS values in .env."))
    else:
        add(preflight_item("FYERS App Config", "ok", "Client ID, secret key, and redirect URI are configured.", ""))

    auth = loader.load_fyers_auth() or {}
    access = auth.get("access_token")
    refresh = auth.get("refresh_token")
    if not access:
        add(preflight_item("FYERS Access Token", "bad", "Access token is missing.", "Open FYERS Auth and exchange auth code."))
    else:
        status = token_expiry_status(access)
        coverage = token_covers_next_market_close(access)
        if status["status"] == "bad":
            add(preflight_item("FYERS Access Token", "bad", status["message"], "Open FYERS Auth and exchange a new code."))
        elif not coverage["ok"]:
            add(preflight_item("FYERS Access Token", "bad", coverage["message"], "Renew token before using live charts/backfill."))
        else:
            add(preflight_item("FYERS Access Token", status["status"], f"{status['message']} {coverage['message']}", "Renew token." if status["status"] != "ok" else ""))
    if not refresh:
        add(preflight_item("FYERS Refresh Token", "warn", "Refresh token is missing.", "Exchange a fresh FYERS auth code."))
    else:
        status = token_expiry_status(refresh)
        add(preflight_item("FYERS Refresh Token", status["status"], status["message"], "Renew token." if status["status"] != "ok" else ""))

    if overall == "ok":
        headline = "Ready"
    elif overall == "warn":
        headline = "Needs Attention"
    else:
        headline = "Action Required"
    return {"status": overall, "headline": headline, "items": items, "checked_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")}


def update_active_backtest_job(run_id: int, updates: dict[str, Any]) -> None:
    with _backtest_job_lock:
        if _backtest_job.get("id") != run_id:
            return
        _backtest_job.update(updates)


def run_backtest_job(run_id: int, symbol: str, start_date: str | None, end_date: str | None) -> None:
    db = get_db()

    def progress(update: dict[str, Any]) -> None:
        progress_pct = float(update.get("percent", 0))
        current_step = str(update.get("current_step") or "Running")
        trades_count = update.get("trades_count")
        skipped_count = update.get("skipped_count")
        job_updates: dict[str, Any] = {
            "progress_pct": progress_pct,
            "current_step": current_step,
        }
        db_updates: dict[str, Any] = {
            "progress_pct": progress_pct,
            "current_step": current_step,
        }
        if trades_count is not None:
            job_updates["trades_count"] = int(trades_count)
            db_updates["trades_count"] = int(trades_count)
        if skipped_count is not None:
            job_updates["skipped_count"] = int(skipped_count)
            db_updates["skipped_count"] = int(skipped_count)
        update_active_backtest_job(run_id, job_updates)
        db.update_backtest_run(run_id, **db_updates)

    try:
        result = get_service().run_database_backtest(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            progress_callback=progress,
        )
        runtime_cache_clear("trades:")
        runtime_cache_clear("skipped:")
        db.update_backtest_run(
            run_id,
            status="COMPLETED",
            progress_pct=100,
            current_step="Completed",
            summary=result["summary"],
            trades_count=len(result["trades"]),
            skipped_count=len(result["skipped_signals"]),
            completed=True,
        )
        update_active_backtest_job(
            run_id,
            {
                "status": "completed",
                "progress_pct": 100,
                "current_step": "Completed",
                "summary": result["summary"],
                "trades_count": len(result["trades"]),
                "skipped_count": len(result["skipped_signals"]),
            },
        )
    except Exception as exc:
        message = str(exc)
        db.update_backtest_run(
            run_id,
            status="FAILED",
            current_step="Failed",
            error_message=message,
            completed=True,
        )
        update_active_backtest_job(run_id, {"status": "failed", "current_step": "Failed", "error_message": message})


def chart_time(value: Any) -> int:
    dt = pd_timestamp(value)
    # Lightweight Charts renders timestamp labels in UTC. The candle database
    # stores IST clock times as naive datetimes, so encode the same clock fields
    # as UTC to keep the chart axis at NSE market hours.
    utc_clock = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=ZoneInfo("UTC"))
    return int(utc_clock.timestamp())


def pd_timestamp(value: Any) -> datetime:
    try:
        import pandas as pd

        return pd.to_datetime(value).to_pydatetime()
    except Exception:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))


def chart_datetime(date_value: Any, hhmm: Any) -> datetime:
    return datetime.fromisoformat(f"{date_value} {hhmm}")


def timeframe_floor(dt: datetime, timeframe: str) -> datetime:
    minutes = {"1m": 1, "5m": 5, "15m": 15}[timeframe]
    floored = dt.replace(second=0, microsecond=0)
    return floored.replace(minute=(floored.minute // minutes) * minutes)


def tick_datetime(message: dict[str, Any] | None = None) -> datetime:
    if isinstance(message, dict):
        value = message.get("exch_feed_time") or message.get("last_traded_time")
        if value:
            try:
                return datetime.fromtimestamp(int(value), tz=IST).replace(tzinfo=None)
            except (TypeError, ValueError, OSError):
                pass
    return datetime.now(IST).replace(tzinfo=None)


def tick_price(message: dict[str, Any]) -> float | None:
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


def process_fyers_tick(message: dict[str, Any]) -> None:
    symbol = str(message.get("symbol") or "")
    if symbol != FYERS_NIFTY_INDEX:
        return
    price = tick_price(message)
    if price is None:
        return
    tick_dt = tick_datetime(message)
    record_live_market_price(float(price), tick_dt, source="FYERS socket")
    update_live_open_trades(float(price), tick_dt)


def live_trade_monitor_status() -> dict[str, Any]:
    with _live_trade_monitor_lock:
        status = dict(_live_trade_monitor_status)
        poller = _live_trade_monitor_poller
    poller_status = poller.status() if poller else {
        "symbol": FYERS_NIFTY_INDEX,
        "interval_seconds": LIVE_TRADE_MONITOR_INTERVAL_SECONDS,
        "duration_seconds": LIVE_TRADE_MONITOR_SECONDS,
        "running": False,
    }
    return {**poller_status, **status}


def start_live_trade_pnl_monitor(
    *,
    duration_seconds: int = LIVE_TRADE_MONITOR_SECONDS,
    interval_seconds: int = LIVE_TRADE_MONITOR_INTERVAL_SECONDS,
) -> dict[str, Any]:
    global _live_trade_monitor_poller, _live_trade_monitor_status
    duration_seconds = max(2, min(int(duration_seconds), 300))
    interval_seconds = max(1, min(int(interval_seconds), 30))
    try:
        open_trades = get_db().list_open_trades(LIVE_DB_SYMBOL, limit=20)
    except Exception as exc:
        with _live_trade_monitor_lock:
            _live_trade_monitor_status = {"last_error": str(exc), "open_trades": 0}
        return live_trade_monitor_status()
    if not open_trades:
        with _live_trade_monitor_lock:
            _live_trade_monitor_status = {
                "open_trades": 0,
                "message": "No open paper trade to monitor",
            }
        return live_trade_monitor_status()

    already_running = False
    with _live_trade_monitor_lock:
        if _live_trade_monitor_poller and _live_trade_monitor_poller.is_running():
            _live_trade_monitor_status["open_trades"] = len(open_trades)
            _live_trade_monitor_status["message"] = "Live trade monitor already running"
            already_running = True
        else:
            poller = FyersQuotePoller(
                FYERS_NIFTY_INDEX,
                interval_seconds=interval_seconds,
                loader=get_loader(),
                duration_seconds=duration_seconds,
            )
            _live_trade_monitor_poller = poller
            _live_trade_monitor_status = {
                "open_trades": len(open_trades),
                "message": "Live trade monitor started",
            }
    if already_running:
        return live_trade_monitor_status()

    def on_quote(price: float) -> None:
        tick_dt = datetime.now(IST).replace(tzinfo=None)
        record_live_market_price(float(price), tick_dt, source="FYERS REST quote monitor")
        update_live_open_trades(float(price), tick_dt, force_quote_refresh=True, allow_time_exit=False)
        with _live_trade_monitor_lock:
            _live_trade_monitor_status.update(
                {
                    "last_price": round(float(price), 2),
                    "last_quote_at": tick_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "message": "Live trade monitor quote applied",
                }
            )

    poller.start(on_quote)
    return live_trade_monitor_status()


def invalidate_chart_cache(timeframe: str | None = None, symbol: str | None = None) -> None:
    with _chart_cache_lock:
        for key in list(_chart_cache):
            key_timeframe, key_symbol, _days = key
            if timeframe and key_timeframe != timeframe:
                continue
            if symbol and key_symbol != symbol:
                continue
            _chart_cache.pop(key, None)


def ensure_live_candle_persist_worker() -> None:
    global _live_candle_persist_started
    with _live_candle_persist_lock:
        if _live_candle_persist_started:
            return
        thread = threading.Thread(target=live_candle_persist_worker, daemon=True)
        thread.start()
        _live_candle_persist_started = True


def live_candle_persist_worker() -> None:
    global _live_candle_last_error
    while True:
        candle = _live_candle_persist_queue.get()
        if candle is None:
            return
        try:
            was_completed = bool(candle.get("completed"))
            get_db().upsert_candle(
                str(candle["timeframe"]),
                LIVE_DB_SYMBOL,
                candle["datetime"],
                float(candle["open"]),
                float(candle["high"]),
                float(candle["low"]),
                float(candle["close"]),
                int(candle.get("volume") or 0),
            )
            _live_candle_last_error = None
            if was_completed and str(candle["timeframe"]) == "5m":
                evaluate_closed_live_5m_candle(candle)
        except Exception as exc:
            _live_candle_last_error = str(exc)
        finally:
            _live_candle_persist_queue.task_done()


def enqueue_live_candle_persist(candle: dict[str, Any]) -> None:
    ensure_live_candle_persist_worker()
    _live_candle_persist_queue.put(dict(candle))


def matches_closed_5m_candle_time(event_time: str, candle_time: datetime) -> bool:
    start_hhmm = candle_time.strftime("%H:%M")
    close_hhmm = (candle_time + timedelta(minutes=5)).strftime("%H:%M")
    return event_time in {start_hhmm, close_hhmm}


def evaluate_closed_live_5m_candle(candle: dict[str, Any]) -> None:
    global _live_signal_last_error
    candle_time = candle.get("datetime")
    if not isinstance(candle_time, datetime):
        return
    candle_key = chart_time(candle_time)
    with _live_signal_lock:
        if candle_key in _live_signal_evaluated_5m:
            return
        _live_signal_evaluated_5m.add(candle_key)
    try:
        trading_date = candle_time.date()
        start_date = (trading_date - timedelta(days=10)).isoformat()
        end_date = trading_date.isoformat()
        db = get_db()
        candles_1m = db.load_candles("1m", symbol=LIVE_DB_SYMBOL, start_date=end_date, end_date=end_date)
        candles_5m = db.load_candles("5m", symbol=LIVE_DB_SYMBOL, start_date=start_date, end_date=end_date)
        if candles_1m.empty or candles_5m.empty:
            return
        level_set = LevelEngine().calculate(candles_5m, trading_date)
        signals, skipped = SignalEngine().generate_for_day(candles_5m, candles_1m, level_set, trading_date)
        paper = PaperTradeEngine()
        saved_trades = 0
        saved_skipped = 0
        for signal in signals:
            if not matches_closed_5m_candle_time(signal.time, candle_time):
                continue
            trade = paper.create_trade(signal).to_dict()
            attach_live_option_pricing(trade, signal)
            if db.insert_trade_if_absent(trade) is not None:
                saved_trades += 1
        for skipped_signal in skipped:
            if not matches_closed_5m_candle_time(skipped_signal.time, candle_time):
                continue
            if db.insert_skipped_if_absent(skipped_signal.to_dict()) is not None:
                saved_skipped += 1
        if saved_trades:
            runtime_cache_clear("trades:")
            start_live_trade_pnl_monitor()
        if saved_skipped:
            runtime_cache_clear("skipped:")
        _live_signal_last_error = None
    except Exception as exc:
        _live_signal_last_error = str(exc)


def attach_live_option_pricing(trade: dict[str, Any], signal: Any) -> None:
    trade["underlying_entry_price"] = trade.get("entry_index_price")
    trade["pnl_source"] = "option_unavailable"
    features = trade.get("features") if isinstance(trade.get("features"), dict) else {}
    try:
        snapshot = cached_option_snapshot()
        selected = select_option_contract(
            direction=str(getattr(signal, "direction", trade.get("direction", ""))),
            spot_price=float(getattr(signal, "entry_index_price", trade.get("entry_index_price") or 0)),
            setup_score=int(getattr(signal, "setup_score", trade.get("setup_score") or 0)),
            features=getattr(signal, "features", features) if isinstance(getattr(signal, "features", features), dict) else features,
            option_snapshot=snapshot,
        )
        features["selected_option"] = selected
        symbol = str(selected.get("symbol") or "").strip()
        trade["option_symbol"] = symbol or None
        trade["option_side"] = selected.get("side") or trade.get("direction")
        trade["option_strike"] = selected.get("strike")
        quote = cached_fyers_quote_details(symbol) if symbol else None
        entry_ltp = to_float((quote or {}).get("ltp"), to_float(selected.get("ltp_ref"), 0.0))
        if entry_ltp > 0.0:
            trade["option_entry_ltp"] = round(entry_ltp, 2)
            trade["option_mark_ltp"] = round(entry_ltp, 2)
            trade["pnl_source"] = "option_quote"
            features["option_entry_ltp"] = round(entry_ltp, 2)
            features["option_quote_ts"] = to_int((quote or {}).get("timestamp"), to_int(selected.get("snapshot_ts"), 0))
        else:
            features["option_pricing_status"] = "entry_ltp_unavailable"
    except Exception as exc:
        features["option_pricing_status"] = f"error:{exc}"
    trade["features"] = features


def update_live_open_trades(
    underlying_price: float,
    tick_time: datetime,
    *,
    force_quote_refresh: bool = False,
    allow_time_exit: bool = True,
) -> None:
    try:
        db = get_db()
        open_trades = db.list_open_trades(LIVE_DB_SYMBOL, limit=20)
    except Exception:
        return
    if not open_trades:
        return

    changed = False
    for trade in open_trades:
        trade_id = int(trade.get("id") or 0)
        if trade_id <= 0:
            continue
        option_symbol = str(trade.get("option_symbol") or "").strip()
        try:
            quote = get_loader().fetch_fyers_quote_details(option_symbol) if force_quote_refresh and option_symbol else cached_fyers_quote_details(option_symbol) if option_symbol else None
        except Exception:
            quote = None
        mark_ltp = to_float((quote or {}).get("ltp"), to_float(trade.get("option_mark_ltp"), 0.0))
        direction = str(trade.get("direction") or "").upper()
        underlying_entry = to_float(trade.get("underlying_entry_price"), to_float(trade.get("entry_index_price"), 0.0))
        if direction == "CE":
            live_underlying_points = underlying_price - underlying_entry
        else:
            live_underlying_points = underlying_entry - underlying_price
        if mark_ltp > 0.0 or underlying_entry > 0.0:
            try:
                db.update_trade_option_mark(
                    trade_id,
                    option_symbol=str((quote or {}).get("resolved_symbol") or (quote or {}).get("symbol") or option_symbol),
                    option_mark_ltp=mark_ltp if mark_ltp > 0.0 and option_symbol else None,
                    underlying_mark_price=underlying_price,
                    underlying_points=live_underlying_points,
                )
                changed = True
            except Exception:
                pass

        close_price: float | None = None
        reason = ""
        sl = to_float(trade.get("sl_index_price"), 0.0)
        target = to_float(trade.get("target_index_price"), 0.0)
        if direction == "CE":
            if underlying_price <= sl:
                close_price, reason = sl, "SL_HIT"
            elif underlying_price >= target:
                close_price, reason = target, "TARGET_HIT"
        elif direction == "PE":
            if underlying_price >= sl:
                close_price, reason = sl, "SL_HIT"
            elif underlying_price <= target:
                close_price, reason = target, "TARGET_HIT"
        if allow_time_exit and close_price is None and tick_time.strftime("%H:%M") >= PaperTradeEngine().cfg.square_off_time:
            close_price, reason = underlying_price, "TIME_EXIT"
        if close_price is None:
            continue

        option_entry = to_float(trade.get("option_entry_ltp"), 0.0)
        option_exit = mark_ltp if mark_ltp > 0.0 else to_float(trade.get("option_mark_ltp"), 0.0)
        if direction == "CE":
            underlying_points = close_price - underlying_entry
        else:
            underlying_points = underlying_entry - close_price
        if option_entry > 0.0 and option_exit > 0.0:
            points = option_exit - option_entry
            pnl_source = "option_quote"
        else:
            points = underlying_points
            option_exit = None
            pnl_source = "underlying_fallback"
        r_multiple = round(points / to_float(trade.get("risk_points"), 1.0), 3) if to_float(trade.get("risk_points"), 0.0) else 0
        result = "WIN" if points > 0 else "LOSS" if points < 0 else "FLAT"
        features = json_payload(trade.get("features_json"))
        features.update(
            {
                "result": result,
                "points": round(points, 2),
                "option_points": round(points, 2) if pnl_source == "option_quote" else None,
                "underlying_points": round(underlying_points, 2),
                "pnl_source": pnl_source,
                "reason_for_exit": reason,
            }
        )
        try:
            db.close_trade(
                trade_id,
                {
                    "exit_time": tick_time.strftime("%H:%M"),
                    "exit_index_price": round(float(close_price), 2),
                    "exit_reason": reason,
                    "result": result,
                    "r_multiple": r_multiple,
                    "max_favorable_excursion": trade.get("max_favorable_excursion"),
                    "max_adverse_excursion": trade.get("max_adverse_excursion"),
                    "option_symbol": str((quote or {}).get("resolved_symbol") or (quote or {}).get("symbol") or option_symbol),
                    "option_mark_ltp": round(option_exit, 2) if option_exit else None,
                    "option_exit_ltp": round(option_exit, 2) if option_exit else None,
                    "option_points": round(points, 2) if pnl_source == "option_quote" else None,
                    "pnl_source": pnl_source,
                    "underlying_entry_price": round(underlying_entry, 2),
                    "underlying_exit_price": round(float(close_price), 2),
                    "underlying_points": round(underlying_points, 2),
                    "features": features,
                },
            )
            changed = True
        except Exception:
            pass
    if changed:
        runtime_cache_clear("trades:")


def record_live_market_price(price: float, tick_time: datetime, source: str) -> dict[str, dict[str, Any]]:
    updated: dict[str, dict[str, Any]] = {}
    completed: list[dict[str, Any]] = []
    with _live_candle_lock:
        for timeframe in ("1m", "5m", "15m"):
            bucket = timeframe_floor(tick_time, timeframe)
            live_time = chart_time(bucket)
            key = (timeframe, live_time)
            previous_keys = [
                candle_key
                for candle_key in _live_candles
                if candle_key[0] == timeframe and candle_key[1] < live_time
            ]
            for previous_key in previous_keys:
                if previous_key in _live_completed_candles_enqueued:
                    continue
                previous = dict(_live_candles[previous_key])
                previous["completed"] = True
                completed.append(previous)
                _live_completed_candles_enqueued.add(previous_key)
            candle = _live_candles.get(key)
            if candle is None:
                candle = {
                    "timeframe": timeframe,
                    "time": live_time,
                    "datetime": bucket,
                    "open": round(price, 2),
                    "high": round(price, 2),
                    "low": round(price, 2),
                    "close": round(price, 2),
                    "volume": 0,
                    "source": source,
                }
                _live_candles[key] = candle
            else:
                candle["high"] = round(max(float(candle["high"]), price), 2)
                candle["low"] = round(min(float(candle["low"]), price), 2)
                candle["close"] = round(price, 2)
                candle["source"] = source
            updated[timeframe] = dict(candle)
        today = tick_time.date()
        for candle_key in list(_live_candles):
            candle = _live_candles[candle_key]
            candle_dt = candle.get("datetime")
            if isinstance(candle_dt, datetime) and candle_dt.date() != today:
                previous = dict(candle)
                previous["completed"] = True
                completed.append(previous)
                _live_completed_candles_enqueued.add(candle_key)
                _live_candles.pop(candle_key, None)

    for candle in [*completed, *updated.values()]:
        enqueue_live_candle_persist(candle)
    return updated


def latest_live_candle(timeframe: str) -> dict[str, Any] | None:
    with _live_candle_lock:
        candles = [dict(candle) for (tf, _time), candle in _live_candles.items() if tf == timeframe]
    if not candles:
        return None
    return max(candles, key=lambda item: int(item["time"]))


def public_live_candle(candle: dict[str, Any] | None) -> dict[str, Any] | None:
    if not candle:
        return None
    return {
        "time": int(candle["time"]),
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
    }


def live_chart_update_payload(timeframe: str) -> dict[str, Any]:
    socket_status = live_socket_quote(FYERS_NIFTY_INDEX)
    latest = latest_live_candle(timeframe)
    source = "FYERS socket"
    message = socket_status.get("message") or "FYERS socket status unavailable"
    if socket_status.get("price") is not None and latest is None:
        tick_dt = datetime.now(IST).replace(tzinfo=None)
        record_live_market_price(float(socket_status["price"]), tick_dt, source="FYERS socket")
        update_live_open_trades(float(socket_status["price"]), tick_dt)
        latest = latest_live_candle(timeframe)
    if latest is None:
        price = cached_fyers_quote(FYERS_NIFTY_INDEX)
        source = "FYERS REST quote"
        message = "Live FYERS REST quote applied; waiting for first socket tick"
        tick_dt = datetime.now(IST).replace(tzinfo=None)
        record_live_market_price(float(price), tick_dt, source=source)
        update_live_open_trades(float(price), tick_dt)
        latest = latest_live_candle(timeframe)
    live_price = round(float(latest["close"]), 2) if latest else None
    live = {
        "enabled": latest is not None,
        "price": live_price,
        "source": source,
        "message": message,
        "socket": socket_status,
    }
    if _live_candle_last_error:
        live["storage_error"] = _live_candle_last_error
    if _live_signal_last_error:
        live["signal_error"] = _live_signal_last_error
    return {
        "timeframe": timeframe,
        "server_time": datetime.now(IST).isoformat(timespec="seconds"),
        "live": live,
        "candle": public_live_candle(latest),
    }


def merge_live_candles(candles: list[Any], timeframe: str) -> None:
    with _live_candle_lock:
        live = sorted(
            [dict(candle) for (tf, _time), candle in _live_candles.items() if tf == timeframe],
            key=lambda item: int(item["time"]),
        )
    if not live:
        return
    by_time = {candle_time_value(candle): candle for candle in candles}
    for live_candle in live:
        live_time = int(live_candle["time"])
        payload = [
            live_time,
            float(live_candle["open"]),
            float(live_candle["high"]),
            float(live_candle["low"]),
            float(live_candle["close"]),
        ]
        existing = by_time.get(live_time)
        if existing is None:
            candles.append(payload)
            by_time[live_time] = payload
        elif isinstance(existing, list):
            existing[1] = payload[1]
            existing[2] = payload[2]
            existing[3] = payload[3]
            existing[4] = payload[4]
        else:
            existing["open"] = payload[1]
            existing["high"] = payload[2]
            existing["low"] = payload[3]
            existing["close"] = payload[4]
    candles.sort(key=candle_time_value)


def candle_payload(frame) -> list[dict[str, Any]]:
    candles: list[list[float]] = []
    if isinstance(frame, list):
        for row in frame:
            candles.append(
                [
                    chart_time(row["datetime"]),
                    round(float(row["open"]), 2),
                    round(float(row["high"]), 2),
                    round(float(row["low"]), 2),
                    round(float(row["close"]), 2),
                ]
            )
        return candles
    for ts, row in frame.iterrows():
        candles.append(
            [
                chart_time(ts),
                round(float(row["open"]), 2),
                round(float(row["high"]), 2),
                round(float(row["low"]), 2),
                round(float(row["close"]), 2),
            ]
        )
    return candles


def candle_price_range(candles: list[Any]) -> dict[str, float] | None:
    if not candles:
        return None
    lows = [candle_low(candle) for candle in candles if math.isfinite(candle_low(candle))]
    highs = [candle_high(candle) for candle in candles if math.isfinite(candle_high(candle))]
    if not lows or not highs:
        return None
    return {"low": min(lows), "high": max(highs)}


def candle_time_value(candle: Any) -> int:
    return int(candle[0] if isinstance(candle, list) else candle["time"])


def candle_high(candle: Any) -> float:
    return float(candle[2] if isinstance(candle, list) else candle["high"])


def candle_low(candle: Any) -> float:
    return float(candle[3] if isinstance(candle, list) else candle["low"])


def candle_close(candle: Any) -> float:
    return float(candle[4] if isinstance(candle, list) else candle["close"])


def marker_is_valid(marker: dict[str, Any], valid_times: set[int]) -> bool:
    try:
        marker_time = int(marker["time"])
    except (KeyError, TypeError, ValueError):
        return False
    return marker_time in valid_times


def visible_markers(markers: list[dict[str, Any]], candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid_times = {candle_time_value(candle) for candle in candles}
    return [marker for marker in markers if marker_is_valid(marker, valid_times)]


def append_live_quote_candle(candles: list[dict[str, Any]], timeframe: str) -> dict[str, Any]:
    live = {
        "enabled": False,
        "price": None,
        "source": None,
        "message": "FYERS quote not available",
        "socket": {
            "enabled": False,
            "running": False,
            "connected": False,
            "price": None,
            "message": "FYERS socket not checked",
        },
    }
    try:
        socket_status = live_socket_quote(FYERS_NIFTY_INDEX)
        live["socket"] = socket_status
        if socket_status.get("price") is not None:
            price = float(socket_status["price"])
            source = "FYERS socket"
            message = "Live FYERS socket quote applied"
        else:
            price = cached_fyers_quote(FYERS_NIFTY_INDEX)
            source = "FYERS REST quote"
            socket_message = socket_status.get("message") or "socket tick unavailable"
            message = f"Live FYERS REST quote applied; {socket_message}"
        live_price = round(float(price), 2)
        if source != "FYERS socket" or latest_live_candle(timeframe) is None:
            tick_dt = datetime.now(IST).replace(tzinfo=None)
            record_live_market_price(live_price, tick_dt, source=source)
            update_live_open_trades(live_price, tick_dt)
        merge_live_candles(candles, timeframe)
        live_candle = latest_live_candle(timeframe)
        if live_candle:
            live_price = round(float(live_candle["close"]), 2)
            live["candle"] = {
                "time": live_candle["time"],
                "open": live_candle["open"],
                "high": live_candle["high"],
                "low": live_candle["low"],
                "close": live_candle["close"],
            }
        live.update({"enabled": True, "price": live_price, "source": source, "message": message})
        if _live_candle_last_error:
            live["storage_error"] = _live_candle_last_error
    except Exception as exc:
        live["message"] = str(exc)
    return live


def trade_markers(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for trade in trades:
        direction = str(trade.get("direction") or "")
        is_ce = direction == "CE"
        setup = str(trade.get("setup_type") or "AI setup")
        entry_time = chart_time(chart_datetime(trade.get("date"), trade.get("entry_time")))
        markers.append(
            {
                "time": entry_time,
                "position": "belowBar" if is_ce else "aboveBar",
                "color": "#059669" if is_ce else "#dc2626",
                "shape": "arrowUp" if is_ce else "arrowDown",
                "text": f"AI {direction} {int(trade.get('setup_score') or 0)} {setup[:22]}",
            }
        )
        if trade.get("exit_time"):
            exit_time = chart_time(chart_datetime(trade.get("date"), trade.get("exit_time")))
            points = trade.get("points")
            if points is None:
                points = trade_points(trade)
            markers.append(
                {
                    "time": exit_time,
                    "position": "aboveBar" if is_ce else "belowBar",
                    "color": "#1d4ed8",
                    "shape": "circle",
                    "text": f"Exit {trade.get('result') or trade.get('status')} {points or 0} pts",
                }
            )
    return markers


def skipped_markers(skipped: list[dict[str, Any]]) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for item in skipped:
        direction = str(item.get("potential_direction") or "")
        is_ce = direction == "CE"
        markers.append(
            {
                "time": chart_time(chart_datetime(item.get("date"), item.get("time"))),
                "position": "belowBar" if is_ce else "aboveBar",
                "color": "#f59e0b",
                "shape": "circle",
                "text": f"Skip {direction}: {str(item.get('skip_reason') or '')[:34]}",
            }
        )
    return markers


def signal_markers(signals) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for signal in signals:
        is_ce = signal.direction == "CE"
        markers.append(
            {
                "time": chart_time(chart_datetime(signal.date, signal.time)),
                "position": "belowBar" if is_ce else "aboveBar",
                "color": "#16a34a" if is_ce else "#e11d48",
                "shape": "arrowUp" if is_ce else "arrowDown",
                "text": f"Now {signal.direction} {signal.setup_score} {signal.setup_type[:24]}",
            }
        )
    return markers


def level_payload(
    levels,
    latest_price: float | None = None,
    price_range: dict[str, float] | None = None,
    relevance_points: float = 200.0,
) -> list[dict[str, Any]]:
    if not levels:
        return []
    level_engine = LevelEngine()
    current_price = latest_price or levels.day_high or levels.pdh or 0
    seen: set[tuple[str, float]] = set()
    min_price = float(price_range["low"]) if price_range else None
    max_price = float(price_range["high"]) if price_range else None
    relevance_band = relevance_points if current_price else None
    core_levels = {"PDH", "PDL", "PDC", "ORH", "ORL", "DAY_HIGH", "DAY_LOW"}
    colors = {
        "PDH": "#2563eb",
        "PDL": "#2563eb",
        "PDC": "#64748b",
        "ORH": "#16a34a",
        "ORL": "#dc2626",
        "DAY_HIGH": "#0f766e",
        "DAY_LOW": "#be123c",
    }
    core: list[dict[str, Any]] = []
    secondary: dict[str, list[dict[str, Any]]] = {"SWING_HIGH": [], "SWING_LOW": [], "ROUND_NUMBER": []}
    for item in level_engine.major_levels(levels, current_price):
        name = str(item["name"])
        try:
            price = round(float(item["price"]), 2)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(price) or price <= 0:
            continue
        if min_price is not None and max_price is not None and not (min_price <= price <= max_price):
            continue
        if relevance_band is not None and abs(price - float(current_price)) > relevance_band:
            continue
        key = (name, price)
        if key in seen:
            continue
        seen.add(key)
        payload = {
            "name": name,
            "price": price,
            "color": colors.get(name, "#8b5cf6"),
            "distance": abs(price - float(current_price)) if current_price else 0,
        }
        if name in core_levels:
            core.append(payload)
        elif name in secondary:
            secondary[name].append(payload)

    out = core[:]
    occupied_prices = [float(item["price"]) for item in core]
    for name, limit in [("SWING_HIGH", 4), ("SWING_LOW", 4), ("ROUND_NUMBER", 5)]:
        added = 0
        for item in sorted(secondary[name], key=lambda value: value["distance"]):
            if any(abs(float(item["price"]) - existing) < 1.0 for existing in occupied_prices):
                continue
            out.append(item)
            occupied_prices.append(float(item["price"]))
            added += 1
            if added >= limit:
                break
    return [{key: value for key, value in item.items() if key != "distance"} for item in out]


def levels_near_underlying(levels: list[dict[str, Any]], underlying: float | None, points: float = 200.0) -> list[dict[str, Any]]:
    if underlying is None:
        return levels
    out: list[dict[str, Any]] = []
    for level in levels:
        try:
            price = float(level["price"])
        except (KeyError, TypeError, ValueError):
            continue
        if abs(price - float(underlying)) <= points:
            out.append(level)
    return out


def cached_admin_chart_base(timeframe: str, symbol: str, days: int) -> dict[str, Any]:
    cache_key = (timeframe, symbol, days)
    now = time.monotonic()
    with _chart_cache_lock:
        cached = _chart_cache.get(cache_key)
        if cached and now - float(cached["stored_at"]) <= CHART_CACHE_TTL_SECONDS:
            base = cached["payload"]
            payload = {
                **base,
                "levels": list(base["levels"]),
                "counts": dict(base["counts"]),
                "latest_ai": dict(base["latest_ai"]),
                "price_range": dict(base["price_range"]) if base.get("price_range") else None,
            }
            payload["cache"] = {"hit": True, "ttl_seconds": CHART_CACHE_TTL_SECONDS}
            return payload

    end = datetime.now(IST).date()
    start = end - timedelta(days=days - 1)
    db = get_db()
    chart_rows = db.load_chart_candles(timeframe, symbol=symbol, start_date=start.isoformat(), end_date=end.isoformat())
    candles = candle_payload(chart_rows)
    price_range = candle_price_range(candles)
    trades = db.list_trades_between(start.isoformat(), end.isoformat(), symbol=symbol, limit=1000)
    markers = trade_markers(trades)
    levels = []
    latest_ai: dict[str, Any] = {"date": None, "signals": 0, "skipped": 0, "message": "No candle data"}

    try:
        if chart_rows:
            latest_date = chart_rows[-1]["datetime"].date()
            ai_start = (latest_date - timedelta(days=10)).isoformat()
            candles_1m = db.load_candles("1m", symbol=symbol, start_date=str(latest_date), end_date=str(latest_date))
            candles_5m = db.load_candles("5m", symbol=symbol, start_date=ai_start, end_date=str(latest_date))
            if not candles_1m.empty and not candles_5m.empty:
                level_set = LevelEngine().calculate(candles_5m, latest_date)
                day_signals, day_skipped = SignalEngine().generate_for_day(candles_5m, candles_1m, level_set, latest_date)
                markers.extend(signal_markers(day_signals))
                markers.extend(skipped_markers([item.to_dict() for item in day_skipped]))
                latest_close = candle_close(candles[-1]) if candles else None
                levels = level_payload(level_set, latest_close, price_range=price_range, relevance_points=1000.0)
                latest_ai = {
                    "date": str(latest_date),
                    "signals": len(day_signals),
                    "skipped": len(day_skipped),
                    "message": "Current loaded trading day evaluated",
                }
    except Exception as exc:
        latest_ai = {"date": None, "signals": 0, "skipped": 0, "message": str(exc)}

    raw_marker_count = len(markers)
    markers = sorted(visible_markers(markers, candles), key=lambda item: int(item["time"]))
    payload = {
        "symbol": symbol,
        "source_symbol": FYERS_NIFTY_INDEX,
        "timeframe": timeframe,
        "days": days,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "candles": candles,
        "markers": markers,
        "levels": levels,
        "price_range": price_range,
        "latest_ai": latest_ai,
        "counts": {
            "candles": len(candles),
            "markers": len(markers),
            "raw_markers": raw_marker_count,
            "levels": len(levels),
        },
        "cache": {"hit": False, "ttl_seconds": CHART_CACHE_TTL_SECONDS},
    }
    with _chart_cache_lock:
        _chart_cache[cache_key] = {"stored_at": now, "payload": deepcopy(payload)}
    return payload


def warm_admin_chart_cache(symbol: str = "NIFTY", days: int = 90) -> None:
    global _chart_warm_running
    with _chart_warm_lock:
        if _chart_warm_running:
            return
        _chart_warm_running = True

    def worker() -> None:
        global _chart_warm_running
        try:
            for timeframe in ("1m", "5m", "15m"):
                cached_admin_chart_base(timeframe, symbol, days)
        finally:
            with _chart_warm_lock:
                _chart_warm_running = False

    threading.Thread(target=worker, daemon=True).start()


@app.get("/auth/me")
def auth_me(request: Request) -> dict[str, Any]:
    session = read_session(request)
    if not session:
        raise HTTPException(status_code=401, detail="Login required")
    user = cached_user(session["username"])
    if not user:
        raise HTTPException(status_code=401, detail="Login required")
    return {"ok": True, "user": public_user(dict(user))}


@app.post("/auth/signup")
def auth_signup(payload: SignupPayload) -> dict[str, Any]:
    email = payload.email.strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")
    if payload.password != payload.confirm_password:
        raise HTTPException(status_code=400, detail="Password and confirm password do not match")
    try:
        get_db().create_user(email, payload.password, "user")
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Account already exists") from exc
    runtime_cache_clear(f"user:{email}")
    user = get_db().get_user(email)
    return {"ok": True, "user": public_user(dict(user))}


@app.post("/auth/login")
def auth_login(payload: LoginPayload, response: Response) -> dict[str, Any]:
    username = (payload.email or payload.username or "").strip().lower()
    user = get_db().verify_user(username, payload.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    role = str(payload.role or "user").strip().lower()
    if role == "admin" and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    response.set_cookie(
        "session",
        sign_payload({"username": user["username"], "role": user["role"]}),
        httponly=True,
        samesite="lax",
    )
    return {"ok": True, "user": public_user(dict(user))}


@app.post("/auth/logout")
def auth_logout(response: Response) -> dict[str, Any]:
    response.delete_cookie("session")
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("landing.html", {"request": request})


@app.get("/ui")
def user_console_alias() -> RedirectResponse:
    return RedirectResponse("/dashboard", status_code=303)


@app.get("/ui/admin")
def admin_console_alias() -> RedirectResponse:
    return RedirectResponse("/admin", status_code=303)


@app.get("/signup", response_class=HTMLResponse)
def signup_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("signup.html", {"request": request, "error": None})


@app.post("/signup")
def signup(request: Request, username: str = Form(...), password: str = Form(...)):
    try:
        get_db().create_user(username.strip(), password, "user")
    except Exception:
        return templates.TemplateResponse("signup.html", {"request": request, "error": "Username already exists"})
    runtime_cache_clear(f"user:{username.strip()}")
    return RedirectResponse("/user/login", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_alias() -> RedirectResponse:
    return RedirectResponse("/user/login", status_code=303)


@app.get("/user/login", response_class=HTMLResponse)
def user_login_form(request: Request) -> HTMLResponse:
    session = read_session(request)
    if session and session.get("role") in {"user", "admin"}:
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "user": None, "error": None, "title": "User Login", "action": "/user/login", "alternate_href": "/admin/login", "alternate_label": "Admin login"},
    )


@app.post("/user/login")
def user_login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = get_db().verify_user(username.strip(), password)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "user": None, "error": "Invalid username or password", "title": "User Login", "action": "/user/login", "alternate_href": "/admin/login", "alternate_label": "Admin login"},
        )
    redirect = RedirectResponse("/dashboard", status_code=303)
    redirect.set_cookie("session", sign_payload({"username": user["username"], "role": user["role"]}), httponly=True, samesite="lax")
    return redirect


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_form(request: Request) -> HTMLResponse:
    session = read_session(request)
    if session and session.get("role") == "admin":
        return RedirectResponse("/admin", status_code=303)
    if session and session.get("role") == "user":
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "user": None, "error": None, "title": "Admin Login", "action": "/admin/login", "alternate_href": "/user/login", "alternate_label": "User login"},
    )


@app.post("/admin/login")
def admin_login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = get_db().verify_user(username.strip(), password)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "user": None, "error": "Invalid username or password", "title": "Admin Login", "action": "/admin/login", "alternate_href": "/user/login", "alternate_label": "User login"},
        )
    if user["role"] != "admin":
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "user": None, "error": "This login is only for admin accounts", "title": "Admin Login", "action": "/admin/login", "alternate_href": "/user/login", "alternate_label": "User login"},
        )
    redirect = RedirectResponse("/admin", status_code=303)
    redirect.set_cookie("session", sign_payload({"username": user["username"], "role": user["role"]}), httponly=True, samesite="lax")
    return redirect


@app.get("/logout")
def logout() -> RedirectResponse:
    response = RedirectResponse("/", status_code=303)
    response.delete_cookie("session")
    return response


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, user: dict[str, Any] = Depends(require_user)) -> HTMLResponse:
    trades = cached_trades(50)
    skipped = cached_skipped(50)
    backtest_status = current_backtest_payload()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "trades": trades,
            "skipped": skipped,
            "stats": trade_stats(trades),
            "active_trade_text": active_trade_text(trades),
            "latest_backtest": backtest_status["latest"],
        },
    )


@app.post("/backtest", response_model=None)
async def backtest(
    request: Request,
    symbol: str = Form("NIFTY"),
    start_date: str | None = Form(None),
    end_date: str | None = Form(None),
    user: dict[str, Any] = Depends(require_user),
):
    clean_symbol = symbol.strip() or "NIFTY"
    clean_start = start_date.strip() if start_date else None
    clean_end = end_date.strip() if end_date else None
    with _backtest_job_lock:
        if _backtest_job.get("status") == "running":
            if wants_json(request):
                raise HTTPException(status_code=409, detail="A backtest is already running")
            return RedirectResponse("/dashboard", status_code=303)
    run_id = get_db().create_backtest_run(clean_symbol, clean_start, clean_end)
    job = {
        "id": run_id,
        "symbol": clean_symbol,
        "start_date": clean_start,
        "end_date": clean_end,
        "status": "running",
        "progress_pct": 0,
        "current_step": "Queued",
        "trades_count": 0,
        "skipped_count": 0,
        "summary": {},
        "error_message": "",
    }
    with _backtest_job_lock:
        _backtest_job.clear()
        _backtest_job.update(job)
    thread = threading.Thread(
        target=run_backtest_job,
        args=(run_id, clean_symbol, clean_start, clean_end),
        daemon=True,
    )
    thread.start()
    if wants_json(request):
        return {"ok": True, "backtest": job}
    return RedirectResponse("/dashboard", status_code=303)


@app.get("/api/backtest/latest")
def api_latest_backtest(user: dict[str, Any] = Depends(current_user)) -> dict[str, Any]:
    return current_backtest_payload()


def render_admin(
    request: Request,
    user: dict[str, Any],
    auth_url: str | None = None,
    token_result: dict[str, Any] | None = None,
    error: str | None = None,
) -> HTMLResponse:
    loader = get_loader()
    auth = loader.load_fyers_auth()
    fyers_app = loader.fyers_app_credentials()
    fyers_token_status = {
        "configured": bool(auth and auth.get("access_token")),
        "client_id": fyers_app.get("client_id"),
        "auth_path": str(loader.auth_path),
        "has_access_token": bool(auth and auth.get("access_token")),
        "has_refresh_token": bool(auth and auth.get("refresh_token")),
    }
    trades = cached_trades(100)
    skipped = cached_skipped(100)
    candle_counts = cached_candle_counts("NIFTY")
    preflight = runtime_cache_get("admin-preflight", 15, admin_preflight, copy_value=True)
    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "user": user,
            "auth": auth,
            "fyers_app": fyers_app,
            "fyers_token_status": fyers_token_status,
            "auth_path": str(loader.auth_path),
            "auth_url": auth_url,
            "token_result": token_result,
            "trades": trades,
            "skipped": skipped,
            "stats": trade_stats(trades),
            "candle_counts": candle_counts,
            "preflight": preflight,
            "error": error,
        },
    )


@app.get("/admin", response_class=HTMLResponse)
def admin_portal(request: Request, user: dict[str, Any] = Depends(require_admin)) -> HTMLResponse:
    warm_admin_chart_cache()
    return render_admin(request, user)


@app.post("/admin/fyers")
def save_fyers_auth(
    client_id: str = Form(...),
    access_token: str | None = Form(None),
    refresh_token: str | None = Form(None),
    secret_key: str | None = Form(None),
    redirect_uri: str | None = Form(None),
    user: dict[str, Any] = Depends(require_admin),
) -> RedirectResponse:
    get_loader().save_fyers_auth(
        client_id=client_id.strip(),
        access_token=access_token.strip() if access_token else None,
        refresh_token=refresh_token.strip() if refresh_token else None,
        secret_key=secret_key.strip() if secret_key else None,
        redirect_uri=redirect_uri.strip() if redirect_uri else None,
    )
    runtime_cache_clear("admin-preflight")
    runtime_cache_clear("fyers-quote:")
    return RedirectResponse("/admin", status_code=303)


@app.post("/admin/fyers/auth-url", response_class=HTMLResponse)
def admin_fyers_auth_url(
    request: Request,
    state: str = Form("price-action-ai"),
    user: dict[str, Any] = Depends(require_admin),
) -> HTMLResponse:
    try:
        url = get_loader().build_fyers_auth_url(
            state=state.strip() or "price-action-ai",
        )
        return render_admin(request, user, auth_url=url)
    except Exception as exc:
        return render_admin(request, user, error=str(exc))


@app.get("/admin/fyers/login-url", response_class=HTMLResponse)
def admin_fyers_login_url(request: Request, user: dict[str, Any] = Depends(require_admin)):
    try:
        url = get_loader().build_fyers_auth_url(state="price-action-ai")
        return RedirectResponse(url, status_code=303)
    except Exception as exc:
        return render_admin(request, user, error=str(exc))


@app.post("/admin/fyers/exchange-code", response_class=HTMLResponse)
def admin_fyers_exchange_code(
    request: Request,
    auth_code: str = Form(...),
    client_id: str | None = Form(None),
    secret_key: str | None = Form(None),
    redirect_uri: str | None = Form(None),
    user: dict[str, Any] = Depends(require_admin),
) -> HTMLResponse:
    try:
        response = get_loader().exchange_fyers_auth_code(
            auth_code=auth_code.strip(),
            client_id=client_id.strip() if client_id else None,
            secret_key=secret_key.strip() if secret_key else None,
            redirect_uri=redirect_uri.strip() if redirect_uri else None,
        )
        runtime_cache_clear("admin-preflight")
        runtime_cache_clear("fyers-quote:")
        safe_response = {key: value for key, value in response.items() if key not in {"access_token", "refresh_token"}}
        safe_response["access_token_saved"] = bool(response.get("access_token"))
        safe_response["refresh_token_saved"] = bool(response.get("refresh_token"))
        return render_admin(request, user, token_result=safe_response)
    except Exception as exc:
        return render_admin(request, user, error=str(exc))


@app.get("/admin/fyers/callback", response_class=HTMLResponse)
def admin_fyers_callback(
    request: Request,
    auth_code: str | None = None,
    code: str | None = None,
    user: dict[str, Any] = Depends(require_admin),
) -> HTMLResponse:
    try:
        response = get_loader().exchange_fyers_auth_code((auth_code or code or "").strip())
        runtime_cache_clear("admin-preflight")
        runtime_cache_clear("fyers-quote:")
        safe_response = {key: value for key, value in response.items() if key not in {"access_token", "refresh_token"}}
        safe_response["access_token_saved"] = bool(response.get("access_token"))
        safe_response["refresh_token_saved"] = bool(response.get("refresh_token"))
        return render_admin(request, user, token_result=safe_response)
    except Exception as exc:
        return render_admin(request, user, error=str(exc))


@app.get("/api/trades")
def api_trades(user: dict[str, Any] = Depends(current_user)) -> list[dict[str, Any]]:
    return cached_trades(500)


@app.get("/api/skipped-signals")
def api_skipped(user: dict[str, Any] = Depends(current_user)) -> list[dict[str, Any]]:
    return cached_skipped(500)


@app.get("/api/admin/fyers", dependencies=[Depends(require_admin)])
def api_fyers_status() -> dict[str, Any]:
    loader = get_loader()
    auth = loader.load_fyers_auth()
    return {"configured": bool(auth), "path": str(loader.auth_path), "client_id": auth.get("client_id") if auth else None}


@app.get("/api/admin/preflight", dependencies=[Depends(require_admin)])
def api_admin_preflight() -> dict[str, Any]:
    return runtime_cache_get("admin-preflight", 15, admin_preflight, copy_value=True)


@app.get("/api/admin/live-trades/monitor", dependencies=[Depends(require_admin)])
def api_admin_live_trade_monitor_status() -> dict[str, Any]:
    return live_trade_monitor_status()


@app.post("/api/admin/live-trades/monitor", dependencies=[Depends(require_admin)])
def api_admin_start_live_trade_monitor(
    duration_seconds: int = LIVE_TRADE_MONITOR_SECONDS,
    interval_seconds: int = LIVE_TRADE_MONITOR_INTERVAL_SECONDS,
) -> dict[str, Any]:
    return start_live_trade_pnl_monitor(duration_seconds=duration_seconds, interval_seconds=interval_seconds)


@app.get("/api/admin/live-chart", dependencies=[Depends(require_admin)])
def api_admin_live_chart(
    timeframe: str = "1m",
    symbol: str = "NIFTY",
    days: int = 90,
    live: bool = True,
) -> dict[str, Any]:
    if timeframe not in {"1m", "5m", "15m"}:
        raise HTTPException(status_code=400, detail="timeframe must be one of: 1m, 5m, 15m")
    days = max(1, min(int(days), 120))
    payload = cached_admin_chart_base(timeframe, symbol, days)
    candles = payload["candles"]
    if live and candles:
        candles = [list(candle) if isinstance(candle, list) else dict(candle) for candle in candles]
        payload["candles"] = candles
    live_status = append_live_quote_candle(candles, timeframe) if live and candles else {"enabled": False, "price": None, "message": "Live quote disabled"}
    if live_status.get("enabled"):
        price_range = candle_price_range(candles)
        payload["price_range"] = price_range
        payload["counts"]["candles"] = len(candles)
        payload["levels"] = [
            level for level in payload["levels"]
            if price_range and price_range["low"] <= float(level["price"]) <= price_range["high"]
        ]
    underlying = live_status.get("price") if live_status.get("enabled") else (candle_close(candles[-1]) if candles else None)
    payload["levels"] = levels_near_underlying(payload["levels"], underlying, 200.0)
    payload["counts"]["levels"] = len(payload["levels"])
    payload["live"] = live_status
    return payload


@app.get("/api/admin/live-chart/update", dependencies=[Depends(require_admin)])
def api_admin_live_chart_update(timeframe: str = "1m") -> dict[str, Any]:
    if timeframe not in {"1m", "5m", "15m"}:
        raise HTTPException(status_code=400, detail="timeframe must be one of: 1m, 5m, 15m")
    return live_chart_update_payload(timeframe)
