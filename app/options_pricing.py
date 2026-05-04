from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    IST = ZoneInfo("Asia/Kolkata")
except ZoneInfoNotFoundError:
    IST = timezone(timedelta(hours=5, minutes=30))


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def normalize_option_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    return (
        text.replace(" ", "")
        .replace("-", "")
        .replace("_", "")
        .replace("INDEX", "")
        .replace("NIFTY50", "NIFTY")
    )


def quote_symbol_candidates(symbol: str) -> list[str]:
    text = str(symbol or "").strip()
    if not text:
        return []
    raw = [text] if ":" in text else [text, f"NFO:{text}", f"NSE:{text}"]
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        key = item.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _expiry_date_from_payload(data: dict[str, Any]) -> str:
    expiry_data = data.get("expiryData")
    if not isinstance(expiry_data, list) or not expiry_data:
        return ""
    first = expiry_data[0] if isinstance(expiry_data[0], dict) else {}
    raw_date = str(first.get("date") or "").strip()
    if raw_date:
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw_date, fmt).date().isoformat()
            except ValueError:
                continue
        return raw_date
    raw_expiry = first.get("expiry")
    if raw_expiry:
        try:
            return datetime.fromtimestamp(int(raw_expiry), IST).date().isoformat()
        except (TypeError, ValueError, OSError):
            return ""
    return ""


def option_snapshot_from_chain_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or to_int(payload.get("code"), 0) != 200:
        return None
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    chain = data.get("optionsChain") if isinstance(data.get("optionsChain"), list) else []
    if not chain:
        return None

    ts = to_int(data.get("timestamp"), int(datetime.now(IST).timestamp()))
    underlying = next((row for row in chain if isinstance(row, dict) and not row.get("option_type")), {})
    spot = to_float(underlying.get("ltp"), to_float(underlying.get("fp"), 0.0)) if isinstance(underlying, dict) else 0.0
    expiry_date = _expiry_date_from_payload(data)
    rows = [row for row in chain if isinstance(row, dict) and str(row.get("option_type") or "").upper() in {"CE", "PE"}]
    strike_values = [to_float(row.get("strike_price"), 0.0) for row in rows]
    strike_values = [strike for strike in strike_values if strike > 0.0]
    if not strike_values:
        return None

    atm_strike = min(strike_values, key=lambda strike: abs(strike - spot)) if spot else sorted(strike_values)[len(strike_values) // 2]
    sorted_strikes = sorted(set(strike_values))
    diffs = [sorted_strikes[idx + 1] - sorted_strikes[idx] for idx in range(len(sorted_strikes) - 1)]
    positives = [diff for diff in diffs if diff > 0]
    strike_step = min(positives) if positives else 50.0
    band_points = max(50.0, strike_step * 5.0)

    compact: list[dict[str, Any]] = []
    for row in rows:
        option_type = str(row.get("option_type") or "").upper()
        strike = to_float(row.get("strike_price"), 0.0)
        if strike <= 0.0 or abs(strike - atm_strike) > band_points:
            continue
        symbol = str(row.get("symbol") or row.get("symbol_ticker") or row.get("name") or "").strip()
        compact.append(
            {
                "option_type": option_type,
                "side": option_type,
                "strike": float(strike),
                "symbol": symbol,
                "symbol_normalized": normalize_option_symbol(symbol),
                "symbol_token": str(row.get("symbol_token") or row.get("symboltoken") or row.get("fy_token") or row.get("token") or "").strip(),
                "exchange": str(row.get("exchange") or "NFO").strip().upper(),
                "underlying": "NIFTY",
                "expiry_date": expiry_date,
                "ltp": to_float(row.get("ltp"), 0.0),
                "volume": to_float(row.get("volume"), 0.0),
                "oi_change": to_float(row.get("oich"), 0.0),
            }
        )

    atm_ce = next((row for row in compact if row["option_type"] == "CE" and abs(row["strike"] - atm_strike) <= 1e-9), {})
    atm_pe = next((row for row in compact if row["option_type"] == "PE" and abs(row["strike"] - atm_strike) <= 1e-9), {})
    return {
        "timestamp": ts,
        "spot_price": float(spot),
        "strike_step": float(strike_step),
        "atm_strike": float(atm_strike),
        "atm_ce_ltp": to_float(atm_ce.get("ltp"), 0.0),
        "atm_pe_ltp": to_float(atm_pe.get("ltp"), 0.0),
        "strikes": compact,
    }


def select_option_contract(
    *,
    direction: str,
    spot_price: float,
    setup_score: int,
    features: dict[str, Any],
    option_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    side = "CE" if str(direction).upper() == "CE" else "PE"
    snapshot = option_snapshot if isinstance(option_snapshot, dict) else {}
    strikes = snapshot.get("strikes") if isinstance(snapshot.get("strikes"), list) else []
    strike_values = sorted({to_float(row.get("strike"), 0.0) for row in strikes if isinstance(row, dict) and to_float(row.get("strike"), 0.0) > 0.0})
    strike_step = to_float(snapshot.get("strike_step"), 0.0)
    if strike_step <= 0.0 and len(strike_values) >= 2:
        positives = [strike_values[idx + 1] - strike_values[idx] for idx in range(len(strike_values) - 1) if strike_values[idx + 1] > strike_values[idx]]
        strike_step = min(positives) if positives else 50.0
    if strike_step <= 0.0:
        strike_step = 50.0

    atm_strike = to_float(snapshot.get("atm_strike"), 0.0)
    if atm_strike <= 0.0:
        atm_strike = round(float(spot_price) / strike_step) * strike_step

    confidence = max(0.0, min(float(setup_score or 0) / 100.0, 1.0))
    speed = to_float(features.get("candle_speed"), 0.0)
    expansion = to_float(features.get("range_expansion"), 0.0)
    style = "ATM"
    offset = 0
    if confidence >= 0.78 and speed >= 1.35 and expansion >= 1.45:
        style = "OTM_1"
        offset = 1
    elif confidence >= 0.62:
        style = "ITM_1"
        offset = -1

    side_sign = 1 if side == "CE" else -1
    target_strike = float(atm_strike + offset * side_sign * strike_step)
    if strike_values:
        target_strike = min(strike_values, key=lambda strike: abs(strike - target_strike))

    pool = [
        row for row in strikes
        if isinstance(row, dict)
        and str(row.get("option_type") or row.get("side") or "").upper() == side
        and abs(to_float(row.get("strike"), 0.0) - target_strike) <= 1e-9
    ]
    selected = max(pool, key=lambda row: to_float(row.get("volume"), 0.0), default={})
    return {
        "side": side,
        "strike": float(target_strike),
        "atm_strike": float(atm_strike),
        "step": float(strike_step),
        "style": style,
        "symbol": str(selected.get("symbol") or "").strip(),
        "symbol_token": str(selected.get("symbol_token") or "").strip(),
        "exchange": str(selected.get("exchange") or "NFO").strip().upper(),
        "underlying": str(selected.get("underlying") or "NIFTY").strip().upper(),
        "expiry_date": str(selected.get("expiry_date") or "").strip(),
        "ltp_ref": to_float(selected.get("ltp"), 0.0),
        "snapshot_ts": to_int(snapshot.get("timestamp"), 0),
        "source": "option_snapshot" if snapshot else "spot_fallback",
    }
