from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from app.config import FYERS_AUTH_PATH, FyersConfig, fyers_config
from app.options_pricing import option_snapshot_from_chain_payload, quote_symbol_candidates, to_float, to_int


REQUIRED_COLUMNS = {"datetime", "open", "high", "low", "close"}


class DataValidationError(ValueError):
    pass


@dataclass
class CandleBundle:
    candles_1m: pd.DataFrame
    candles_5m: pd.DataFrame
    candles_15m: pd.DataFrame


class DataLoader:
    """Loads and validates candles for strategy simulation.

    Fyers auth and quote access are kept here, but there is intentionally no
    order placement method in V1.
    """

    def __init__(self, auth_path: Path = FYERS_AUTH_PATH, cfg: FyersConfig = fyers_config) -> None:
        self.auth_path = auth_path
        self.cfg = cfg

    def load_csv(self, path: str | Path, timeframe: str) -> pd.DataFrame:
        frame = pd.read_csv(path)
        return self.validate_candles(frame, timeframe)

    def validate_candles(self, frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        missing = REQUIRED_COLUMNS - set(frame.columns)
        if missing:
            raise DataValidationError(f"{timeframe} candles missing columns: {sorted(missing)}")

        candles = frame.copy()
        candles["datetime"] = self._parse_datetime_column(candles["datetime"])
        candles = candles.sort_values("datetime").drop_duplicates("datetime")
        candles = candles.set_index("datetime")
        for column in ["open", "high", "low", "close", "volume"]:
            if column in candles.columns:
                candles[column] = pd.to_numeric(candles[column], errors="coerce")

        if candles[["open", "high", "low", "close"]].isna().any().any():
            raise DataValidationError(f"{timeframe} candles contain invalid OHLC values")
        if ((candles["high"] < candles[["open", "close"]].max(axis=1)) | (candles["low"] > candles[["open", "close"]].min(axis=1))).any():
            raise DataValidationError(f"{timeframe} candles contain inconsistent OHLC ranges")

        candles["date"] = candles.index.date
        candles["time"] = candles.index.strftime("%H:%M")
        return candles

    def _parse_datetime_column(self, values: pd.Series) -> pd.Series:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d-%m-%Y %H:%M:%S",
            "%d-%m-%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
        ]
        source = values.astype(str).str.strip()
        parsed = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
        for fmt in formats:
            missing = parsed.isna()
            if not missing.any():
                break
            parsed.loc[missing] = pd.to_datetime(source.loc[missing], format=fmt, errors="coerce")
        if parsed.isna().any():
            parsed.loc[parsed.isna()] = pd.to_datetime(source.loc[parsed.isna()], dayfirst=True, errors="coerce")
        if parsed.isna().any():
            bad_examples = source.loc[parsed.isna()].head(3).to_list()
            raise DataValidationError(f"Invalid datetime values: {bad_examples}")
        return parsed

    def resample_from_1m(self, candles_1m: pd.DataFrame) -> CandleBundle:
        base = candles_1m.copy()
        if "volume" not in base.columns:
            base["volume"] = 0

        def resample(rule: str) -> pd.DataFrame:
            out = base.resample(rule, label="left", closed="left").agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            out = out.dropna(subset=["open", "high", "low", "close"])
            out["date"] = out.index.date
            out["time"] = out.index.strftime("%H:%M")
            return out

        return CandleBundle(candles_1m=base, candles_5m=resample("5min"), candles_15m=resample("15min"))

    def save_fyers_auth(
        self,
        client_id: str,
        access_token: str | None = None,
        refresh_token: str | None = None,
        secret_key: str | None = None,
        redirect_uri: str | None = None,
    ) -> None:
        self.auth_path.parent.mkdir(parents=True, exist_ok=True)
        existing = self._load_fyers_auth_file()
        payload = existing.copy()
        if client_id and not self.cfg.client_id:
            payload["client_id"] = client_id
        if access_token:
            payload["access_token"] = access_token
        if refresh_token:
            payload["refresh_token"] = refresh_token
        if secret_key and not self.cfg.secret_key:
            payload["secret_key"] = secret_key
        if redirect_uri and not self.cfg.redirect_uri:
            payload["redirect_uri"] = redirect_uri
        self.auth_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _load_fyers_auth_file(self) -> dict[str, Any]:
        if not self.auth_path.exists():
            return {}
        return json.loads(self.auth_path.read_text(encoding="utf-8"))

    def load_fyers_auth(self) -> dict[str, Any] | None:
        auth = {key: value for key, value in self._load_fyers_auth_file().items() if value}
        if self.cfg.client_id:
            auth["client_id"] = self.cfg.client_id
        if self.cfg.secret_key:
            auth["secret_key"] = self.cfg.secret_key
        if self.cfg.redirect_uri:
            auth["redirect_uri"] = self.cfg.redirect_uri
        return auth if any(auth.values()) else None

    def fyers_app_credentials(self) -> dict[str, str]:
        auth = self.load_fyers_auth() or {}
        return {
            "client_id": auth.get("client_id") or self.cfg.client_id,
            "secret_key": auth.get("secret_key") or self.cfg.secret_key,
            "redirect_uri": auth.get("redirect_uri") or self.cfg.redirect_uri,
        }

    def build_fyers_auth_url(
        self,
        client_id: str | None = None,
        secret_key: str | None = None,
        redirect_uri: str | None = None,
        state: str = "price-action-ai",
    ) -> str:
        credentials = self.fyers_app_credentials()
        client_id = client_id or credentials.get("client_id")
        secret_key = secret_key or credentials.get("secret_key")
        redirect_uri = redirect_uri or credentials.get("redirect_uri")
        if not client_id or not secret_key or not redirect_uri:
            raise RuntimeError("FYERS_CLIENT_ID, FYERS_SECRET_KEY, and FYERS_REDIRECT_URI must be configured in .env")
        try:
            from fyers_apiv3 import fyersModel
        except ImportError as exc:
            raise RuntimeError("Install fyers-apiv3 to use Fyers auth") from exc
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code",
            state=state,
        )
        return session.generate_authcode()

    def exchange_fyers_auth_code(
        self,
        auth_code: str,
        client_id: str | None = None,
        secret_key: str | None = None,
        redirect_uri: str | None = None,
    ) -> dict[str, Any]:
        auth = self.fyers_app_credentials()
        client_id = client_id or auth.get("client_id")
        secret_key = secret_key or auth.get("secret_key")
        redirect_uri = redirect_uri or auth.get("redirect_uri")
        if not client_id or not secret_key or not redirect_uri:
            raise RuntimeError("Fyers client ID, secret key, and redirect URI are required before exchanging auth code")
        try:
            from fyers_apiv3 import fyersModel
        except ImportError as exc:
            raise RuntimeError("Install fyers-apiv3 to use Fyers auth") from exc
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code",
        )
        session.set_token(auth_code)
        response = session.generate_token()
        if response.get("s") == "error" or not response.get("access_token"):
            raise RuntimeError(f"Fyers token exchange failed: {response}")
        self.save_fyers_auth(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            access_token=response.get("access_token"),
            refresh_token=response.get("refresh_token"),
        )
        return response

    def fyers_client(self):
        auth = self.load_fyers_auth()
        if not auth or not auth.get("access_token"):
            raise RuntimeError("Fyers access token is not configured")
        try:
            from fyers_apiv3 import fyersModel
        except ImportError as exc:
            raise RuntimeError("Install fyers-apiv3 to use Fyers market data") from exc
        return fyersModel.FyersModel(
            client_id=auth["client_id"],
            token=auth["access_token"],
            is_async=False,
            log_path="",
        )

    def fetch_fyers_quote_raw(self, symbols: list[str]) -> dict[str, Any]:
        if not symbols:
            raise ValueError("At least one symbol is required")
        return self.fyers_client().quotes({"symbols": ",".join(symbols)})

    def fetch_fyers_quote_details(self, symbol: str) -> dict[str, Any] | None:
        requested = str(symbol or "").strip()
        if not requested:
            return None
        client = self.fyers_client()
        for candidate in quote_symbol_candidates(requested):
            payload = client.quotes({"symbols": candidate})
            if to_int(payload.get("code"), 0) not in {0, 200} and payload.get("s") != "ok":
                continue
            rows = payload.get("d") or payload.get("data") or []
            if not isinstance(rows, list) or not rows:
                continue
            row = rows[0] if isinstance(rows[0], dict) else {}
            values = row.get("v") if isinstance(row.get("v"), dict) else {}
            ltp = to_float(values.get("lp"), to_float(row.get("lp"), 0.0))
            if ltp <= 0.0:
                continue
            ts = to_int(values.get("tt"), to_int(row.get("tt"), 0))
            if ts > 10_000_000_000:
                ts = int(ts / 1000)
            return {
                "requested_symbol": requested,
                "symbol": str(row.get("n") or row.get("symbol") or candidate).strip(),
                "resolved_symbol": str(row.get("n") or row.get("symbol") or candidate).strip(),
                "ltp": float(ltp),
                "timestamp": ts,
                "raw": payload,
            }
        return None

    def fetch_fyers_quote(self, symbol: str) -> float:
        data = self.fetch_fyers_quote_raw([symbol])
        values = data.get("d") or []
        if not values:
            raise RuntimeError(f"No Fyers quote returned for {symbol}")
        return float(values[0]["v"]["lp"])

    def fetch_fyers_option_chain_raw(self, symbol: str, strikecount: int = 11) -> dict[str, Any]:
        return self.fyers_client().optionchain(
            data={
                "symbol": symbol,
                "strikecount": max(11, int(strikecount)),
                "timestamp": "",
            }
        )

    def fetch_fyers_option_snapshot(self, symbol: str, strikecount: int = 11) -> dict[str, Any] | None:
        return option_snapshot_from_chain_payload(self.fetch_fyers_option_chain_raw(symbol, strikecount))

    def fetch_fyers_history(
        self,
        symbol: str,
        resolution: str,
        range_from: str,
        range_to: str,
        date_format: str = "1",
        cont_flag: str = "1",
    ) -> pd.DataFrame:
        payload = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": date_format,
            "range_from": range_from,
            "range_to": range_to,
            "cont_flag": cont_flag,
        }
        response = self.fyers_client().history(payload)
        if response.get("s") == "no_data":
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
        if response.get("s") != "ok":
            raise RuntimeError(f"Fyers history error: {response}")
        candles = response.get("candles") or []
        frame = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        if frame.empty:
            return frame
        frame["datetime"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        return frame[["datetime", "open", "high", "low", "close", "volume"]]

    def save_fyers_history_csv(self, frame: pd.DataFrame, symbol: str, resolution: str, range_from: str, range_to: str) -> Path:
        folder = self.auth_path.parent / "fyers_history"
        folder.mkdir(parents=True, exist_ok=True)
        safe_symbol = symbol.replace(":", "_").replace("/", "_")
        path = folder / f"{safe_symbol}_{resolution}_{range_from}_{range_to}.csv"
        frame.to_csv(path, index=False)
        return path
