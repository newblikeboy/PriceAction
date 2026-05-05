from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
DATA_DIR = BASE_DIR / "data"
FYERS_AUTH_PATH = DATA_DIR / "fyers_auth.json"


@dataclass(frozen=True)
class FyersConfig:
    client_id: str = os.getenv("FYERS_CLIENT_ID", "")
    secret_key: str = os.getenv("FYERS_SECRET_KEY", "")
    redirect_uri: str = os.getenv("FYERS_REDIRECT_URI", "")
    user_id: str = os.getenv("FYERS_USER_ID", "")
    pin: str = os.getenv("FYERS_PIN", "")
    totp_key: str = os.getenv("FYERS_TOTP_KEY", "")
    login_app_id: str = os.getenv("FYERS_LOGIN_APP_ID", "2")

    @property
    def is_configured(self) -> bool:
        return bool(self.client_id and self.secret_key and self.redirect_uri)

    @property
    def is_totp_configured(self) -> bool:
        return bool(self.is_configured and self.user_id and self.pin and self.totp_key)


@dataclass(frozen=True)
class MySQLConfig:
    uri: str = os.getenv("MYSQL_URI", "")
    connect_timeout: int = int(os.getenv("MYSQL_CONNECT_TIMEOUT", "10"))

    @property
    def is_configured(self) -> bool:
        return bool(self.uri)

    @property
    def host(self) -> str:
        return self._parsed().hostname or ""

    @property
    def port(self) -> int:
        return self._parsed().port or 3306

    @property
    def user(self) -> str:
        return unquote(self._parsed().username or "")

    @property
    def password(self) -> str:
        return unquote(self._parsed().password or "")

    @property
    def database(self) -> str:
        return unquote(self._parsed().path.lstrip("/"))

    @property
    def ssl_ca_path(self) -> str:
        query = parse_qs(self._parsed().query)
        for key in ("ssl_ca", "ssl-ca", "ssl_ca_path", "ssl-ca-path"):
            if query.get(key):
                return query[key][0]
        return ""

    @property
    def ssl_required(self) -> bool:
        query = parse_qs(self._parsed().query)
        mode = (query.get("ssl-mode") or query.get("ssl_mode") or [""])[0].upper()
        return mode in {"REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"} or bool(self.ssl_ca_path)

    def validate(self) -> None:
        parsed = self._parsed()
        if parsed.scheme not in {"mysql", "mysql+pymysql"}:
            raise ValueError("MYSQL_URI must use mysql:// or mysql+pymysql://")
        if not self.host or not self.user or not self.password or not self.database:
            raise ValueError("MYSQL_URI must include user, password, host, port, and database")

    def _parsed(self):
        return urlparse(self.uri)


@dataclass(frozen=True)
class StrategyConfig:
    symbol: str = "NIFTY"
    opening_range_start: str = "09:15"
    opening_range_end: str = "09:30"
    no_fresh_trade_after: str = "14:15"
    square_off_time: str = "15:15"
    best_window_end: str = "11:00"
    continuation_window_end: str = "13:30"
    swing_left: int = 2
    swing_right: int = 2
    displacement_body_pct: float = 0.60
    displacement_range_multiplier: float = 1.20
    displacement_avg_lookback: int = 10
    close_near_extreme_pct: float = 0.25
    minimum_rr: float = 1.5
    min_setup_score: int = 60
    sl_buffer_points: float = 2.0
    max_entry_sl_points: float = 90.0
    round_number_step: int = 100
    failed_level_limit: int = 2
    entry_mode: str = "close"

    # Scoring Weights
    weight_bos: int = 20
    weight_displacement_max: int = 20
    weight_entry: int = 15
    weight_sl: int = 10
    weight_time_max: int = 5


config = StrategyConfig()
mysql_config = MySQLConfig()
fyers_config = FyersConfig()
