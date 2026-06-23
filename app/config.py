from __future__ import annotations

import os
from dataclasses import dataclass, field
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
    fvg_min_points: float = 1.0
    fvg_lookback_candles: int = 12
    mss_min_strength: float = 0.20
    htf_bias_filter_enabled: bool = True
    htf_bias_allow_neutral: bool = True
    htf_15m_min_bars: int = 8
    htf_60m_min_bars: int = 4
    premium_discount_filter_enabled: bool = True
    premium_discount_allow_equilibrium: bool = True
    premium_discount_equilibrium_band_pct: float = 0.10
    premium_discount_min_range_points: float = 20.0
    late_reversal_start: str = "11:00"
    target_reversal_min_rejection_pct: float = 0.55
    target_reversal_hit_buffer_points: float = 2.0
    target_reversal_require_fresh_touch: bool = True
    inducement_lookback_candles: int = 8
    option_selection_enabled: bool = True
    option_selection_moneyness: str = "SCORE_BASED"
    option_score_atm_min: int = 75
    option_score_otm_min: int = 88
    option_selection_strikecount: int = 11
    option_selection_min_ltp: float = 1.0
    smart_min_zone_width_points: float = 15.0
    smart_max_zone_width_points: float = 80.0
    smart_cluster_atr_multiplier: float = 0.6
    smart_zone_atr_multiplier: float = 0.8
    smart_min_zone_score: float = 55.0
    smart_max_distance_from_current_price_atr: float = 80.0
    smart_max_allowed_breaks: int = 3
    smart_min_reaction_atr: float = 1.2
    smart_quality_displacement_atr: float = 1.4
    smart_quality_structure_lookback: int = 6
    smart_quality_max_base_candles: int = 3
    smart_quality_min_body_pct: float = 0.55
    smart_quality_swing_reaction_atr: float = 1.0
    smart_quality_sweep_reclaim_atr: float = 0.15
    smart_atr_period: int = 14
    smart_recent_trading_days: int = 5
    smart_max_age_days_without_touch: int = 30
    smart_trade_enabled: bool = True
    smart_trade_min_zone_score: float = 60.0
    smart_trade_confirmation_window_candles: int = 2
    smart_trade_retest_window_candles: int = 8
    smart_trade_retest_min_score: int = 75
    smart_trade_max_chase_atr: float = 1.5
    smart_trade_sl_atr_buffer: float = 0.20
    smart_trade_sl_zone_inner_fraction: float = 0.25
    smart_trade_retest_score_bonus: int = 8
    smart_trade_reaction_min_zone_score: float = 75.0
    smart_trade_zone_history_days: int = 2
    smart_trade_zone_refresh_candles: int = 12
    smart_trade_htf_override_min_score: int = 75
    smart_trade_htf_override_min_zone_score: float = 80.0
    smart_trade_rejection_override_min_zone_score: float = 85.0
    smart_trade_sweep_reclaim_min_body_pct: float = 0.55
    smart_trade_sweep_reclaim_min_range_atr: float = 1.0
    smart_trade_counter_pd_min_score: int = 72
    smart_trade_counter_pd_min_zone_score: float = 78.0
    smart_trade_reaction_requires_hold: bool = False
    smart_trade_min_forward_space_width_ratio: float = 0.10
    # Trend continuation: buy/sell the pullback into a with-trend zone while the trend is intact.
    smart_trade_continuation_enabled: bool = True
    smart_trade_continuation_pullback_lookback: int = 4
    smart_trade_continuation_min_zone_score: float = 65.0
    # TEMP strong-zone experiment: remove these three fields and the matching TEMP checks in levels.py to revert.
    smart_temp_strong_move_zone_enabled: bool = False
    smart_temp_strong_move_points: float = 100.0
    smart_temp_strong_move_min_score: float = 90.0
    # TEMP freshness filter experiment: remove these fields and SmartTradeEngine._freshness_filter_reason to revert.
    smart_temp_freshness_filter_enabled: bool = True
    smart_temp_min_freshness_enhancer: float = 1.5
    smart_temp_freshness_filter_setups: tuple[str, ...] = (
        "SMART_ZONE_BREAK_CONFIRMATION",
        "SMART_ZONE_RETEST_CONFIRMATION",
        "SMART_ZONE_SUPPORT_REACTION_CONFIRMATION",
        "SMART_ZONE_RESISTANCE_REJECTION_CONFIRMATION",
    )
    paper_breakeven_after_r: float = 1.0
    paper_profit_lock_after_r: float = 1.0
    paper_profit_lock_r: float = 0.5
    paper_near_target_exit_pct: float = 0.95
    smart_level_weights: dict[str, float] = field(
        default_factory=lambda: {
            "reaction_score": 0.25,
            "speed_score": 0.08,
            "touch_quality_score": 0.13,
            "freshness_score": 0.14,
            "recency_score": 0.10,
            "htf_visibility_score": 0.09,
            "volume_score": 0.05,
            "gap_overlap_score": 0.03,
            "liquidity_sweep_score": 0.05,
            "departure_fvg_score": 0.05,
            "confluence_score": 0.03,
        }
    )

    # Scoring Weights
    weight_bos: int = 20
    weight_displacement_max: int = 20
    weight_entry: int = 15
    weight_sl: int = 10
    weight_time_max: int = 5


config = StrategyConfig()
mysql_config = MySQLConfig()
fyers_config = FyersConfig()
