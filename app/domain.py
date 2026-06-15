from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Literal


Direction = Literal["CE", "PE"]
TradeStatus = Literal["OPEN", "CLOSED"]


@dataclass
class LevelSet:
    trading_date: date
    pdh: float | None
    pdl: float | None
    pdc: float | None
    orh: float | None
    orl: float | None
    swing_highs: list[dict[str, Any]] = field(default_factory=list)
    swing_lows: list[dict[str, Any]] = field(default_factory=list)
    day_high: float | None = None
    day_low: float | None = None


ZoneStatus = Literal["fresh", "active", "tested", "weakened", "broken", "flipped"]


@dataclass
class SmartZone:
    zone_id: str
    zone_type: str
    low: float
    high: float
    midpoint: float
    created_at: Any
    last_touched_at: Any | None
    touch_count: int
    reaction_count: int
    break_count: int
    score: float
    freshness_score: float
    recency_score: float
    reaction_score: float
    speed_score: float
    touch_quality_score: float
    htf_visibility_score: float
    volume_score: float
    gap_overlap_score: float
    liquidity_sweep_score: float
    noise_penalty: float
    status: ZoneStatus
    notes: list[str] = field(default_factory=list)
    enhancers: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = str(self.created_at) if self.created_at is not None else None
        payload["last_touched_at"] = str(self.last_touched_at) if self.last_touched_at is not None else None
        return payload


@dataclass
class SmartLevelResult:
    current_price: float
    atr: float
    zones: list[SmartZone]
    nearest_support_demand: list[SmartZone]
    nearest_resistance_supply: list[SmartZone]
    strongest_zones: list[SmartZone]
    recently_touched_zones: list[SmartZone]
    fresh_untested_zones: list[SmartZone]

    def to_dict(self) -> dict[str, Any]:
        return {
            "current_price": round(float(self.current_price), 2),
            "atr": round(float(self.atr), 2),
            "zones": [zone.to_dict() for zone in self.zones],
            "nearest_support_demand": [zone.to_dict() for zone in self.nearest_support_demand],
            "nearest_resistance_supply": [zone.to_dict() for zone in self.nearest_resistance_supply],
            "strongest_zones": [zone.to_dict() for zone in self.strongest_zones],
            "recently_touched_zones": [zone.to_dict() for zone in self.recently_touched_zones],
            "fresh_untested_zones": [zone.to_dict() for zone in self.fresh_untested_zones],
        }


@dataclass
class SignalCandidate:
    date: str
    time: str
    symbol: str
    direction: Direction
    setup_type: str
    entry_index_price: float
    sl_index_price: float
    target_index_price: float
    risk_points: float
    reward_points: float
    risk_reward: float
    setup_score: int
    features: dict[str, Any]
    notes: list[str] = field(default_factory=list)


@dataclass
class PaperTrade:
    date: str
    symbol: str
    direction: Direction
    setup_type: str
    entry_time: str
    entry_index_price: float
    sl_index_price: float
    target_index_price: float
    risk_points: float
    reward_points: float
    risk_reward: float
    setup_score: int
    status: TradeStatus = "OPEN"
    exit_time: str | None = None
    exit_index_price: float | None = None
    exit_reason: str | None = None
    result: str | None = None
    r_multiple: float | None = None
    option_symbol: str | None = None
    option_side: str | None = None
    option_strike: float | None = None
    option_entry_ltp: float | None = None
    option_mark_ltp: float | None = None
    option_exit_ltp: float | None = None
    option_points: float | None = None
    pnl_source: str | None = None
    underlying_entry_price: float | None = None
    underlying_exit_price: float | None = None
    underlying_points: float | None = None
    notes: list[str] = field(default_factory=list)
    features: dict[str, Any] = field(default_factory=dict)
    max_favorable_excursion: float = 0.0
    max_adverse_excursion: float = 0.0

    @classmethod
    def from_signal(cls, signal: SignalCandidate) -> "PaperTrade":
        return cls(
            date=signal.date,
            symbol=signal.symbol,
            direction=signal.direction,
            setup_type=signal.setup_type,
            entry_time=signal.time,
            entry_index_price=signal.entry_index_price,
            sl_index_price=signal.sl_index_price,
            target_index_price=signal.target_index_price,
            risk_points=signal.risk_points,
            reward_points=signal.reward_points,
            risk_reward=signal.risk_reward,
            setup_score=signal.setup_score,
            notes=signal.notes[:],
            features=signal.features.copy(),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SkippedSignal:
    date: str
    time: str
    potential_direction: Direction
    potential_setup: str
    skip_reason: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def hhmm(value: datetime) -> str:
    return value.strftime("%H:%M")
