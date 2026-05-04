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
