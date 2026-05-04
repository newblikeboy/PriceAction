from __future__ import annotations

from app.domain import PaperTrade, SkippedSignal
from app.storage.database import Database


class TradeLogger:
    def __init__(self, database: Database | None = None) -> None:
        self.database = database or Database()

    def log_trade(self, trade: PaperTrade) -> int:
        return self.database.insert_trade(trade.to_dict())

    def log_skipped_signal(self, skipped: SkippedSignal) -> int:
        return self.database.insert_skipped(skipped.to_dict())

    def log_backtest(self, trades: list[PaperTrade], skipped: list[SkippedSignal]) -> None:
        self.database.insert_backtest_logs(
            [trade.to_dict() for trade in trades],
            [signal.to_dict() for signal in skipped],
        )
