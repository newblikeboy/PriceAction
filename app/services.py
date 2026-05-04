from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from app.backtest import BacktestRunner
from app.data_loader import DataLoader
from app.storage.database import Database
from app.storage.logger import TradeLogger


class StrategyService:
    def __init__(self, database: Database | None = None) -> None:
        self.loader = DataLoader()
        self.runner = BacktestRunner()
        self.database = database or Database()
        self.logger = TradeLogger(self.database)

    def run_csv_backtest(self, csv_path: str | Path) -> dict:
        candles_1m = self.loader.load_csv(csv_path, "1m")
        bundle = self.loader.resample_from_1m(candles_1m)
        result = self.runner.run(bundle.candles_5m, bundle.candles_1m)
        self.logger.log_backtest(result.trades, result.skipped_signals)
        return {
            "summary": result.summary,
            "trades": [trade.to_dict() for trade in result.trades],
            "skipped_signals": [signal.to_dict() for signal in result.skipped_signals],
        }

    def run_database_backtest(
        self,
        symbol: str = "NIFTY",
        start_date: str | None = None,
        end_date: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        if progress_callback:
            progress_callback({"percent": 0, "current_step": "Loading candles"})
        candles_1m = self.database.load_candles("1m", symbol=symbol, start_date=start_date, end_date=end_date)
        candles_5m = self.database.load_candles("5m", symbol=symbol, start_date=start_date, end_date=end_date)
        if candles_1m.empty:
            raise RuntimeError("No 1m candles found in database. Run the FYERS backfill script first.")
        if candles_5m.empty:
            if progress_callback:
                progress_callback({"percent": 0, "current_step": "Resampling 5m candles"})
            candles_5m = self.loader.resample_from_1m(candles_1m).candles_5m
        result = self.runner.run(candles_5m, candles_1m, progress_callback=progress_callback)
        if progress_callback:
            progress_callback(
                {
                    "percent": 100,
                    "current_step": "Saving backtest logs",
                    "trades_count": len(result.trades),
                    "skipped_count": len(result.skipped_signals),
                }
            )
        self.logger.log_backtest(result.trades, result.skipped_signals)
        return {
            "summary": result.summary,
            "trades": [trade.to_dict() for trade in result.trades],
            "skipped_signals": [signal.to_dict() for signal in result.skipped_signals],
        }
