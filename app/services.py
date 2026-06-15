from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from app.backtest import BacktestRunner
from app.config import config
from app.data_loader import DataLoader
from app.engines.levels import LevelEngine
from app.storage.database import Database
from app.storage.logger import TradeLogger


class StrategyService:
    def __init__(self, database: Database | None = None) -> None:
        self.loader = DataLoader()
        self.runner = BacktestRunner()
        self.levels = LevelEngine()
        self.database = database or Database()
        self.logger = TradeLogger(self.database)

    def run_csv_backtest(self, csv_path: str | Path) -> dict:
        candles_5m = self.loader.load_csv(csv_path, "5m")
        result = self.runner.run(candles_5m)
        self.logger.log_backtest(result.trades, result.skipped_signals)
        return {
            "summary": result.summary,
            "trades": [trade.to_dict() for trade in result.trades],
            "skipped_signals": [signal.to_dict() for signal in result.skipped_signals],
        }

    def smart_levels_from_candles(
        self,
        candles_5m,
        current_price: float | None = None,
        export_format: str | None = None,
    ) -> dict | str:
        result = self.levels.calculate_smart_zones(candles_5m, current_price=current_price)
        if export_format == "json":
            return self.levels.smart_zones_json(result)
        if export_format == "csv":
            return self.levels.smart_zones_csv(result)
        return result.to_dict()

    def smart_levels_from_database(
        self,
        symbol: str = "NIFTY",
        start_date: str | None = None,
        end_date: str | None = None,
        current_price: float | None = None,
        export_format: str | None = None,
    ) -> dict | str:
        candles_5m = self.database.load_candles("5m", symbol=symbol, start_date=start_date, end_date=end_date)
        if candles_5m.empty:
            raise RuntimeError("No 5m candles found for smart level calculation.")
        return self.smart_levels_from_candles(candles_5m, current_price=current_price, export_format=export_format)

    def run_database_backtest(
        self,
        symbol: str = "NIFTY",
        start_date: str | None = None,
        end_date: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        backtest_run_id: int | None = None,
    ) -> dict:
        if progress_callback:
            progress_callback({"percent": 0, "current_step": "Loading 5m candles"})
        load_start_date = self._backtest_load_start_date(start_date)
        candles_5m = self.database.load_candles("5m", symbol=symbol, start_date=load_start_date, end_date=end_date)
        if candles_5m.empty:
            raise RuntimeError("No 5m candles found in database. Run the FYERS backfill script first.")
        option_snapshot = self._live_option_snapshot_for_backtest(progress_callback)
        result = self.runner.run(
            candles_5m,
            progress_callback=progress_callback,
            option_snapshot=option_snapshot,
            test_start_date=start_date,
            test_end_date=end_date,
        )
        if progress_callback:
            progress_callback(
                {
                    "percent": 100,
                    "current_step": "Saving backtest logs",
                    "trades_count": len(result.trades),
                    "skipped_count": len(result.skipped_signals),
                }
            )
        self.logger.log_backtest(result.trades, result.skipped_signals, backtest_run_id=backtest_run_id)
        return {
            "summary": result.summary,
            "trades": [trade.to_dict() for trade in result.trades],
            "skipped_signals": [signal.to_dict() for signal in result.skipped_signals],
        }

    @staticmethod
    def _backtest_load_start_date(start_date: str | None) -> str | None:
        if not start_date:
            return None
        warmup_days = max(int(getattr(config, "smart_trade_zone_history_days", 0) or 0) + 10, 45)
        return (pd.to_datetime(start_date).date() - timedelta(days=warmup_days)).isoformat()

    def _live_option_snapshot_for_backtest(
        self,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any] | None:
        if not config.option_selection_enabled:
            return None
        if progress_callback:
            progress_callback({"percent": 0, "current_step": "Loading live option chain for strike metadata"})
        try:
            return self.loader.fetch_fyers_option_snapshot("NSE:NIFTY50-INDEX", config.option_selection_strikecount)
        except Exception:
            return None
