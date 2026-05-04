from __future__ import annotations

import json
import secrets
from datetime import datetime
from hashlib import pbkdf2_hmac
from typing import Any

import pandas as pd
import pymysql
from pymysql.connections import Connection
from pymysql.cursors import DictCursor

from app.config import MySQLConfig, mysql_config


BACKTEST_RUNS_TABLE = "price_action_backtest_runs"


class DatabaseConfigurationError(RuntimeError):
    pass


class Database:
    def __init__(self, cfg: MySQLConfig = mysql_config) -> None:
        self.cfg = cfg
        if not self.cfg.is_configured:
            raise DatabaseConfigurationError(
                "MySQL is not configured. Set MYSQL_URI in the environment or .env file."
            )
        try:
            self.cfg.validate()
        except ValueError as exc:
            raise DatabaseConfigurationError(str(exc)) from exc
        self.init()

    def connect(self) -> Connection:
        ssl = None
        if self.cfg.ssl_ca_path:
            ssl = {"ca": self.cfg.ssl_ca_path}
        elif self.cfg.ssl_required:
            ssl = {}
        return pymysql.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            user=self.cfg.user,
            password=self.cfg.password,
            database=self.cfg.database,
            cursorclass=DictCursor,
            autocommit=True,
            connect_timeout=self.cfg.connect_timeout,
            ssl=ssl,
        )

    def init(self) -> None:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                        username VARCHAR(191) UNIQUE NOT NULL,
                        password_hash VARCHAR(255) NOT NULL,
                        role ENUM('user', 'admin') NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS paper_trades (
                        id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                        date DATE NOT NULL,
                        symbol VARCHAR(32) NOT NULL,
                        direction ENUM('CE', 'PE') NOT NULL,
                        setup_type VARCHAR(96) NOT NULL,
                        entry_time VARCHAR(8) NOT NULL,
                        entry_index_price DECIMAL(12, 2) NOT NULL,
                        sl_index_price DECIMAL(12, 2) NOT NULL,
                        target_index_price DECIMAL(12, 2) NOT NULL,
                        risk_points DECIMAL(12, 2) NOT NULL,
                        reward_points DECIMAL(12, 2) NOT NULL,
                        risk_reward DECIMAL(8, 3) NOT NULL,
                        setup_score INT NOT NULL,
                        status ENUM('OPEN', 'CLOSED') NOT NULL,
                        exit_time VARCHAR(8),
                        exit_index_price DECIMAL(12, 2),
                        exit_reason VARCHAR(64),
                        result VARCHAR(16),
                        r_multiple DECIMAL(10, 4),
                        max_favorable_excursion DECIMAL(12, 2),
                        max_adverse_excursion DECIMAL(12, 2),
                        notes_json JSON NOT NULL,
                        features_json JSON NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_trade_date (date),
                        INDEX idx_trade_setup (setup_type),
                        INDEX idx_trade_direction (direction)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                self._ensure_paper_trade_option_columns(cursor)
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS skipped_signals (
                        id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                        date DATE NOT NULL,
                        time VARCHAR(8) NOT NULL,
                        potential_direction ENUM('CE', 'PE') NOT NULL,
                        potential_setup VARCHAR(96) NOT NULL,
                        skip_reason VARCHAR(255) NOT NULL,
                        context_json JSON NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_skip_date (date),
                        INDEX idx_skip_reason (skip_reason)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ml_features (
                        id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                        trade_id BIGINT UNSIGNED,
                        features_json JSON NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT fk_ml_trade FOREIGN KEY (trade_id) REFERENCES paper_trades(id)
                            ON DELETE SET NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS price_action_backtest_runs (
                        id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                        symbol VARCHAR(64) NOT NULL,
                        start_date DATE,
                        end_date DATE,
                        status ENUM('RUNNING', 'COMPLETED', 'FAILED') NOT NULL,
                        progress_pct DECIMAL(5, 2) NOT NULL DEFAULT 0,
                        current_step VARCHAR(255),
                        trades_count INT NOT NULL DEFAULT 0,
                        skipped_count INT NOT NULL DEFAULT 0,
                        summary_json JSON,
                        error_message TEXT,
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP NULL,
                        INDEX idx_backtest_started (started_at),
                        INDEX idx_backtest_status (status)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                for timeframe in ("1m", "5m", "15m"):
                    table = self.candle_table(timeframe)
                    cursor.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {table} (
                            id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                            symbol VARCHAR(64) NOT NULL,
                            candle_time DATETIME NOT NULL,
                            open DECIMAL(12, 2) NOT NULL,
                            high DECIMAL(12, 2) NOT NULL,
                            low DECIMAL(12, 2) NOT NULL,
                            close DECIMAL(12, 2) NOT NULL,
                            volume BIGINT NOT NULL DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            UNIQUE KEY uq_{table}_symbol_time (symbol, candle_time),
                            INDEX idx_{table}_time (candle_time)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                        """
                    )
        if not self.get_user("admin"):
            self.create_user("admin", "admin123", "admin")

    def _ensure_paper_trade_option_columns(self, cursor: Any) -> None:
        cursor.execute("SHOW COLUMNS FROM paper_trades")
        existing = {str(row.get("Field") or "") for row in cursor.fetchall()}
        columns = {
            "option_symbol": "VARCHAR(128)",
            "option_side": "VARCHAR(2)",
            "option_strike": "DECIMAL(12, 2)",
            "option_entry_ltp": "DECIMAL(12, 2)",
            "option_mark_ltp": "DECIMAL(12, 2)",
            "option_exit_ltp": "DECIMAL(12, 2)",
            "option_points": "DECIMAL(12, 2)",
            "pnl_source": "VARCHAR(32)",
            "underlying_entry_price": "DECIMAL(12, 2)",
            "underlying_exit_price": "DECIMAL(12, 2)",
            "underlying_points": "DECIMAL(12, 2)",
        }
        for name, definition in columns.items():
            if name not in existing:
                cursor.execute(f"ALTER TABLE paper_trades ADD COLUMN {name} {definition}")

    @staticmethod
    def candle_table(timeframe: str) -> str:
        mapping = {
            "1m": "nifty_index_candles_1m",
            "5m": "nifty_index_candles_5m",
            "15m": "nifty_index_candles_15m",
        }
        if timeframe not in mapping:
            raise ValueError("timeframe must be one of: 1m, 5m, 15m")
        return mapping[timeframe]

    def create_user(self, username: str, password: str, role: str = "user") -> None:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
                    (username, self.hash_password(password), role),
                )

    def get_user(self, username: str) -> dict[str, Any] | None:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
                return cursor.fetchone()

    def verify_user(self, username: str, password: str) -> dict[str, Any] | None:
        user = self.get_user(username)
        if user and self.verify_password(password, user["password_hash"]):
            return user
        return None

    def insert_trade(self, trade: dict[str, Any]) -> int:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO paper_trades (
                        date, symbol, direction, setup_type, entry_time, entry_index_price,
                        sl_index_price, target_index_price, risk_points, reward_points,
                        risk_reward, setup_score, status, exit_time, exit_index_price,
                        exit_reason, result, r_multiple, max_favorable_excursion,
                        max_adverse_excursion, option_symbol, option_side, option_strike,
                        option_entry_ltp, option_mark_ltp, option_exit_ltp, option_points,
                        pnl_source, underlying_entry_price, underlying_exit_price,
                        underlying_points, notes_json, features_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        trade["date"],
                        trade["symbol"],
                        trade["direction"],
                        trade["setup_type"],
                        trade["entry_time"],
                        trade["entry_index_price"],
                        trade["sl_index_price"],
                        trade["target_index_price"],
                        trade["risk_points"],
                        trade["reward_points"],
                        trade["risk_reward"],
                        trade["setup_score"],
                        trade["status"],
                        trade["exit_time"],
                        trade["exit_index_price"],
                        trade["exit_reason"],
                        trade["result"],
                        trade["r_multiple"],
                        trade["max_favorable_excursion"],
                        trade["max_adverse_excursion"],
                        trade.get("option_symbol"),
                        trade.get("option_side"),
                        trade.get("option_strike"),
                        trade.get("option_entry_ltp"),
                        trade.get("option_mark_ltp"),
                        trade.get("option_exit_ltp"),
                        trade.get("option_points"),
                        trade.get("pnl_source"),
                        trade.get("underlying_entry_price") or trade.get("entry_index_price"),
                        trade.get("underlying_exit_price"),
                        trade.get("underlying_points"),
                        json.dumps(trade["notes"]),
                        json.dumps(trade["features"], default=str),
                    ),
                )
                trade_id = int(cursor.lastrowid)
                cursor.execute(
                    "INSERT INTO ml_features (trade_id, features_json) VALUES (%s, %s)",
                    (trade_id, json.dumps(trade["features"], default=str)),
                )
                return trade_id

    def insert_skipped(self, skipped: dict[str, Any]) -> int:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO skipped_signals (date, time, potential_direction, potential_setup, skip_reason, context_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        skipped["date"],
                        skipped["time"],
                        skipped["potential_direction"],
                        skipped["potential_setup"],
                        skipped["skip_reason"],
                        json.dumps(skipped["context"], default=str),
                    ),
            )
            return int(cursor.lastrowid)

    def trade_exists(self, trade: dict[str, Any]) -> bool:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id
                    FROM paper_trades
                    WHERE date = %s
                      AND symbol = %s
                      AND direction = %s
                      AND setup_type = %s
                      AND entry_time = %s
                    LIMIT 1
                    """,
                    (
                        trade["date"],
                        trade["symbol"],
                        trade["direction"],
                        trade["setup_type"],
                        trade["entry_time"],
                    ),
                )
                return cursor.fetchone() is not None

    def insert_trade_if_absent(self, trade: dict[str, Any]) -> int | None:
        if self.trade_exists(trade):
            return None
        return self.insert_trade(trade)

    def list_open_trades(self, symbol: str = "NIFTY", limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM paper_trades
                    WHERE symbol = %s AND status = 'OPEN'
                    ORDER BY date ASC, entry_time ASC, id ASC
                    LIMIT %s
                    """,
                    (symbol, limit),
                )
                return list(cursor.fetchall())

    def update_trade_option_mark(
        self,
        trade_id: int,
        *,
        option_symbol: str,
        option_mark_ltp: float | None = None,
        underlying_mark_price: float | None = None,
        underlying_points: float | None = None,
        option_quote_time: str | None = None,
    ) -> None:
        mark_ltp = round(float(option_mark_ltp), 2) if option_mark_ltp is not None else None
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE paper_trades
                    SET option_symbol = COALESCE(NULLIF(%s, ''), option_symbol),
                        option_mark_ltp = COALESCE(%s, option_mark_ltp),
                        option_points = CASE
                            WHEN %s IS NOT NULL AND option_entry_ltp IS NOT NULL AND option_entry_ltp > 0
                                THEN ROUND(%s - option_entry_ltp, 2)
                            ELSE option_points
                        END,
                        pnl_source = CASE
                            WHEN %s IS NOT NULL AND option_entry_ltp IS NOT NULL AND option_entry_ltp > 0 THEN 'option_quote'
                            WHEN %s IS NOT NULL THEN 'underlying_live'
                            ELSE pnl_source
                        END,
                        underlying_exit_price = COALESCE(%s, underlying_exit_price),
                        underlying_points = COALESCE(%s, underlying_points)
                    WHERE id = %s AND status = 'OPEN'
                    """,
                    (
                        option_symbol,
                        mark_ltp,
                        mark_ltp,
                        mark_ltp,
                        mark_ltp,
                        underlying_points,
                        round(float(underlying_mark_price), 2) if underlying_mark_price is not None else None,
                        round(float(underlying_points), 2) if underlying_points is not None else None,
                        int(trade_id),
                    ),
                )

    def close_trade(self, trade_id: int, updates: dict[str, Any]) -> None:
        features = updates.get("features")
        notes = updates.get("notes")
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE paper_trades
                    SET status = 'CLOSED',
                        exit_time = %s,
                        exit_index_price = %s,
                        exit_reason = %s,
                        result = %s,
                        r_multiple = %s,
                        max_favorable_excursion = %s,
                        max_adverse_excursion = %s,
                        option_symbol = COALESCE(NULLIF(%s, ''), option_symbol),
                        option_mark_ltp = %s,
                        option_exit_ltp = %s,
                        option_points = %s,
                        pnl_source = %s,
                        underlying_entry_price = %s,
                        underlying_exit_price = %s,
                        underlying_points = %s,
                        notes_json = COALESCE(%s, notes_json),
                        features_json = COALESCE(%s, features_json)
                    WHERE id = %s
                    """,
                    (
                        updates.get("exit_time"),
                        updates.get("exit_index_price"),
                        updates.get("exit_reason"),
                        updates.get("result"),
                        updates.get("r_multiple"),
                        updates.get("max_favorable_excursion"),
                        updates.get("max_adverse_excursion"),
                        updates.get("option_symbol"),
                        updates.get("option_mark_ltp"),
                        updates.get("option_exit_ltp"),
                        updates.get("option_points"),
                        updates.get("pnl_source"),
                        updates.get("underlying_entry_price"),
                        updates.get("underlying_exit_price"),
                        updates.get("underlying_points"),
                        json.dumps(notes, default=str) if notes is not None else None,
                        json.dumps(features, default=str) if features is not None else None,
                        int(trade_id),
                    ),
                )

    def skipped_exists(self, skipped: dict[str, Any]) -> bool:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id
                    FROM skipped_signals
                    WHERE date = %s
                      AND time = %s
                      AND potential_direction = %s
                      AND potential_setup = %s
                      AND skip_reason = %s
                    LIMIT 1
                    """,
                    (
                        skipped["date"],
                        skipped["time"],
                        skipped["potential_direction"],
                        skipped["potential_setup"],
                        skipped["skip_reason"],
                    ),
                )
                return cursor.fetchone() is not None

    def insert_skipped_if_absent(self, skipped: dict[str, Any]) -> int | None:
        if self.skipped_exists(skipped):
            return None
        return self.insert_skipped(skipped)

    def insert_backtest_logs(self, trades: list[dict[str, Any]], skipped_signals: list[dict[str, Any]]) -> None:
        with self.connect() as db:
            with db.cursor() as cursor:
                if trades:
                    for trade in trades:
                        cursor.execute(
                            """
                            INSERT INTO paper_trades (
                                date, symbol, direction, setup_type, entry_time, entry_index_price,
                                sl_index_price, target_index_price, risk_points, reward_points,
                                risk_reward, setup_score, status, exit_time, exit_index_price,
                                exit_reason, result, r_multiple, max_favorable_excursion,
                                max_adverse_excursion, notes_json, features_json
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                trade["date"],
                                trade["symbol"],
                                trade["direction"],
                                trade["setup_type"],
                                trade["entry_time"],
                                trade["entry_index_price"],
                                trade["sl_index_price"],
                                trade["target_index_price"],
                                trade["risk_points"],
                                trade["reward_points"],
                                trade["risk_reward"],
                                trade["setup_score"],
                                trade["status"],
                                trade["exit_time"],
                                trade["exit_index_price"],
                                trade["exit_reason"],
                                trade["result"],
                                trade["r_multiple"],
                                trade["max_favorable_excursion"],
                                trade["max_adverse_excursion"],
                                json.dumps(trade["notes"]),
                                json.dumps(trade["features"], default=str),
                            ),
                        )
                        trade_id = int(cursor.lastrowid)
                        cursor.execute(
                            "INSERT INTO ml_features (trade_id, features_json) VALUES (%s, %s)",
                            (trade_id, json.dumps(trade["features"], default=str)),
                        )
                if skipped_signals:
                    cursor.executemany(
                        """
                        INSERT INTO skipped_signals (date, time, potential_direction, potential_setup, skip_reason, context_json)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                skipped["date"],
                                skipped["time"],
                                skipped["potential_direction"],
                                skipped["potential_setup"],
                                skipped["skip_reason"],
                                json.dumps(skipped["context"], default=str),
                            )
                            for skipped in skipped_signals
                        ],
                    )

    def create_backtest_run(self, symbol: str, start_date: str | None, end_date: str | None) -> int:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {BACKTEST_RUNS_TABLE} (symbol, start_date, end_date, status, progress_pct, current_step)
                    VALUES (%s, %s, %s, 'RUNNING', 0, 'Queued')
                    """,
                    (symbol, start_date or None, end_date or None),
                )
                return int(cursor.lastrowid)

    def update_backtest_run(
        self,
        run_id: int,
        *,
        status: str | None = None,
        progress_pct: float | None = None,
        current_step: str | None = None,
        summary: dict[str, Any] | None = None,
        trades_count: int | None = None,
        skipped_count: int | None = None,
        error_message: str | None = None,
        completed: bool = False,
    ) -> None:
        assignments: list[str] = []
        params: list[Any] = []
        if status is not None:
            assignments.append("status = %s")
            params.append(status)
        if progress_pct is not None:
            assignments.append("progress_pct = %s")
            params.append(round(float(progress_pct), 2))
        if current_step is not None:
            assignments.append("current_step = %s")
            params.append(current_step)
        if summary is not None:
            assignments.append("summary_json = %s")
            params.append(json.dumps(summary, default=str))
        if trades_count is not None:
            assignments.append("trades_count = %s")
            params.append(int(trades_count))
        if skipped_count is not None:
            assignments.append("skipped_count = %s")
            params.append(int(skipped_count))
        if error_message is not None:
            assignments.append("error_message = %s")
            params.append(error_message)
        if completed:
            assignments.append("completed_at = CURRENT_TIMESTAMP")
        if not assignments:
            return
        params.append(run_id)
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(f"UPDATE {BACKTEST_RUNS_TABLE} SET {', '.join(assignments)} WHERE id = %s", tuple(params))

    def latest_backtest_run(self) -> dict[str, Any] | None:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {BACKTEST_RUNS_TABLE} ORDER BY id DESC LIMIT 1")
                row = cursor.fetchone()
        if not row:
            return None
        summary = row.get("summary_json")
        if isinstance(summary, str):
            try:
                row["summary"] = json.loads(summary)
            except json.JSONDecodeError:
                row["summary"] = {}
        else:
            row["summary"] = summary or {}
        row.pop("summary_json", None)
        return row

    def list_trades(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute("SELECT * FROM paper_trades ORDER BY id DESC LIMIT %s", (limit,))
                return list(cursor.fetchall())

    def list_trades_between(self, start_date: str, end_date: str, symbol: str = "NIFTY", limit: int = 1000) -> list[dict[str, Any]]:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM paper_trades
                    WHERE symbol = %s AND date BETWEEN %s AND %s
                    ORDER BY date ASC, entry_time ASC
                    LIMIT %s
                    """,
                    (symbol, start_date, end_date, limit),
                )
                return list(cursor.fetchall())

    def list_skipped(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute("SELECT * FROM skipped_signals ORDER BY id DESC LIMIT %s", (limit,))
                return list(cursor.fetchall())

    def list_skipped_between(self, start_date: str, end_date: str, limit: int = 300) -> list[dict[str, Any]]:
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM skipped_signals
                    WHERE date BETWEEN %s AND %s
                    ORDER BY date DESC, time DESC
                    LIMIT %s
                    """,
                    (start_date, end_date, limit),
                )
                return list(cursor.fetchall())

    def upsert_candles(self, timeframe: str, symbol: str, candles: pd.DataFrame) -> int:
        if candles.empty:
            return 0
        table = self.candle_table(timeframe)
        frame = candles.copy()
        if "datetime" not in frame.columns:
            frame = frame.reset_index().rename(columns={"index": "datetime"})
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        if "volume" not in frame.columns:
            frame["volume"] = 0
        rows = [
            (
                symbol,
                row.datetime.to_pydatetime(),
                float(row.open),
                float(row.high),
                float(row.low),
                float(row.close),
                int(float(row.volume or 0)),
            )
            for row in frame[["datetime", "open", "high", "low", "close", "volume"]].itertuples(index=False)
        ]
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.executemany(
                    f"""
                    INSERT INTO {table} (symbol, candle_time, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume)
                    """,
                    rows,
                )
                return int(cursor.rowcount or 0)

    def upsert_candle(
        self,
        timeframe: str,
        symbol: str,
        candle_time: datetime,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: int = 0,
    ) -> int:
        table = self.candle_table(timeframe)
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {table} (symbol, candle_time, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume)
                    """,
                    (
                        symbol,
                        candle_time,
                        float(open_price),
                        float(high_price),
                        float(low_price),
                        float(close_price),
                        int(volume),
                    ),
                )
                return int(cursor.rowcount or 0)

    def load_candles(
        self,
        timeframe: str,
        symbol: str = "NIFTY",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        table = self.candle_table(timeframe)
        where = ["symbol = %s"]
        params: list[Any] = [symbol]
        if start_date:
            where.append("candle_time >= %s")
            params.append(f"{start_date} 00:00:00")
        if end_date:
            where.append("candle_time <= %s")
            params.append(f"{end_date} 23:59:59")
        sql = f"""
            SELECT candle_time AS datetime, open, high, low, close, volume
            FROM {table}
            WHERE {" AND ".join(where)}
            ORDER BY candle_time ASC
        """
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        frame = frame.set_index("datetime")
        for column in ["open", "high", "low", "close", "volume"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["date"] = frame.index.date
        frame["time"] = frame.index.strftime("%H:%M")
        return frame

    def load_chart_candles(
        self,
        timeframe: str,
        symbol: str = "NIFTY",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        table = self.candle_table(timeframe)
        where = ["symbol = %s"]
        params: list[Any] = [symbol]
        if start_date:
            where.append("candle_time >= %s")
            params.append(f"{start_date} 00:00:00")
        if end_date:
            where.append("candle_time <= %s")
            params.append(f"{end_date} 23:59:59")
        sql = f"""
            SELECT candle_time AS datetime, open, high, low, close
            FROM {table}
            WHERE {" AND ".join(where)}
            ORDER BY candle_time ASC
        """
        with self.connect() as db:
            with db.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return list(cursor.fetchall())

    def candle_counts(self, symbol: str = "NIFTY") -> dict[str, int]:
        out: dict[str, int] = {}
        with self.connect() as db:
            with db.cursor() as cursor:
                for timeframe in ("1m", "5m", "15m"):
                    table = self.candle_table(timeframe)
                    cursor.execute(f"SELECT COUNT(*) AS count FROM {table} WHERE symbol = %s", (symbol,))
                    row = cursor.fetchone() or {}
                    out[timeframe] = int(row.get("count") or 0)
        return out

    @staticmethod
    def hash_password(password: str) -> str:
        salt = secrets.token_hex(16)
        digest = pbkdf2_hmac("sha256", password.encode(), salt.encode(), 150_000).hex()
        return f"{salt}${digest}"

    @staticmethod
    def verify_password(password: str, stored: str) -> bool:
        salt, digest = stored.split("$", 1)
        check = pbkdf2_hmac("sha256", password.encode(), salt.encode(), 150_000).hex()
        return secrets.compare_digest(check, digest)
