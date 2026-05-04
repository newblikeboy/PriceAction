from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

import pandas as pd

from app.data_loader import DataLoader
from app.storage.database import Database


FYERS_NIFTY_INDEX = "NSE:NIFTY50-INDEX"
DB_SYMBOL = "NIFTY"


def date_chunks(start: date, end: date, chunk_days: int = 30):
    current = start
    while current <= end:
        chunk_end = min(end, current + timedelta(days=chunk_days - 1))
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def normalize_history(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    out["datetime"] = pd.to_datetime(out["datetime"])
    out = out.sort_values("datetime").drop_duplicates("datetime")
    out = out[["datetime", "open", "high", "low", "close", "volume"]]
    for column in ["open", "high", "low", "close", "volume"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out.dropna(subset=["open", "high", "low", "close"])


def run_backfill(days: int = 60, symbol: str = FYERS_NIFTY_INDEX, db_symbol: str = DB_SYMBOL) -> dict:
    loader = DataLoader()
    database = Database()
    end = datetime.now().date()
    start = end - timedelta(days=days)

    parts: list[pd.DataFrame] = []
    for chunk_start, chunk_end in date_chunks(start, end):
        frame = loader.fetch_fyers_history(
            symbol=symbol,
            resolution="1",
            range_from=chunk_start.isoformat(),
            range_to=chunk_end.isoformat(),
        )
        parts.append(frame)
        print(f"Fetched {len(frame)} 1m rows from {chunk_start} to {chunk_end}")

    non_empty_parts = [part for part in parts if not part.empty]
    candles_1m = normalize_history(pd.concat(non_empty_parts, ignore_index=True) if non_empty_parts else pd.DataFrame())
    if candles_1m.empty:
        raise RuntimeError("FYERS returned no Nifty 1m candles")

    validated_1m = loader.validate_candles(candles_1m, "1m")
    bundle = loader.resample_from_1m(validated_1m)

    rows_1m = database.upsert_candles("1m", db_symbol, candles_1m)
    rows_5m = database.upsert_candles("5m", db_symbol, bundle.candles_5m)
    rows_15m = database.upsert_candles("15m", db_symbol, bundle.candles_15m)
    counts = database.candle_counts(db_symbol)
    return {
        "symbol": db_symbol,
        "source_symbol": symbol,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "source_1m_rows": len(candles_1m),
        "upserted": {"1m": rows_1m, "5m": rows_5m, "15m": rows_15m},
        "database_counts": counts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Nifty index candles from FYERS into MySQL.")
    parser.add_argument("--days", type=int, default=60, help="Calendar days to backfill. Default: 60.")
    parser.add_argument("--symbol", default=FYERS_NIFTY_INDEX, help="FYERS symbol. Default: NSE:NIFTY50-INDEX.")
    parser.add_argument("--db-symbol", default=DB_SYMBOL, help="Database symbol. Default: NIFTY.")
    args = parser.parse_args()
    result = run_backfill(days=args.days, symbol=args.symbol, db_symbol=args.db_symbol)
    print("Backfill complete")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
