# Nifty 50 Price Action Paper Trading AI - V1 Status

## Completed

- Python/FastAPI project scaffold with login, signup, dashboard, and role-based admin portal.
- Separate user and admin login flows:
  - User login: `/user/login` (admin can also use this to inspect paper trades as a user)
  - Admin login: `/admin/login`
- FinalPT-style panel UI has been ported:
  - Landing page from `D:\Desktop\FinalPT` is now served at `/`.
  - User console sidebar/dashboard structure from `D:\Desktop\FinalPT`.
  - Admin console sidebar/dashboard structure from `D:\Desktop\FinalPT`.
  - Shared panel CSS copied into `static/finalpt`.
  - Local panel navigation JS added at `static/finalpt/panel.js`.
  - Landing auth JS copied into `static/finalpt/site.js`.
- Admin Fyers token management that reads app credentials from `.env` and persists only token data to `data/fyers_auth.json`.
- Full admin Fyers OAuth flow:
  - Open the Fyers login/auth URL from the admin portal.
  - Exchange returned auth code for access token.
  - Auto-save token through `/admin/fyers/callback` when used as the Fyers redirect URI.
  - Admin UI shows only Open Login URL, Exchange Auth Code, and token status.
- Admin-only Fyers market-data controls:
  - FYERS OAuth/token management remains in the admin portal.
  - Quote, historical, and socket functions are backend-only now and are not exposed as frontend controls.
  - FYERS socket is restricted to NSE market hours only: 09:15 to 15:30 IST on weekdays.
- MySQL candle storage:
  - `nifty_index_candles_1m`
  - `nifty_index_candles_5m`
  - `nifty_index_candles_15m`
- Database backtest flow:
  - User console backtest now runs from MySQL candles instead of CSV upload.
  - The old CSV upload UI has been removed.
- Paper-trading-only architecture. There are no live broker order placement routes or methods.
- CSV 1m candle upload and backtest runner.
- Modular rule engines:
  - `DataLoader` for CSV validation, 1m to 5m/15m resampling, Fyers auth, and quote fetch.
  - `LevelEngine` for PDH, PDL, PDC, ORH, ORL, swings, day high/low, and round levels.
  - `StructureEngine` for confirmed swings, trend, BOS, and 1m confirmation.
  - `DisplacementEngine` for bullish/bearish displacement rules.
  - `LiquidityEngine` for sweeps of ORH, ORL, PDH, PDL, and swing levels.
  - `OrderBlockEngine` for last opposite candle order-block zones and retests.
  - `SignalEngine` for OR breakout, liquidity sweep reversal, and OB retest continuation candidates.
  - `RiskEngine` for candle/structure-based SL, liquidity target, minimum 1:2 RR, and setup score.
  - `PaperTradeEngine` for trade creation, 1m candle simulation exits, quote-based updates, MFE, MAE, and R multiples.
  - `TradeLogger` for paper trades, skipped signals, and ML-ready feature persistence in SQLite.
  - `BacktestRunner` for multi-day simulation and summary stats.
- Aiven Cloud MySQL persistence through `PyMySQL`.
- ML feature export and initial XGBoost trainer skeleton for later model training.
- Fyers market-data integration adapter:
  - `FyersMarketDataSocket` for live tick subscription.
  - `FyersQuotePoller` for 3-second quote polling used by paper trade monitoring.

## Implemented Setup Coverage

- Opening Range breakout/breakdown continuation with displacement, BOS, 1m confirmation, candle SL, liquidity target, RR validation, and scoring.
- Liquidity sweep reversal with sweep detection, displacement within the next 1 to 5 candles, BOS, optional OB context, candle SL, liquidity target, RR validation, and scoring.
- Order-block retest continuation after displacement and BOS.

## Database Configuration

The app uses Aiven Cloud MySQL through one connection string. Set `MYSQL_URI` before starting the server:

```text
MYSQL_URI=mysql://avnadmin:your-password@your-aiven-mysql-host.aivencloud.com:12345/price_action_ai?ssl-mode=REQUIRED
SESSION_SECRET=change-this-long-random-secret
FYERS_CLIENT_ID=your-fyers-client-id
FYERS_SECRET_KEY=your-fyers-secret-key
FYERS_REDIRECT_URI=http://127.0.0.1:8000/admin/fyers/callback
FYERS_USER_ID=your-fyers-login-id
FYERS_PIN=your-fyers-pin
FYERS_TOTP_KEY=your-fyers-external-2fa-totp-secret
# Optional, defaults to 2 for web login:
FYERS_LOGIN_APP_ID=2
# Optional daily automatic token refresh time in IST:
FYERS_TOTP_REFRESH_HOUR=8
FYERS_TOTP_REFRESH_MINUTE=0
```

If you want to validate with Aiven's CA certificate, include the CA path in the URI:

```text
MYSQL_URI=mysql://avnadmin:your-password@your-aiven-mysql-host.aivencloud.com:12345/price_action_ai?ssl_ca=C:\path\to\aiven-ca.pem
```

The database schema is created automatically on startup. The app creates the default admin user if it does not already exist.

## Partially Completed / Needs More Work

- Late reversal after liquidity target hit is not fully implemented yet.
- CHoCH is not separately exposed yet; BOS is implemented.
- Choppy market detection, repeated false-break penalties, and failed-level counters need stronger stateful rules.
- 15m higher timeframe bias is scaffolded through data validation/resampling but not yet used as a hard setup filter.
- Fyers socket is added as a market-data adapter, but live candle building from ticks still needs production hardening.
- Option instrument selection for ATM or 1-strike ITM CE/PE is not implemented because V1 paper trades are tracked on Nifty index price only.

## How To Run

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

For faster local development, watch only source/UI folders:

```bash
python -m uvicorn app.main:app --reload --reload-dir app --reload-dir templates --reload-dir static --host 127.0.0.1 --port 8000
```

For fastest startup when you are not editing code, run without reload:

```bash
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000`.

Default admin login:

```text
url: /admin/login
username: admin
password: admin123
```

User login:

```text
url: /user/login
```

## CSV Input Format

Backtests now read candles from MySQL. Use the one-time FYERS backfill script first:

```bash
python -m app.scripts.backfill_nifty_history --days 60
```

This fetches Nifty index 1m candles from FYERS, stores them in MySQL, and writes resampled 5m and 15m candles.

## Safety Rule

This V1 is paper-trading-only. It must not place real broker orders. Future live execution should be built as a separate stage with explicit risk controls, approvals, and broker-side safeguards.
