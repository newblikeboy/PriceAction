# Nifty Paper Trading AI

Rule-based, paper-trading-only Nifty 50 price-action engine with FastAPI UI, admin Fyers token storage, backtesting, skipped-signal logging, and ML-ready feature persistence in Aiven Cloud MySQL.

Database configuration is intentionally one variable:

```text
MYSQL_URI=mysql://avnadmin:your-password@your-aiven-mysql-host.aivencloud.com:12345/price_action_ai?ssl-mode=REQUIRED
```

See `read.md` for completion status and run instructions.
