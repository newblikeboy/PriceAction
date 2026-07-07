from __future__ import annotations

import base64
import json
import os
import re
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyotp
import requests

from app.storage.database import Database


class AngelExecutionError(RuntimeError):
    pass


class AngelExecutionManager:
    LOGIN_URL = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"
    SEARCH_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/searchScrip"
    ORDER_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/placeOrder"
    INSTRUMENT_MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    IST = ZoneInfo("Asia/Kolkata")

    def __init__(self, database: Database) -> None:
        self.db = database
        self.timeout = float(os.getenv("ANGEL_REQUEST_TIMEOUT_SECONDS", "20"))
        self.execution_enabled = os.getenv("ANGEL_LIVE_EXECUTION_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
        self.default_lot_size_env = max(1, int(os.getenv("ANGEL_LOT_SIZE_QTY", "75")))
        self.product_type = os.getenv("ANGEL_PRODUCT_TYPE", "INTRADAY").strip().upper() or "INTRADAY"
        self.variety = os.getenv("ANGEL_ORDER_VARIETY", "NORMAL").strip().upper() or "NORMAL"
        self.duration = os.getenv("ANGEL_ORDER_DURATION", "DAY").strip().upper() or "DAY"
        self.order_type = os.getenv("ANGEL_ORDER_TYPE", "MARKET").strip().upper() or "MARKET"
        cache_default = Path(__file__).resolve().parents[1] / "data" / "angel_futures_contracts.json"
        self.future_contract_cache_path = Path(os.getenv("ANGEL_FUTURE_CONTRACT_CACHE", str(cache_default)))
        self.future_contract_cache_seconds = max(
            3600,
            int(os.getenv("ANGEL_FUTURE_CONTRACT_CACHE_SECONDS", "43200")),
        )
        self._future_contract_lock = threading.Lock()
        self._future_refresh_lock = threading.Lock()
        self._future_refresh_running = False
        self._future_refresh_done = threading.Event()
        self._future_refresh_done.set()
        self._future_contracts: list[dict[str, Any]] = []
        self._future_contracts_loaded_at = 0.0
        self._load_future_contract_cache()

    def status(self, username: str) -> dict[str, Any]:
        config = self.db.broker_config(username)
        lot_size = self.default_lot_size()
        if config.get("execution_instrument") == "FUTURE":
            with self._future_contract_lock:
                contract = self._pick_future_contract(
                    self._future_contracts,
                    datetime.now(self.IST).date(),
                )
            if contract:
                lot_size = int(contract["lot_size"])
                config["execution_symbol"] = contract["symbol"]
                config["execution_expiry"] = contract["expiry"]
        config["server_execution_enabled"] = self.execution_enabled
        config["default_lot_size"] = lot_size
        config["order_quantity"] = lot_size * max(1, int(config.get("lot_count") or 1))
        return config

    def default_lot_size(self) -> int:
        return self.db.execution_lot_size(self.default_lot_size_env)

    def warm_future_contracts(self) -> None:
        if not self.execution_enabled:
            return
        today = datetime.now(self.IST).date()
        with self._future_contract_lock:
            selected = self._pick_future_contract(self._future_contracts, today)
            cache_age = time.time() - self._future_contracts_loaded_at
        if not selected or cache_age > self.future_contract_cache_seconds:
            self._refresh_future_contracts_async()

    def login_user(self, username: str) -> dict[str, Any]:
        session = self.db.get_user_angel_session(username)
        if not session:
            raise AngelExecutionError("User was not found")
        missing = [
            label
            for label, value in {
                "client_id": session.get("client_id"),
                "api_key": session.get("api_key"),
                "pin": session.get("pin"),
                "totp_secret": session.get("totp_secret"),
            }.items()
            if not str(value or "").strip()
        ]
        if missing:
            raise AngelExecutionError(f"Missing Angel One fields: {', '.join(missing)}")

        otp = pyotp.TOTP(str(session["totp_secret"]).replace(" ", "")).now()
        payload = {"clientcode": session["client_id"], "password": session["pin"], "totp": otp}
        response_payload, status_code, ok = self._post(
            self.LOGIN_URL,
            payload,
            self._headers(api_key=str(session["api_key"])),
        )
        self.db.save_angel_api_hit(
            username=username,
            action="login",
            request_payload={"clientcode": session["client_id"], "password": "***", "totp": "***"},
            response_payload=self._safe_response(response_payload),
            http_status=status_code,
            ok=ok,
            error_message=None if ok else self._message(response_payload),
        )
        if not ok:
            raise AngelExecutionError(self._message(response_payload) or "Angel One login failed")

        data = response_payload.get("data") if isinstance(response_payload, dict) else {}
        jwt_token = str((data or {}).get("jwtToken") or (data or {}).get("accessToken") or "").strip()
        if not jwt_token:
            raise AngelExecutionError("Angel One login did not return an access token")
        refresh_token = str((data or {}).get("refreshToken") or "").strip() or None
        feed_token = str((data or {}).get("feedToken") or "").strip() or None
        token_expiry = self._jwt_expiry(jwt_token)
        self.db.save_user_angel_session(
            username,
            access_token=jwt_token,
            feed_token=feed_token,
            refresh_token=refresh_token,
            token_expires_at=token_expiry,
        )
        return {"ok": True, "connected": True, "token_expires_at": token_expiry}

    def dispatch_entry(self, paper_trade_id: int, trade: dict[str, Any]) -> dict[str, Any]:
        if not self.execution_enabled:
            return {"ok": False, "skipped": True, "reason": "ANGEL_LIVE_EXECUTION_ENABLED is not true"}
        sessions = self.db.list_connected_angel_sessions()
        if not sessions:
            return {"ok": False, "skipped": True, "reason": "No connected Angel One user sessions"}

        existing_orders = {
            str(order.get("username")): order
            for order in self.db.list_angel_live_orders(paper_trade_id)
        }
        option_contract = self._selected_contract(trade)
        option_symbol = str(
            option_contract.get("angel_symbol")
            or option_contract.get("tradingsymbol")
            or option_contract.get("symbol")
            or trade.get("option_symbol")
            or ""
        ).strip()
        future_instrument: dict[str, Any] | None = None
        option_instrument: dict[str, Any] | None = None
        results = []
        for session in sessions:
            mode = self._execution_instrument(session.get("execution_instrument"))
            existing_order = existing_orders.get(str(session["username"]))
            if (
                existing_order
                and str(existing_order.get("status") or "").upper() in {"OPEN", "CLOSED"}
                and existing_order.get("entry_order_id")
            ):
                results.append(
                    {
                        "username": session["username"],
                        "ok": True,
                        "skipped": True,
                        "instrument": mode,
                        "order_id": existing_order["entry_order_id"],
                        "message": "Entry order already dispatched for this paper trade",
                    }
                )
                continue
            try:
                if mode == "FUTURE":
                    if future_instrument is None:
                        future_instrument = self._resolve_future_instrument()
                    resolved = future_instrument
                    side = self._future_entry_side(trade.get("direction"))
                    lot_size = int(resolved["lot_size"])
                else:
                    if not option_symbol:
                        raise AngelExecutionError("No option symbol available for live order")
                    if option_instrument is None:
                        option_instrument = self._resolve_option_instrument(option_symbol, option_contract, session)
                    resolved = option_instrument
                    side = "BUY"
                    lot_size = self._positive_int(option_contract.get("lot_size")) or self.default_lot_size()
                if not resolved.get("symbol") or not resolved.get("token"):
                    raise AngelExecutionError(f"Angel {mode.lower()} instrument token could not be resolved")
            except (AngelExecutionError, TypeError, ValueError) as exc:
                message = str(exc)
                self.db.save_angel_api_hit(
                    username=str(session["username"]),
                    paper_trade_id=paper_trade_id,
                    action="entry_resolve",
                    symbol=option_symbol or "NIFTY",
                    request_payload={"execution_instrument": mode, "direction": trade.get("direction")},
                    response_payload={},
                    ok=False,
                    error_message=message,
                )
                results.append(
                    {
                        "username": session["username"],
                        "ok": False,
                        "instrument": mode,
                        "message": message,
                    }
                )
                continue

            quantity = self._order_quantity(session, lot_size)
            payload = self._order_payload(
                side=side,
                symbol=str(resolved["symbol"]),
                token=str(resolved["token"]),
                exchange=str(resolved.get("exchange") or "NFO"),
                quantity=quantity,
            )
            response_payload, status_code, ok = self._post(
                self.ORDER_URL,
                payload,
                self._headers(api_key=str(session["api_key"]), access_token=str(session["access_token"])),
            )
            order_id = self._order_id(response_payload)
            placed_ok = ok and bool(order_id)
            self.db.save_angel_api_hit(
                username=str(session["username"]),
                paper_trade_id=paper_trade_id,
                action="entry",
                symbol=str(resolved["symbol"]),
                request_payload=payload,
                response_payload=self._safe_response(response_payload),
                http_status=status_code,
                ok=ok,
                error_message=None if ok else self._message(response_payload),
            )
            self.db.save_angel_live_entry(
                username=str(session["username"]),
                paper_trade_id=paper_trade_id,
                symbol=str(resolved["symbol"]),
                token=str(resolved["token"]),
                exchange=str(resolved.get("exchange") or "NFO"),
                quantity=quantity,
                entry_side=side,
                entry_order_id=order_id,
                response_payload=self._safe_response(response_payload),
                failed=not placed_ok,
            )
            results.append(
                {
                    "username": session["username"],
                    "ok": placed_ok,
                    "order_id": order_id,
                    "instrument": mode,
                    "symbol": resolved["symbol"],
                    "side": side,
                    "quantity": quantity,
                    "message": self._message(response_payload),
                }
            )
        return {"ok": any(item["ok"] for item in results), "results": results}

    def place_test_future_order(self, username: str, side: str = "BUY") -> dict[str, Any]:
        if not self.execution_enabled:
            raise AngelExecutionError("ANGEL_LIVE_EXECUTION_ENABLED is not true on the server")
        clean_side = str(side or "BUY").strip().upper()
        if clean_side not in {"BUY", "SELL"}:
            raise AngelExecutionError(f"Unsupported order side: {clean_side}")
        session = self.db.get_user_angel_session(username)
        if not session:
            raise AngelExecutionError("User was not found")
        if not str(session.get("api_key") or "").strip():
            raise AngelExecutionError("No Angel One API key saved for this user")
        if not str(session.get("access_token") or "").strip():
            raise AngelExecutionError("No Angel One access token. Run Terminal Login first.")
        instrument = self._resolve_future_instrument()
        quantity = self._order_quantity(session, int(instrument["lot_size"]))
        payload = self._order_payload(
            side=clean_side,
            symbol=str(instrument["symbol"]),
            token=str(instrument["token"]),
            exchange=str(instrument.get("exchange") or "NFO"),
            quantity=quantity,
        )
        response_payload, status_code, ok = self._post(
            self.ORDER_URL,
            payload,
            self._headers(api_key=str(session["api_key"]), access_token=str(session["access_token"])),
        )
        order_id = self._order_id(response_payload)
        placed_ok = ok and bool(order_id)
        self.db.save_angel_api_hit(
            username=username,
            action="future_test",
            symbol=str(instrument["symbol"]),
            request_payload=payload,
            response_payload=self._safe_response(response_payload),
            http_status=status_code,
            ok=ok,
            error_message=None if ok else self._message(response_payload),
        )
        return {
            "ok": placed_ok,
            "order_id": order_id,
            "instrument": instrument,
            "request": payload,
            "response": self._safe_response(response_payload),
            "http_status": status_code,
            "message": self._message(response_payload),
        }

    def dispatch_exit(self, paper_trade_id: int, close_reason: str) -> dict[str, Any]:
        if not self.execution_enabled:
            return {"ok": False, "skipped": True, "reason": "ANGEL_LIVE_EXECUTION_ENABLED is not true"}
        orders = self.db.list_open_angel_live_orders(paper_trade_id)
        if not orders:
            return {"ok": False, "skipped": True, "reason": "No open Angel live order for this paper trade"}
        results = []
        for order in orders:
            session = self.db.get_user_angel_session(str(order["username"]))
            if not session or not session.get("access_token") or not session.get("api_key"):
                results.append({"username": order["username"], "ok": False, "message": "Angel session missing"})
                continue
            entry_side = str(order.get("entry_side") or "").strip().upper()
            if entry_side not in {"BUY", "SELL"}:
                results.append({"username": order["username"], "ok": False, "message": "Invalid live entry side"})
                continue
            exit_side = "SELL" if entry_side == "BUY" else "BUY"
            payload = self._order_payload(
                side=exit_side,
                symbol=str(order["symbol"]),
                token=str(order["token"]),
                exchange=str(order["exchange"] or "NFO"),
                quantity=int(order["quantity"]),
            )
            response_payload, status_code, ok = self._post(
                self.ORDER_URL,
                payload,
                self._headers(api_key=str(session["api_key"]), access_token=str(session["access_token"])),
            )
            exit_order_id = self._order_id(response_payload)
            placed_ok = ok and bool(exit_order_id)
            self.db.save_angel_api_hit(
                username=str(order["username"]),
                paper_trade_id=paper_trade_id,
                action="exit",
                symbol=str(order["symbol"]),
                request_payload=payload,
                response_payload=self._safe_response(response_payload),
                http_status=status_code,
                ok=ok,
                error_message=None if ok else self._message(response_payload),
            )
            self.db.save_angel_live_exit(
                order_id=int(order["id"]),
                exit_order_id=exit_order_id,
                close_reason=close_reason,
                response_payload=self._safe_response(response_payload),
                failed=not placed_ok,
            )
            results.append({"username": order["username"], "ok": placed_ok, "order_id": exit_order_id, "message": self._message(response_payload)})
        return {"ok": any(item["ok"] for item in results), "results": results}

    def _resolve_future_instrument(self, on_date: date | None = None) -> dict[str, Any]:
        trade_date = on_date or datetime.now(self.IST).date()
        with self._future_contract_lock:
            selected = self._pick_future_contract(self._future_contracts, trade_date)
            cache_age = time.time() - self._future_contracts_loaded_at
        if selected:
            if cache_age > self.future_contract_cache_seconds:
                self._refresh_future_contracts_async()
            return selected

        with self._future_refresh_lock:
            refresh_running = self._future_refresh_running
        if refresh_running:
            self._future_refresh_done.wait(timeout=max(60.0, self.timeout))
            with self._future_contract_lock:
                selected = self._pick_future_contract(self._future_contracts, trade_date)
            if selected:
                return selected

        contracts = self._download_future_contracts()
        selected = self._pick_future_contract(contracts, trade_date)
        if not selected:
            raise AngelExecutionError("No unexpired NIFTY futures contract was found in Angel instrument master")
        return selected

    def _download_future_contracts(self) -> list[dict[str, Any]]:
        try:
            response = requests.get(self.INSTRUMENT_MASTER_URL, timeout=max(60.0, self.timeout))
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise AngelExecutionError(f"Angel instrument master download failed: {exc}") from exc
        rows = payload if isinstance(payload, list) else []
        contracts = [
            contract
            for item in rows
            if (contract := self._normalise_future_contract(item)) is not None
        ]
        if not contracts:
            raise AngelExecutionError("Angel instrument master returned no NIFTY futures contracts")
        with self._future_contract_lock:
            self._future_contracts = contracts
            self._future_contracts_loaded_at = time.time()
        self._save_future_contract_cache(contracts)
        return contracts

    def _normalise_future_contract(self, item: Any) -> dict[str, Any] | None:
        if not isinstance(item, dict):
            return None
        if str(item.get("exch_seg") or item.get("exchange") or "").strip().upper() != "NFO":
            return None
        name = str(item.get("name") or "").strip().upper()
        if name and name != "NIFTY":
            return None
        instrument_type = str(item.get("instrumenttype") or "").strip().upper()
        if instrument_type and instrument_type not in {"FUTIDX", "FUT"}:
            return None
        symbol = str(item.get("symbol") or item.get("tradingsymbol") or "").strip().upper()
        token = str(item.get("token") or item.get("symboltoken") or "").strip()
        expiry = self._parse_expiry(item.get("expiry"))
        lot_size = self._positive_int(item.get("lotsize") or item.get("lot_size"))
        if not symbol.startswith("NIFTY") or not symbol.endswith("FUT") or not token or not expiry or not lot_size:
            return None
        return {
            "symbol": symbol,
            "token": token,
            "exchange": "NFO",
            "expiry": expiry.isoformat(),
            "lot_size": lot_size,
        }

    def _pick_future_contract(
        self,
        contracts: list[dict[str, Any]],
        on_date: date,
    ) -> dict[str, Any] | None:
        eligible = []
        for contract in contracts:
            expiry = self._parse_expiry(contract.get("expiry"))
            if expiry and expiry >= on_date:
                eligible.append((expiry, str(contract.get("symbol") or ""), contract))
        return min(eligible, key=lambda item: (item[0], item[1]))[2] if eligible else None

    def _load_future_contract_cache(self) -> None:
        try:
            payload = json.loads(self.future_contract_cache_path.read_text(encoding="utf-8"))
            rows = payload.get("contracts") if isinstance(payload, dict) else payload
            contracts = [
                contract
                for item in (rows if isinstance(rows, list) else [])
                if (contract := self._normalise_future_contract(item)) is not None
            ]
            if contracts:
                self._future_contracts = contracts
                self._future_contracts_loaded_at = self.future_contract_cache_path.stat().st_mtime
        except (OSError, ValueError, TypeError):
            return

    def _save_future_contract_cache(self, contracts: list[dict[str, Any]]) -> None:
        try:
            self.future_contract_cache_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.future_contract_cache_path.with_suffix(".tmp")
            temp_path.write_text(
                json.dumps({"updated_at": int(time.time()), "contracts": contracts}, indent=2),
                encoding="utf-8",
            )
            temp_path.replace(self.future_contract_cache_path)
        except OSError:
            return

    def _refresh_future_contracts_async(self) -> None:
        with self._future_refresh_lock:
            if self._future_refresh_running:
                return
            self._future_refresh_running = True
            self._future_refresh_done.clear()

        def refresh() -> None:
            try:
                self._download_future_contracts()
            except AngelExecutionError:
                pass
            finally:
                with self._future_refresh_lock:
                    self._future_refresh_running = False
                    self._future_refresh_done.set()

        threading.Thread(target=refresh, name="angel-future-contract-refresh", daemon=True).start()

    def _parse_expiry(self, value: Any) -> date | None:
        text = str(value or "").strip().upper()
        for pattern in ("%d%b%Y", "%Y-%m-%d", "%d-%b-%Y"):
            try:
                return datetime.strptime(text, pattern).date()
            except ValueError:
                continue
        return None

    def _future_entry_side(self, direction: Any) -> str:
        normalised = str(direction or "").strip().upper()
        if normalised == "CE":
            return "BUY"
        if normalised == "PE":
            return "SELL"
        raise AngelExecutionError(f"Unsupported futures signal direction: {normalised or 'missing'}")

    def _execution_instrument(self, value: Any) -> str:
        mode = str(value or "FUTURE").strip().upper()
        return mode if mode in {"FUTURE", "OPTION"} else "FUTURE"

    def _selected_contract(self, trade: dict[str, Any]) -> dict[str, Any]:
        features = trade.get("features") if isinstance(trade.get("features"), dict) else {}
        if not features and trade.get("features_json"):
            try:
                features = json.loads(str(trade.get("features_json") or "{}"))
            except json.JSONDecodeError:
                features = {}
        contract = features.get("selected_option_contract") if isinstance(features, dict) else {}
        return contract if isinstance(contract, dict) else {}

    def _resolve_option_instrument(self, symbol: str, contract: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
        existing_token = str(contract.get("angel_token") or "").strip()
        existing_symbol = str(contract.get("angel_symbol") or "").strip()
        if existing_symbol and existing_token:
            return {"symbol": existing_symbol, "token": existing_token, "exchange": contract.get("angel_exchange") or "NFO"}
        query = self._search_query(symbol, contract)
        payload = {"exchange": "NFO", "searchscrip": query}
        response_payload, status_code, ok = self._post(
            self.SEARCH_URL,
            payload,
            self._headers(api_key=str(session["api_key"]), access_token=str(session["access_token"])),
        )
        self.db.save_angel_api_hit(
            username=str(session["username"]),
            action="searchScrip",
            symbol=symbol,
            request_payload=payload,
            response_payload=self._safe_response(response_payload),
            http_status=status_code,
            ok=ok,
            error_message=None if ok else self._message(response_payload),
        )
        data = response_payload.get("data") if isinstance(response_payload, dict) else []
        candidates = data if isinstance(data, list) else []
        selected = self._pick_candidate(candidates, contract)
        return {
            "symbol": selected.get("tradingsymbol") or selected.get("symbol") or selected.get("name") or symbol,
            "token": selected.get("symboltoken") or selected.get("token") or "",
            "exchange": selected.get("exchange") or "NFO",
        }

    def _pick_candidate(self, candidates: list[dict[str, Any]], contract: dict[str, Any]) -> dict[str, Any]:
        side = str(contract.get("side") or contract.get("option_type") or "").upper()
        strike = self._normalise_strike(contract.get("strike"))
        for item in candidates:
            text = f"{item.get('tradingsymbol') or ''} {item.get('symbol') or ''}".upper()
            if side and side not in text:
                continue
            if strike and strike not in re.sub(r"\D", "", text):
                continue
            return item
        return candidates[0] if candidates else {}

    def _search_query(self, symbol: str, contract: dict[str, Any]) -> str:
        text = symbol.upper().replace("NSE:", "").replace("NFO:", "").replace("-INDEX", "")
        if text.startswith("NIFTY"):
            return text
        side = str(contract.get("side") or contract.get("option_type") or "").upper()
        strike = self._normalise_strike(contract.get("strike"))
        return f"NIFTY {strike} {side}".strip()

    def _normalise_strike(self, value: Any) -> str:
        try:
            return str(int(round(float(value))))
        except (TypeError, ValueError):
            return ""

    def _positive_int(self, value: Any) -> int | None:
        try:
            parsed = int(float(value))
            return parsed if parsed > 0 else None
        except (TypeError, ValueError):
            return None

    def _order_quantity(self, session: dict[str, Any], lot_size: int | None = None) -> int:
        lot_count = max(1, int(session.get("lot_count") or 1))
        return lot_count * max(1, int(lot_size or self.default_lot_size()))

    def _order_payload(self, *, side: str, symbol: str, token: str, exchange: str, quantity: int) -> dict[str, Any]:
        return {
            "variety": self.variety,
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": side,
            "exchange": exchange,
            "ordertype": self.order_type,
            "producttype": self.product_type,
            "duration": self.duration,
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(int(quantity)),
        }

    def _headers(self, *, api_key: str, access_token: str | None = None) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": os.getenv("ANGEL_CLIENT_LOCAL_IP", os.getenv("CLIENT_LOCAL_IP", "127.0.0.1")),
            "X-ClientPublicIP": os.getenv("ANGEL_CLIENT_PUBLIC_IP", os.getenv("CLIENT_PUBLIC_IP", "0.0.0.0")),
            "X-MACAddress": os.getenv("ANGEL_CLIENT_MAC", os.getenv("CLIENT_MAC", "00:00:00:00:00:00")),
            "X-PrivateKey": api_key,
        }
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        return headers

    def _post(self, url: str, payload: dict[str, Any], headers: dict[str, str]) -> tuple[dict[str, Any], int | None, bool]:
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            try:
                body = response.json()
            except ValueError:
                body = {"raw": response.text[:2000]}
            ok = response.ok and self._angel_success(body)
            return body if isinstance(body, dict) else {"data": body}, response.status_code, ok
        except requests.RequestException as exc:
            return {"message": str(exc)}, None, False

    def _angel_success(self, body: dict[str, Any]) -> bool:
        if not isinstance(body, dict):
            return False
        if body.get("status") is True:
            return True
        status = str(body.get("status") or body.get("success") or "").strip().lower()
        return status in {"true", "success"}

    def _message(self, body: dict[str, Any]) -> str:
        if not isinstance(body, dict):
            return ""
        return str(body.get("message") or body.get("error") or body.get("errorcode") or "").strip()

    def _order_id(self, body: dict[str, Any]) -> str | None:
        data = body.get("data") if isinstance(body, dict) else {}
        if isinstance(data, dict):
            return str(data.get("orderid") or data.get("uniqueorderid") or "").strip() or None
        return None

    def _jwt_expiry(self, token: str) -> int | None:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        try:
            padded = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
            exp = payload.get("exp")
            return int(exp) if exp else None
        except Exception:
            return int(time.time()) + 86400

    def _safe_response(self, body: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(body, dict):
            return {}
        copied = dict(body)
        data = copied.get("data")
        if isinstance(data, dict):
            safe_data = dict(data)
            for key in ("jwtToken", "accessToken", "refreshToken", "feedToken"):
                if key in safe_data:
                    safe_data[key] = "***"
            copied["data"] = safe_data
        return copied
