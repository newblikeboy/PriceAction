from __future__ import annotations

import base64
import json
import os
import re
import time
from typing import Any

import pyotp
import requests

from app.storage.database import Database


class AngelExecutionError(RuntimeError):
    pass


class AngelExecutionManager:
    LOGIN_URL = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"
    SEARCH_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/searchScrip"
    ORDER_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/placeOrder"

    def __init__(self, database: Database) -> None:
        self.db = database
        self.timeout = float(os.getenv("ANGEL_REQUEST_TIMEOUT_SECONDS", "20"))
        self.execution_enabled = os.getenv("ANGEL_LIVE_EXECUTION_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
        self.default_lot_size_env = max(1, int(os.getenv("ANGEL_LOT_SIZE_QTY", "75")))
        self.product_type = os.getenv("ANGEL_PRODUCT_TYPE", "INTRADAY").strip().upper() or "INTRADAY"
        self.variety = os.getenv("ANGEL_ORDER_VARIETY", "NORMAL").strip().upper() or "NORMAL"
        self.duration = os.getenv("ANGEL_ORDER_DURATION", "DAY").strip().upper() or "DAY"
        self.order_type = os.getenv("ANGEL_ORDER_TYPE", "MARKET").strip().upper() or "MARKET"

    def status(self, username: str) -> dict[str, Any]:
        config = self.db.broker_config(username)
        config["server_execution_enabled"] = self.execution_enabled
        config["default_lot_size"] = self.default_lot_size()
        config["order_quantity"] = int(config["default_lot_size"]) * max(1, int(config.get("lot_count") or 1))
        return config

    def default_lot_size(self) -> int:
        return self.db.execution_lot_size(self.default_lot_size_env)

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
        contract = self._selected_contract(trade)
        symbol = str(contract.get("angel_symbol") or contract.get("tradingsymbol") or contract.get("symbol") or trade.get("option_symbol") or "").strip()
        if not symbol:
            return {"ok": False, "skipped": True, "reason": "No option symbol available for live order"}

        sessions = self.db.list_connected_angel_sessions()
        if not sessions:
            return {"ok": False, "skipped": True, "reason": "No connected Angel One user sessions"}

        resolved = self._resolve_option_instrument(symbol, contract, sessions[0])
        if not resolved.get("symbol") or not resolved.get("token"):
            return {"ok": False, "skipped": True, "reason": "Angel instrument token could not be resolved"}

        results = []
        for session in sessions:
            quantity = self._order_quantity(session)
            payload = self._order_payload(
                side="BUY",
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
                entry_side="BUY",
                entry_order_id=order_id,
                response_payload=self._safe_response(response_payload),
                failed=not placed_ok,
            )
            results.append({"username": session["username"], "ok": placed_ok, "order_id": order_id, "message": self._message(response_payload)})
        return {"ok": any(item["ok"] for item in results), "results": results}

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
            payload = self._order_payload(
                side="SELL",
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

    def _order_quantity(self, session: dict[str, Any]) -> int:
        lot_count = max(1, int(session.get("lot_count") or 1))
        return lot_count * self.default_lot_size()

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
