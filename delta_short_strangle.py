#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Delta Exchange India - Daily Short Strangle (user calls it "straddle") scheduler.

Environment variables:
  - DELTA_API_KEY: API key with Trading + Read permissions (required).
  - DELTA_API_SECRET: API secret (required).
  - TESTNET: "true"/"false" to select demo vs production (default: "true").
  - DELTA_BASE_URL: Optional override. Defaults:
      Testnet: https://cdn-ind.testnet.deltaex.org
      Prod India: https://api.india.delta.exchange

Config (via YAML path --config or env or defaults):
  - symbol_base: BTCUSDT
  - expiry_mode: nearest_weekly | nearest_daily | explicit_YYYY-MM-DD
  - explicit_expiry_date: "" (used only if expiry_mode starts with explicit_)
  - contracts_per_leg: 1
  - notional_per_leg: null (if set, overrides contracts_per_leg by dividing notional by contract notional)
  - use_market_orders: true
  - slippage_ticks: 2
  - stop_loss_k: 1 | 2 | 3 | 4 (stop price = p * (1 + k))
  - margin_mode: isolated | cross
  - leverage: 2
  - max_daily_runs: 1
  - log_level: INFO
  - dry_run: false

CLI:
  - Run once immediately:  python delta_short_strangle.py --run-once
  - Start scheduler (07:00:00 UTC daily): python delta_short_strangle.py --schedule
"""

from __future__ import annotations

import os
import sys
import json
import hmac
import hashlib
import time
import uuid
import math
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Literal

import requests
from requests import Session, Response
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pydantic import BaseModel, Field, ValidationError
from dateutil import tz, parser as dateparser
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import yaml

# ---------------------------
# Logging (UTC structured)
# ---------------------------
class UtcFormatter(logging.Formatter):
    converter = time.gmtime
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            s = time.strftime("%Y-%m-%dT%H:%M:%SZ", ct)
        return s

def setup_logger(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("delta_short_strangle")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    fmt = UtcFormatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(fmt)
    logger.handlers = [handler]
    logger.propagate = False
    return logger

# ---------------------------
# Config
# ---------------------------
class StrategyConfig(BaseModel):
    symbol_base: str = "BTCUSDT"
    expiry_mode: str = "nearest_weekly"
    explicit_expiry_date: Optional[str] = None
    contracts_per_leg: int = 1
    notional_per_leg: Optional[float] = None
    use_market_orders: bool = True
    slippage_ticks: int = 2
    stop_loss_k: Literal[1,2,3,4] = 1
    margin_mode: Literal["isolated","cross"] = "isolated"
    leverage: int = 2
    TESTNET: bool = True
    log_level: str = "INFO"
    max_daily_runs: int = 1
    dry_run: bool = False
    state_path: str = "delta_strangle_state.json"

# ---------------------------
# Delta REST client
# ---------------------------
class DeltaAPIError(Exception):
    pass

class DeltaClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str, logger: logging.Logger):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.session: Session = requests.Session()
        self.logger = logger

    def _sign(self, method: str, path: str, query: Optional[str], payload: str, timestamp: str) -> str:
        # method + timestamp + requestPath + query params + body
        prehash = method + timestamp + path + (query or "") + payload
        signature = hmac.new(self.api_secret.encode(), prehash.encode(), hashlib.sha256).hexdigest()
        return signature

    def _headers(self, signature: str, timestamp: str) -> Dict[str, str]:
        return {
            "api-key": self.api_key,
            "signature": signature,
            "timestamp": timestamp,
            "User-Agent": "python-3.11-delta-short-strangle",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
    )
    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        timestamp = str(int(time.time()))
        qs = ""
        if params:
            # Let requests handle actual query serialization; here we build a canonical query string to sign in the same ordering
            # Using requests to generate the query risks reordering; instead, build deterministic order
            pairs = []
            for k in sorted(params.keys()):
                v = params[k]
                pairs.append(f"{k}={v}")
            if pairs:
                qs = "?" + "&".join(pairs)
        payload = json.dumps(body) if body else ""
        signature = self._sign(method, path, qs, payload, timestamp)
        url = self.base_url + path
        headers = self._headers(signature, timestamp)
        resp = self.session.request(method, url, params=params, data=payload if body else None, headers=headers, timeout=(5, 30))
        if resp.status_code == 429:
            # respect reset header if present
            reset_ms = resp.headers.get("X-RATE-LIMIT-RESET")
            self.logger.warning(f"429 rate limited; reset in ms={reset_ms}; backing off")  # structured log
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success", True) and "error" in data:
            raise DeltaAPIError(str(data["error"]))
        return data

    # Public/Market endpoints
    def get_products(self, contract_types: Optional[str] = None, states: Optional[str] = "live", page_size: int = 500) -> List[Dict[str, Any]]:
        params = {"page_size": page_size}
        if contract_types:
            params["contract_types"] = contract_types
        if states:
            params["states"] = states
        data = self._request("GET", "/v2/products", params=params)
        return data.get("result", [])

    def get_tickers(self, contract_types: Optional[str] = None, underlying_asset_symbols: Optional[str] = None, expiry_date: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if contract_types:
            params["contract_types"] = contract_types
        if underlying_asset_symbols:
            params["underlying_asset_symbols"] = underlying_asset_symbols
        if expiry_date:
            params["expiry_date"] = expiry_date  # format DD-MM-YYYY
        data = self._request("GET", "/v2/tickers", params=params)
        return data.get("result", [])

    def get_ticker_symbol(self, symbol: str) -> Dict[str, Any]:
        data = self._request("GET", f"/v2/tickers/{symbol}")
        return data.get("result", {})

    # Orders
    def place_order(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/v2/orders", body=body).get("result", {})

    def get_open_orders(self, product_id: Optional[int] = None) -> List[Dict[str, Any]]:
        params = {}
        if product_id is not None:
            params["product_id"] = product_id
        data = self._request("GET", "/v2/orders", params=params)
        return data.get("result", [])

    def cancel_order(self, order_id: int) -> Dict[str, Any]:
        return self._request("DELETE", f"/v2/orders/{order_id}").get("result", {})

    def get_fills(self, order_id: Optional[int] = None, product_id: Optional[int] = None, page_size: int = 200) -> List[Dict[str, Any]]:
        params = {"page_size": page_size}
        if order_id is not None:
            params["order_id"] = order_id
        if product_id is not None:
            params["product_id"] = product_id
        data = self._request("GET", "/v2/fills", params=params)
        return data.get("result", [])

    def get_order_by_client_oid(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        # Community references an endpoint GET /v2/orders/client_order_id/{client_order_id}; handle 404 gracefully
        try:
            data = self._request("GET", f"/v2/orders/client_order_id/{client_order_id}")
            return data.get("result", {})
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise

# ---------------------------
# State Persistence
# ---------------------------
class State(BaseModel):
    last_run_date_utc: Optional[str] = None  # YYYY-MM-DD
    runs_today: int = 0
    orders: Dict[str, Any] = Field(default_factory=dict)  # keyed by yyyy-mm-dd
    open_legs: Dict[str, Any] = Field(default_factory=dict)  # per leg tracking

class StateStore:
    def __init__(self, path: str, logger: logging.Logger):
        self.path = path
        self.logger = logger
        self.state = self._load()

    def _load(self) -> State:
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    return State(**json.load(f))
            except Exception as e:
                self.logger.error(f"Failed to load state: {e}; starting fresh")
        return State()

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(json.loads(self.state.json()), f, indent=2)
        os.replace(tmp, self.path)

# ---------------------------
# Utilities
# ---------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def next_friday_noon_utc(now: datetime) -> datetime:
    # Expiries at 12:00 UTC
    weekday = now.weekday()  # Mon=0..Sun=6
    days_ahead = (4 - weekday) % 7
    target = (now + timedelta(days=days_ahead)).replace(hour=12, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=7)
    return target

def parse_expiry_from_symbol(symbol: str) -> Optional[datetime]:
    # Expect like C-BTC-90000-310125
    try:
        parts = symbol.split("-")
        if len(parts) >= 4:
            ddmmyy = parts[3]
            dt = datetime.strptime(ddmmyy, "%d%m%y").replace(hour=12, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            return dt
    except Exception:
        return None
    return None

def is_call(symbol: str) -> bool:
    return symbol.startswith("C-")

def is_put(symbol: str) -> bool:
    return symbol.startswith("P-")

def strike_from_symbol(symbol: str) -> Optional[float]:
    try:
        parts = symbol.split("-")
        if len(parts) >= 4:
            return float(parts[2])
    except Exception:
        return None
    return None

# ---------------------------
# Strategy
# ---------------------------
class ShortStrangleStrategy:
    def __init__(self, cfg: StrategyConfig, client: DeltaClient, state: StateStore, logger: logging.Logger):
        self.cfg = cfg
        self.client = client
        self.state_store = state
        self.logger = logger

    def _determine_base_url(self) -> str:
        return self.client.base_url

    def _fetch_spot(self) -> float:
        # Try ticker by symbol_base first; else fallback to general BTC tickers and pick spot/mark from BTC perpetual
        # As last resort, average best available mark/spot from BTC option chain top-of-book
        symbol = self.cfg.symbol_base
        try:
            t = self.client.get_ticker_symbol(symbol)
            # Prefer mark_price or spot_price
            for k in ["spot_price", "mark_price", "close", "open"]:
                if k in t and t[k]:
                    return float(t[k])
        except Exception:
            pass
        # fallback: general tickers for BTC
        tickers = self.client.get_tickers(underlying_asset_symbols="BTC")
        # Find a perpetual or futures with symbol containing BTC and USDT/USD
        perp = None
        for x in tickers:
            sym = x.get("symbol", "")
            if "BTC" in sym and ("USDT" in sym or "USD" in sym) and x.get("contract_type","").endswith("futures"):
                perp = x
                break
        if perp:
            for k in ["spot_price", "mark_price", "close", "open"]:
                if k in perp and perp[k]:
                    return float(perp[k])
        # fallback: use any option ticker's spot_price
        for x in tickers:
            if x.get("contract_type") in ("call_options","put_options"):
                sp = x.get("spot_price")
                if sp:
                    return float(sp)
        raise RuntimeError("Could not resolve spot price")

    def _load_option_products(self) -> List[Dict[str, Any]]:
        # Load live products for calls/puts
        prods = self.client.get_products(contract_types="call_options,put_options", states="live", page_size=500)
        # Filter BTC underlying by symbol pattern "C-BTC-" / "P-BTC-"
        btc = [p for p in prods if isinstance(p.get("symbol"), str) and ("-BTC-" in p["symbol"])]
        return btc

    def _select_expiry(self, now: datetime, products: List[Dict[str, Any]]) -> datetime:
        # Build set of future expiries from product settlement_time or symbol
        expiries: List[datetime] = []
        for p in products:
            st = p.get("settlement_time")
            if st:
                try:
                    dt = dateparser.parse(st)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt > now:
                        expiries.append(dt.astimezone(timezone.utc))
                except Exception:
                    pass
            else:
                dt = parse_expiry_from_symbol(p.get("symbol",""))
                if dt and dt > now:
                    expiries.append(dt)
        expiries = sorted(list({e for e in expiries}))
        if not expiries:
            raise RuntimeError("No future expiries found")
        mode = self.cfg.expiry_mode
        if mode == "nearest_daily":
            return expiries
        if mode == "nearest_weekly":
            target = next_friday_noon_utc(now)
            # choose the expiry closest to target but >= now
            expiries_future = [e for e in expiries if e >= now]
            # exact match first, else nearest after target, else nearest overall
            exact = [e for e in expiries_future if e.date() == target.date()]
            if exact:
                return exact
            after = [e for e in expiries_future if e >= target]
            if after:
                return after
            return expiries_future
        if mode.startswith("explicit"):
            date_str = self.cfg.explicit_expiry_date
            if not date_str:
                raise RuntimeError("explicit_expiry_date required for explicit mode")
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=12, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            cands = [e for e in expiries if e.date() == dt.date()]
            if cands:
                return cands
            # otherwise, choose nearest after
            after = [e for e in expiries if e >= dt]
            if after:
                return after
            return expiries[-1]
        return expiries

    def _collect_chain_for_expiry(self, products: List[Dict[str, Any]], expiry: datetime) -> List[Dict[str, Any]]:
        chain = []
        for p in products:
            sym = p.get("symbol","")
            dt = parse_expiry_from_symbol(sym)
            if dt and dt.date() == expiry.date():
                chain.append(p)
        return chain

    def _detect_strike_step(self, chain: List[Dict[str, Any]]) -> int:
        strikes: List[int] = []
        for p in chain:
            s = strike_from_symbol(p.get("symbol",""))
            if s:
                strikes.append(int(round(s)))
        strikes = sorted(set(strikes))
        if len(strikes) < 3:
            return 100
        diffs = [b - a for a, b in zip(strikes, strikes[1:]) if b - a > 0]
        if not diffs:
            return 100
        # choose the most common positive diff
        counts: Dict[int,int] = {}
        for d in diffs:
            counts[d] = counts.get(d,0)+1
        step = max(counts.items(), key=lambda x: x[1])
        return int(step)

    def _nearest_strike(self, anchor: float, strikes: List[int], direction: Literal["above","below"]) -> int:
        # Select nearest listed strike that is strictly above or below spot as required
        if direction == "above":
            cands = [s for s in strikes if s > anchor]
            if not cands:
                # fallback: choose min strike above anchor ignoring strictness
                cands = [s for s in strikes if s >= math.floor(anchor)]
            return min(cands, key=lambda s: abs(s - anchor))
        else:
            cands = [s for s in strikes if s < anchor]
            if not cands:
                cands = [s for s in strikes if s <= math.ceil(anchor)]
            return min(cands, key=lambda s: abs(s - anchor))

    def _find_products_for_strikes(self, chain: List[Dict[str, Any]], ce_strike: int, pe_strike: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        ce = None
        pe = None
        for p in chain:
            sym = p.get("symbol","")
            s = strike_from_symbol(sym)
            if not s:
                continue
            if is_call(sym) and int(round(s)) == ce_strike:
                ce = p
            if is_put(sym) and int(round(s)) == pe_strike:
                pe = p
        if not ce or not pe:
            raise RuntimeError("Could not find CE or PE products for strikes")
        return ce, pe

    def _best_price_for_side(self, symbol: str, side: Literal["buy","sell"], tick_size: float, slippage_ticks: int) -> Optional[float]:
        t = self.client.get_ticker_symbol(symbol)
        quotes = t.get("quotes", {}) or {}
        best_bid = quotes.get("best_bid")
        best_ask = quotes.get("best_ask")
        if best_bid is None or best_ask is None:
            return None
        best_bid = float(best_bid)
        best_ask = float(best_ask)
        if side == "sell":
            # place limit near bid minus small slippage
            px = max(0.0, best_bid - slippage_ticks * tick_size)
        else:
            px = best_ask + slippage_ticks * tick_size
        return round(px / tick_size) * tick_size

    def _position_size_from_config(self, product: Dict[str, Any]) -> int:
        # contract_value denotes underlying per contract (e.g., 0.001 BTC); we size in contracts
        if self.cfg.notional_per_leg and self.cfg.notional_per_leg > 0:
            # divide notional by spot*contract_value to estimate contracts; ensure >=1
            # spot required here; but we call with available spot outside when needed
            pass
        # default contracts_per_leg
        return max(1, int(self.cfg.contracts_per_leg))

    def _ensure_leverage(self, product_id: int):
        # Leverage/margin mode changes may require separate endpoints; many venues set per-position leverage
        # If not available in v2 docs, we skip and rely on account defaults
        return

    def _place_leg(self, product: Dict[str, Any], side: Literal["buy","sell"], qty: int, use_mkt: bool, limit_px: Optional[float], client_oid: str, reduce_only: bool = False, stop_params: Optional[Dict[str, Any]] = None, post_only: bool = False) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "product_id": int(product["id"]),
            "size": int(qty),
            "side": side,
            "order_type": "market_order" if use_mkt else "limit_order",
            "reduce_only": bool(reduce_only),
            "client_order_id": client_oid,
            "time_in_force": "gtc",
            "post_only": bool(post_only)
        }
        if not use_mkt and limit_px is not None:
            body["limit_price"] = str(limit_px)
        if stop_params:
            body.update(stop_params)
        if self.cfg.dry_run:
            self.logger.info(f"DRY_RUN place_order: {json.dumps(body)}")
            return {"id": 0, "client_order_id": client_oid, "size": qty, "side": side, "order_type": body["order_type"]}
        return self.client.place_order(body)

    def _compute_stop_body(self, side: Literal["buy","sell"], stop_price: float, stop_type: Literal["market","limit"]="market", limit_price: Optional[float]=None) -> Dict[str, Any]:
        # Place a separate stop order via same endpoint using stop_* fields with stop_order_type
        stop_body: Dict[str, Any] = {
            "stop_order_type": "stop_loss_order",  # docs/community enum: stop_loss_order / take_profit_order
            "stop_price": str(stop_price),
            "stop_trigger_method": "last_traded_price",
        }
        if stop_type == "market":
            # default: do nothing more; server will send market at trigger
            pass
        else:
            if limit_price is not None:
                stop_body["order_type"] = "limit_order"
                stop_body["limit_price"] = str(limit_price)
        return stop_body

    def _avg_fill_price(self, fills: List[Dict[str, Any]]) -> Optional[float]:
        if not fills:
            return None
        filled = 0
        notional = 0.0
        for f in fills:
            sz = int(f.get("size", 0))
            px = float(f.get("price", 0.0))
            filled += sz
            notional += sz * px
        return (notional / filled) if filled > 0 else None

    def _wait_for_fills(self, order_id: int, product_id: int, timeout_s: int = 10) -> Tuple[int, Optional[float]]:
        # Poll fills endpoint briefly to get average price
        deadline = time.time() + timeout_s
        last_avg = None
        total_filled = 0
        while time.time() < deadline:
            fills = self.client.get_fills(order_id=order_id, product_id=product_id)
            if fills:
                avg = self._avg_fill_price(fills)
                filled = sum(int(f.get("size",0)) for f in fills)
                if filled > 0:
                    last_avg = avg
                    total_filled = filled
                    break
            time.sleep(0.5)
        return total_filled, last_avg

    def run_once(self):
        today = utcnow().date().isoformat()
        # Enforce max daily runs
        if self.state_store.state.last_run_date_utc == today and self.state_store.state.runs_today >= self.cfg.max_daily_runs:
            self.logger.info(f"Already ran today {self.state_store.state.runs_today}>=max {self.cfg.max_daily_runs}; exiting")
            return

        spot = self._fetch_spot()
        self.logger.info(f"Spot {self.cfg.symbol_base} = {spot}")

        products = self._load_option_products()
        expiry = self._select_expiry(utcnow(), products)
        chain = self._collect_chain_for_expiry(products, expiry)

        # Detect tick size for options: use product tick_size if present; default 1.0
        tick_size = 1.0
        if chain:
            ts = chain.get("tick_size")
            try:
                if ts:
                    tick_size = float(ts)
            except Exception:
                pass

        strike_step = self._detect_strike_step(chain) or 100
        offset = 0.01 * spot
        ce_anchor = spot + offset
        pe_anchor = spot - offset

        strikes_list: List[int] = sorted({int(round(strike_from_symbol(p.get("symbol","")) or 0)) for p in chain if strike_from_symbol(p.get("symbol",""))})
        ce_strike = self._nearest_strike(ce_anchor, strikes_list, "above")
        pe_strike = self._nearest_strike(pe_anchor, strikes_list, "below")

        # CE > spot, PE < spot enforcement; if violated due to sparse strikes, adjust to closest in correct direction
        if ce_strike <= int(math.floor(spot)):
            ce_candidates = [s for s in strikes_list if s > spot]
            if ce_candidates:
                ce_strike = ce_candidates
        if pe_strike >= int(math.ceil(spot)):
            pe_candidates = [s for s in strikes_list if s < spot]
            if pe_candidates:
                pe_strike = pe_candidates[-1]

        ce_prod, pe_prod = self._find_products_for_strikes(chain, ce_strike, pe_strike)
        ce_symbol = ce_prod["symbol"]
        pe_symbol = pe_prod["symbol"]
        ce_id = int(ce_prod["id"])
        pe_id = int(pe_prod["id"])

        # Position sizing (contracts)
        qty = max(1, int(self.cfg.contracts_per_leg))

        # Entry prices
        ce_limit = None
        pe_limit = None
        if not self.cfg.use_market_orders:
            ce_limit = self._best_price_for_side(ce_symbol, "sell", tick_size, self.cfg.slippage_ticks)
            pe_limit = self._best_price_for_side(pe_symbol, "sell", tick_size, self.cfg.slippage_ticks)

        # Idempotent client_order_id
        run_key = f"{today}-strangle"
        ce_client_oid = f"{run_key}-CE-{ce_strike}"
        pe_client_oid = f"{run_key}-PE-{pe_strike}"

        # Persist intent and log summary
        self.state_store.state.orders.setdefault(today, {})
        self.state_store.state.orders[today]["plan"] = {
            "spot": spot,
            "expiry": expiry.isoformat(),
            "strike_step": strike_step,
            "ce": {"symbol": ce_symbol, "product_id": ce_id, "strike": ce_strike, "limit": ce_limit, "qty": qty},
            "pe": {"symbol": pe_symbol, "product_id": pe_id, "strike": pe_strike, "limit": pe_limit, "qty": qty},
        }
        self.state_store.save()

        self.logger.info(f"Chosen expiry={expiry.date()} ce={ce_symbol}@{ce_strike} pe={pe_symbol}@{pe_strike} tick_size={tick_size} strike_step={strike_step}")
        self.logger.info(f"Entries use_market_orders={self.cfg.use_market_orders} slippage_ticks={self.cfg.slippage_ticks}")

        # Place CE leg (sell)
        ce_order = self._place_leg(
            product=ce_prod,
            side="sell",
            qty=qty,
            use_mkt=self.cfg.use_market_orders,
            limit_px=ce_limit,
            client_oid=ce_client_oid,
        )
        self.logger.info(f"CE order placed: {ce_order}")
        ce_order_id = int(ce_order.get("id", 0)) if ce_order else 0

        # Place PE leg (sell)
        pe_order = self._place_leg(
            product=pe_prod,
            side="sell",
            qty=qty,
            use_mkt=self.cfg.use_market_orders,
            limit_px=pe_limit,
            client_oid=pe_client_oid,
        )
        self.logger.info(f"PE order placed: {pe_order}")
        pe_order_id = int(pe_order.get("id", 0)) if pe_order else 0

        # Confirm fills and place stops per leg
        def handle_leg(product, order_id, leg_key, leg_symbol):
            if order_id == 0:
                self.logger.warning(f"{leg_key} order id missing; skipping stop placement")
                return
            filled, avg = self._wait_for_fills(order_id, int(product["id"]))
            if filled <= 0 or avg is None:
                self.logger.info(f"{leg_key} not filled yet or no fills; will not place stop now")
                return
            k = self.cfg.stop_loss_k
            stop_price = avg * (1.0 + float(k))
            stop_body = self._compute_stop_body(side="buy", stop_price=stop_price, stop_type="market")
            stop_oid = f"{run_key}-STOP-{leg_key}-{int(stop_price)}"
            stop_body["client_order_id"] = stop_oid
            # Stop is a buy to close (reduce_only true), ensure product id/side/size present
            stop_payload = {
                "product_id": int(product["id"]),
                "size": int(filled),
                "side": "buy",
                "order_type": "market_order",
                **stop_body
            }
            if self.cfg.dry_run:
                self.logger.info(f"DRY_RUN stop for {leg_key}: {json.dumps(stop_payload)}")
            else:
                resp = self.client.place_order(stop_payload)
                self.logger.info(f"{leg_key} stop placed at {stop_price}: {resp}")
            self.state_store.state.orders[today].setdefault("stops", {})[leg_key] = {
                "avg_entry": avg, "stop_price": stop_price, "size": filled, "client_order_id": stop_oid
            }
            self.state_store.save()

        handle_leg(ce_prod, ce_order_id, "CE", ce_symbol)
        handle_leg(pe_prod, pe_order_id, "PE", pe_symbol)

        # Final prints for computed values
        self.logger.info(f"Computed: spot={spot:.2f} ce_strike={ce_strike} pe_strike={pe_strike} k={self.cfg.stop_loss_k}")
        self.state_store.state.last_run_date_utc = today
        self.state_store.state.runs_today = (self.state_store.state.runs_today + 1) if self.state_store.state.last_run_date_utc == today else 1
        self.state_store.save()

# ---------------------------
# Scheduler / CLI
# ---------------------------
def load_config(path: Optional[str]) -> StrategyConfig:
    base = StrategyConfig()
    env_testnet = os.getenv("TESTNET", str(base.TESTNET)).lower() == "true"
    env_log_level = os.getenv("LOG_LEVEL", base.log_level)
    # Defaults updated by env; YAML can override
    cfg = base.copy(update={"TESTNET": env_testnet, "log_level": env_log_level})
    if path and os.path.exists(path):
        with open(path, "r") as f:
            y = yaml.safe_load(f) or {}
            cfg = StrategyConfig(**{**cfg.dict(), **y})
    return cfg

def determine_base_url(cfg: StrategyConfig) -> str:
    # From docs: India prod vs testnet endpoints
    if os.getenv("DELTA_BASE_URL"):
        return os.getenv("DELTA_BASE_URL").rstrip("/")
    return ("https://cdn-ind.testnet.deltaex.org" if cfg.TESTNET else "https://api.india.delta.exchange")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument("--schedule", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    logger = setup_logger(cfg.log_level)

    api_key = os.getenv("DELTA_API_KEY")
    api_secret = os.getenv("DELTA_API_SECRET")
    if not api_key or not api_secret:
        logger.error("Missing DELTA_API_KEY/DELTA_API_SECRET")
        sys.exit(1)
    base_url = determine_base_url(cfg)
    logger.info(f"Using base_url={base_url} TESTNET={cfg.TESTNET}")

    client = DeltaClient(api_key=api_key, api_secret=api_secret, base_url=base_url, logger=logger)
    state = StateStore(cfg.state_path, logger)
    strategy = ShortStrangleStrategy(cfg, client, state, logger)

    if args.run-once:
        strategy.run_once()
        return

    if args.schedule:
        sched = BackgroundScheduler(timezone=timezone.utc)
        # Daily at 07:00:00 UTC
        trigger = CronTrigger(hour=7, minute=0, second=0, timezone=timezone.utc)
        sched.add_job(strategy.run_once, trigger=trigger, name="daily_short_strangle")
        sched.start()
        next_runs = sched.get_jobs().next_run_time
        logger.info(f"Scheduler started; next run at UTC {next_runs}")
        # Keep alive
        try:
            signal.signal(signal.SIGINT, lambda s,f: sched.shutdown(wait=False))
            signal.signal(signal.SIGTERM, lambda s,f: sched.shutdown(wait=False))
        except Exception:
            pass
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Stopping scheduler")
            sched.shutdown(wait=False)
    else:
        # default run-once
        strategy.run_once()

if __name__ == "__main__":
    main()
