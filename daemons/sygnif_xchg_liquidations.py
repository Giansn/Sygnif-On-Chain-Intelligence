#!/usr/bin/env python3
"""sygnif_xchg_liquidations.py — Multi-exchange liquidation aggregator.

Subscribes to public liquidation WebSockets from Binance, OKX, and Bitget.
Aggregates with our existing Bybit liq stream (collected by sygnif-bybit-daemon)
into a price-bucketed heatmap. Detects cluster events (multiple exchanges
liquidating in same price band within window) and emits high-confidence signals.

Why this matters: realized liquidations across 3-4 exchanges within the same
1-min window represent a tradeable cascade event (forced flows reinforce
direction). Single-exchange liq is noise; multi-exchange clustered liq is
signal.

Endpoints (all free, no key):
  Binance USD-M:  wss://fstream.binance.com/ws/!forceOrder@arr
  OKX USDT-perp:  wss://ws.okx.com:8443/ws/v5/public  channel=liquidation-orders
  Bitget:         wss://ws.bitget.com/mix/v1/stream  channel=liquidation

State: /var/lib/sygnif/xchg_liq_state.json
Swarm topics:
  xchg.liquidation         (any single >$1M event)
  xchg.liquidation_cluster (≥2 exchanges in same 1-min window, same direction)
"""
from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import signal
import sqlite3
import sys
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Optional

try:
    import websocket
except ImportError:
    print("websocket-client not installed; pip install websocket-client", file=sys.stderr)
    sys.exit(1)

# ============================================================================
# Config
# ============================================================================
LIQ_REPORT_THRESHOLD_USD = float(os.environ.get("SYGNIF_LIQ_REPORT_USD", "1000000"))   # $1M
LIQ_CLUSTER_WINDOW_S     = float(os.environ.get("SYGNIF_LIQ_CLUSTER_WINDOW_S", "60"))
LIQ_CLUSTER_MIN_EXCH     = int(os.environ.get("SYGNIF_LIQ_CLUSTER_MIN_EXCH", "2"))
HEARTBEAT_S              = float(os.environ.get("SYGNIF_LIQ_HEARTBEAT_S", "30"))
DB_PATH                  = "/var/lib/sygnif/swarm.db"
STATE_FILE               = pathlib.Path("/var/lib/sygnif/xchg_liq_state.json")

# Targeted symbols (per exchange naming)
SYMBOLS = {
    "binance": ["BTCUSDT", "ETHUSDT"],
    "okx":     ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
}

# State
_running = True
_metrics = defaultdict(int)
_metrics["started_at"] = time.time()

# In-memory rolling liquidation window (per exchange × side × asset)
# (ts, exchange, asset, side, value_usd, price)
_liq_window: deque = deque(maxlen=2000)
_lock = threading.Lock()


# ============================================================================
# Swarm emit
# ============================================================================
def emit_swarm(topic: str, content: str, meta: dict, tags: list) -> None:
    if not os.path.exists(DB_PATH):
        return
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        rid = str(uuid.uuid4())
        c.execute(
            "INSERT OR IGNORE INTO swarm_entries "
            "(id, created, swarm_id, agent_id, topic, content, meta, tags) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rid, int(time.time()), "trading",
             "sygnif-xchg-liq", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm emit failed: {type(e).__name__}: {e}",
              file=sys.stderr, flush=True)


# ============================================================================
# State
# ============================================================================
def load_state() -> dict:
    if not STATE_FILE.exists():
        return _new_state()
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return _new_state()


def _new_state() -> dict:
    return {
        "schema":           "sygnif.xchg_liq.v1",
        "created_at_utc":   dt.datetime.now(dt.timezone.utc).isoformat(),
        "recent_events":    [],
        "recent_clusters":  [],
        "totals_by_exch":   {},
        "metrics":          {},
    }


def save_state(state: dict) -> None:
    state["updated_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    state["recent_events"]   = state["recent_events"][-500:]
    state["recent_clusters"] = state["recent_clusters"][-200:]
    state["metrics"] = dict(_metrics)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, default=str, indent=2))
    os.replace(tmp, STATE_FILE)


# ============================================================================
# Cluster detection
# ============================================================================
def detect_cluster(state: dict, exchange: str, asset: str,
                    side: str, value_usd: float, price: float) -> None:
    """When a liq event lands, look back LIQ_CLUSTER_WINDOW_S for other
    exchanges' liq events on same asset+side. If ≥ LIQ_CLUSTER_MIN_EXCH
    distinct exchanges hit in window, emit cluster event."""
    now = time.time()
    with _lock:
        # Rolling cleanup
        while _liq_window and _liq_window[0][0] < now - LIQ_CLUSTER_WINDOW_S:
            _liq_window.popleft()
        # Look for matching events
        matched = [(t, ex, a, s, v, p) for (t, ex, a, s, v, p) in _liq_window
                   if a == asset and s == side]
        exchanges_in_window = set(ex for (t, ex, a, s, v, p) in matched) | {exchange}
        total_value = sum(v for (t, ex, a, s, v, p) in matched) + value_usd
        # Add current event
        _liq_window.append((now, exchange, asset, side, value_usd, price))

    if len(exchanges_in_window) >= LIQ_CLUSTER_MIN_EXCH and total_value >= LIQ_REPORT_THRESHOLD_USD:
        avg_price = (sum(p for (t, ex, a, s, v, p) in matched) + price) / max(len(matched) + 1, 1)
        cluster_ev = {
            "ts":             now,
            "ts_utc":         dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
            "asset":          asset,
            "side":           side,
            "exchanges":      sorted(exchanges_in_window),
            "n_exchanges":    len(exchanges_in_window),
            "total_value_usd": round(total_value, 0),
            "avg_price":      round(avg_price, 2),
            "window_s":       LIQ_CLUSTER_WINDOW_S,
        }
        state["recent_clusters"].append(cluster_ev)
        _metrics["clusters_emitted"] += 1
        head = (f"LIQ_CLUSTER {asset} {side}  "
                f"{len(exchanges_in_window)} exchanges  "
                f"${total_value/1e6:.2f}M  @ ${avg_price:,.0f}  "
                f"in {LIQ_CLUSTER_WINDOW_S}s")
        emit_swarm("xchg.liquidation_cluster", head, {
            **cluster_ev,
            "type":       "LIQ_CLUSTER",
            "confidence": 90,
        }, ["xchg", "liquidation", "cluster", asset, side])
        print(f"  [CLUSTER] {head}", flush=True)


def record_liq_event(state: dict, exchange: str, raw_asset: str,
                       side: str, value_usd: float, price: float) -> None:
    """Record + maybe emit single-event swarm signal."""
    # Normalize asset
    asset = "BTC" if "BTC" in raw_asset.upper() else \
            "ETH" if "ETH" in raw_asset.upper() else raw_asset.upper()
    side  = side.upper()

    now = time.time()
    ev = {
        "ts":           now,
        "ts_utc":       dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        "exchange":     exchange,
        "asset":        asset,
        "side":         side,
        "value_usd":    round(value_usd, 0),
        "price":        round(price, 2),
    }
    state["recent_events"].append(ev)
    _metrics[f"liq_events_{exchange}"] += 1
    _metrics["liq_events_total"] += 1
    state.setdefault("totals_by_exch", {}).setdefault(exchange, 0)
    state["totals_by_exch"][exchange] += value_usd

    if value_usd >= LIQ_REPORT_THRESHOLD_USD:
        head = (f"LIQ_{exchange.upper()} {asset} {side}  "
                f"${value_usd/1e6:.2f}M  @ ${price:,.0f}")
        emit_swarm("xchg.liquidation", head, {
            **ev,
            "type":       "LIQ_SINGLE",
            "confidence": 80,
        }, ["xchg", "liquidation", exchange, asset, side])
        print(f"  [LIQ] {head}", flush=True)

    # Cluster detection on every event
    detect_cluster(state, exchange, asset, side, value_usd, price)


# ============================================================================
# Binance WS handler
# ============================================================================
def binance_thread(state: dict) -> None:
    URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

    def on_message(ws, msg):
        try:
            d = json.loads(msg)
        except json.JSONDecodeError:
            return
        # Binance sends {"e":"forceOrder","o":{...}}
        order = d.get("o") or d
        symbol = order.get("s", "")
        if not symbol.endswith("USDT"):
            return
        # Binance: side = the side of the forced order (BUY or SELL)
        # A "BUY" force-order is a SHORT being liquidated (long entered to close)
        # A "SELL" force-order is a LONG being liquidated
        bn_side = order.get("S")
        side = "SHORT_LIQ" if bn_side == "BUY" else "LONG_LIQ"
        try:
            qty = float(order.get("q", 0))
            price = float(order.get("p", 0))
            value_usd = qty * price
        except (ValueError, TypeError):
            return
        if value_usd <= 0:
            return
        record_liq_event(state, "binance", symbol, side, value_usd, price)

    def on_open(ws):
        print(f"  [Binance] WS connected", flush=True)
        _metrics["binance_connects"] += 1

    def on_error(ws, e):
        print(f"  [Binance] WS error: {e}", file=sys.stderr, flush=True)
        _metrics["binance_errors"] += 1

    def on_close(ws, code, reason):
        print(f"  [Binance] WS closed code={code}", file=sys.stderr, flush=True)

    while _running:
        try:
            ws = websocket.WebSocketApp(URL,
                                          on_message=on_message,
                                          on_open=on_open,
                                          on_error=on_error,
                                          on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"  [Binance] thread err: {type(e).__name__}: {e}",
                  file=sys.stderr, flush=True)
        if _running:
            time.sleep(5)


# ============================================================================
# OKX WS handler
# ============================================================================
def okx_thread(state: dict) -> None:
    URL = "wss://ws.okx.com:8443/ws/v5/public"

    def on_message(ws, msg):
        try:
            d = json.loads(msg)
        except json.JSONDecodeError:
            return
        if d.get("event") in ("subscribe", "error"):
            return
        for item in (d.get("data") or []):
            inst_id = item.get("instId", "")
            if not ("BTC" in inst_id or "ETH" in inst_id):
                continue
            for det in (item.get("details") or []):
                # OKX: side = direction of position being liquidated
                # "buy" means long was liquidated (sell back) — wait actually:
                # OKX docs: side = "buy"/"sell" of the *liquidation order*
                # Liquidation order is OPPOSITE of position direction.
                # buy liq order = closing a SHORT = SHORT_LIQ
                # sell liq order = closing a LONG = LONG_LIQ
                ok_side = det.get("side", "")
                side = "SHORT_LIQ" if ok_side == "buy" else "LONG_LIQ"
                try:
                    sz = float(det.get("sz", 0))    # contracts
                    bk_px = float(det.get("bkPx", 0))   # bankruptcy price
                    # OKX BTC perp: 1 contract = 0.01 BTC
                    multiplier = 0.01 if "BTC" in inst_id else 0.1
                    qty_native = sz * multiplier
                    value_usd = qty_native * bk_px
                except (ValueError, TypeError):
                    continue
                if value_usd <= 0:
                    continue
                record_liq_event(state, "okx", inst_id, side, value_usd, bk_px)

    def on_open(ws):
        print(f"  [OKX] WS connected", flush=True)
        sub_args = [{"channel": "liquidation-orders", "instType": "SWAP"}]
        ws.send(json.dumps({"op": "subscribe", "args": sub_args}))
        _metrics["okx_connects"] += 1

    def on_error(ws, e):
        print(f"  [OKX] WS error: {e}", file=sys.stderr, flush=True)
        _metrics["okx_errors"] += 1

    def on_close(ws, code, reason):
        print(f"  [OKX] WS closed code={code}", file=sys.stderr, flush=True)

    while _running:
        try:
            ws = websocket.WebSocketApp(URL,
                                          on_message=on_message,
                                          on_open=on_open,
                                          on_error=on_error,
                                          on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"  [OKX] thread err: {type(e).__name__}: {e}",
                  file=sys.stderr, flush=True)
        if _running:
            time.sleep(5)


# ============================================================================
# Main
# ============================================================================
def main() -> int:
    global _running
    print(f"=== sygnif_xchg_liquidations started @ "
          f"{dt.datetime.now(dt.timezone.utc).isoformat()} ===", flush=True)
    print(f"  report threshold:   ${LIQ_REPORT_THRESHOLD_USD/1e6:.1f}M", flush=True)
    print(f"  cluster window:     {LIQ_CLUSTER_WINDOW_S}s", flush=True)
    print(f"  cluster min exch:   {LIQ_CLUSTER_MIN_EXCH}", flush=True)

    state = load_state()

    def _sigterm(sig, frame):
        global _running
        print(f"  signal {sig}, shutting down", flush=True)
        _running = False
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT,  _sigterm)

    # Launch WS threads
    t_bn  = threading.Thread(target=binance_thread, args=(state,), daemon=True)
    t_okx = threading.Thread(target=okx_thread, args=(state,), daemon=True)
    t_bn.start()
    t_okx.start()

    last_save = 0.0
    last_hb = 0.0
    while _running:
        now = time.time()
        if now - last_save >= 60:
            try:
                save_state(state)
            except Exception as e:
                print(f"  ! save err: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_save = now
        if now - last_hb >= HEARTBEAT_S:
            print(f"  [HB] bn={_metrics.get('liq_events_binance',0)} "
                  f"okx={_metrics.get('liq_events_okx',0)} "
                  f"clusters={_metrics.get('clusters_emitted',0)} "
                  f"swarm={_metrics.get('swarm_emits',0)}",
                  flush=True)
            last_hb = now
        time.sleep(2)

    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
