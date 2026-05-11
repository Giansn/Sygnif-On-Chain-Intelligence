#!/usr/bin/env python3
"""sygnif_market_premium.py — Cross-venue BTC price premium tracker.

Tracks the gap between Coinbase BTC/USD and Binance BTC/USDT.
Widening premium = US institutional buying surge ("Saylor signal").
Negative premium = US selling pressure.

Also tracks Binance vs Bybit basis to detect Asian-vs-perp imbalance.

Endpoints (all free, no key):
  Coinbase:  https://api.exchange.coinbase.com/products/BTC-USD/ticker
  Binance:   https://api.binance.com/api/v3/ticker/price
  Bybit:     https://api.bybit.com/v5/market/tickers

Cadence: every 60s.
Swarm topic: market.premium
"""
from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import signal
import sqlite3
import sys
import time
import urllib.request
import uuid
from collections import defaultdict

DB_PATH    = "/var/lib/sygnif/swarm.db"
STATE_FILE = pathlib.Path("/var/lib/sygnif/market_premium.json")
POLL_S     = float(os.environ.get("SYGNIF_PREMIUM_POLL_S", "60"))
EMIT_BPS_THRESHOLD = float(os.environ.get("SYGNIF_PREMIUM_BPS_EMIT", "5"))   # only emit on ≥5bps

HEADERS = {"User-Agent": "sygnif-market-premium/1.0"}

_running = True
_metrics = defaultdict(int)
_metrics["started_at"] = time.time()


def _jget(url, timeout=8):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        _metrics["http_failures"] += 1
        return None


def fetch_coinbase_btc_usd():
    r = _jget("https://api.exchange.coinbase.com/products/BTC-USD/ticker")
    if not r: return None
    try:
        return float(r.get("price", 0))
    except (ValueError, TypeError):
        return None


def fetch_binance_btc_usdt():
    r = _jget("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
    if not r: return None
    try:
        return float(r.get("price", 0))
    except (ValueError, TypeError):
        return None


def fetch_bybit_btc_usdt():
    r = _jget("https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT")
    if not r: return None
    try:
        rows = (r.get("result") or {}).get("list") or []
        if not rows: return None
        return float(rows[0].get("lastPrice", 0))
    except (ValueError, TypeError):
        return None


def emit_swarm(topic, content, meta, tags):
    if not os.path.exists(DB_PATH): return
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        rid = str(uuid.uuid4())
        c.execute(
            "INSERT OR IGNORE INTO swarm_entries "
            "(id, created, swarm_id, agent_id, topic, content, meta, tags) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rid, int(time.time()), "trading",
             "sygnif-market-premium", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm err: {e}", file=sys.stderr, flush=True)


def load_state():
    if not STATE_FILE.exists():
        return {"schema": "sygnif.premium.v1",
                "history": [], "metrics": {}}
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {"schema": "sygnif.premium.v1",
                "history": [], "metrics": {}}


def save_state(state):
    state["updated_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    state["history"] = state.get("history", [])[-720:]   # last 12h @ 60s
    state["metrics"] = dict(_metrics)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, default=str, indent=2))
    os.replace(tmp, STATE_FILE)


def main():
    global _running
    print(f"=== sygnif_market_premium started @ "
          f"{dt.datetime.now(dt.timezone.utc).isoformat()} ===", flush=True)
    print(f"  poll: {POLL_S}s   emit threshold: {EMIT_BPS_THRESHOLD} bps", flush=True)
    state = load_state()

    def _sigterm(sig, frame):
        global _running
        print(f"  signal {sig}", flush=True)
        _running = False
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT,  _sigterm)

    last_poll = 0.0
    last_save = 0.0
    while _running:
        now = time.time()
        if now - last_poll >= POLL_S:
            try:
                cb = fetch_coinbase_btc_usd()
                bn = fetch_binance_btc_usdt()
                bb = fetch_bybit_btc_usdt()
                if cb and bn and bb:
                    # Coinbase USD vs Binance USDT
                    cb_bn_bps = (cb - bn) / bn * 10000
                    # Binance spot vs Bybit perp
                    bn_bb_bps = (bn - bb) / bb * 10000
                    snap = {
                        "ts":           int(now),
                        "ts_utc":       dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
                        "coinbase_usd": round(cb, 2),
                        "binance_usdt": round(bn, 2),
                        "bybit_usdt":   round(bb, 2),
                        "cb_bn_bps":    round(cb_bn_bps, 2),
                        "bn_bb_bps":    round(bn_bb_bps, 2),
                    }
                    state["history"].append(snap)
                    _metrics["polls_ok"] += 1
                    # Emit if meaningful
                    if abs(cb_bn_bps) >= EMIT_BPS_THRESHOLD or abs(bn_bb_bps) >= EMIT_BPS_THRESHOLD:
                        signal_kind = "US_BUY" if cb_bn_bps >= EMIT_BPS_THRESHOLD else \
                                       "US_SELL" if cb_bn_bps <= -EMIT_BPS_THRESHOLD else \
                                       "SPOT_PERP_BASIS"
                        head = (f"PREMIUM cb→bn {cb_bn_bps:+.1f}bps  "
                                f"bn→bb {bn_bb_bps:+.1f}bps  "
                                f"cb=${cb:,.1f} bn=${bn:,.1f} bb=${bb:,.1f}  "
                                f"signal={signal_kind}")
                        emit_swarm("market.premium", head, {
                            **snap,
                            "type":       "PREMIUM_DELTA",
                            "signal":     signal_kind,
                            "confidence": 90,
                        }, ["market", "premium", signal_kind])
                        print(f"  [PREMIUM] {head}", flush=True)
                else:
                    print(f"  [poll] partial: cb={cb} bn={bn} bb={bb}", flush=True)
                    _metrics["polls_partial"] += 1
            except Exception as e:
                print(f"  ! poll err: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_poll = now

        if now - last_save >= 120:
            try:
                save_state(state)
            except Exception as e:
                print(f"  ! save err: {e}", file=sys.stderr, flush=True)
            last_save = now

        time.sleep(2)

    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
