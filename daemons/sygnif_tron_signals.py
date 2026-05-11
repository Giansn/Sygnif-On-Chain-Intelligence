#!/usr/bin/env python3
"""sygnif_tron_signals.py — Tron-side stablecoin mint tracker.

Tether mints MORE on Tron than Ethereum on most days. This daemon polls
the Tether-Tron treasury and emits a swarm event for every outflow above
threshold (default $10M).

Endpoint: TronGrid public v1 (no key required, key boosts rate limit).
Treasury: TKHuVq1oKVruCGLvqVexFs6dawKv6fQgFs
USDT TRC20 contract: TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t

State file: /var/lib/sygnif/tron_state.json
Swarm topic emitted: tron.stablecoin_mint
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
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict

# ============================================================================
# Config
# ============================================================================
TRON_API_KEY = os.environ.get("SYGNIF_TRON_KEY", "")
MINT_THRESHOLD_USD = float(os.environ.get("SYGNIF_TRON_MINT_USD", "10000000"))   # $10M
POLL_INTERVAL_S    = float(os.environ.get("SYGNIF_TRON_POLL_S", "300"))   # 5min

TRONGRID = "https://api.trongrid.io"
DB_PATH  = "/var/lib/sygnif/swarm.db"
STATE_FILE = pathlib.Path("/var/lib/sygnif/tron_state.json")

# Tether Tron Treasury & contract
TETHER_TRON_TREASURY = "TKHuVq1oKVruCGLvqVexFs6dawKv6fQgFs"
USDT_TRC20_CONTRACT  = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
USDD_TRC20_CONTRACT  = "TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn"   # Tron's USDD
USDC_TRC20_CONTRACT  = "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8"

# Known Tron exchange hot wallets (for receiver classification)
TRON_EXCHANGE_WALLETS = {
    "TEMR6WoasFkM8qYQXVDVDsd2pE9w2nyAZv": "Binance (Tron hot)",  # frequently receives mints
    "TWxhucvVoGHk2JhaPhFWDiUjQ2k7sk1MnP": "Binance (Tron alt)",
    "TQFRLF5YdXA1L8TVvz2xea7B7QzmcJYtfm": "Binance (Tron alt 2)",
    "TZ9kTy7mFc8bR4idaXgUWT4xqg3WkFwc1f": "Bitfinex (Tron)",
    "TVMrLZUcZhdSifFpbiZEWqfBVe1Br3VKAQ": "Bitfinex (Tron alt)",
    "TNzwdty5a4gEDHeRGXCLSz5j7Y2k3UvxxM": "Huobi (Tron)",
    "TEYsBwNGhEU3TWZusYRjne7t5dskJYwBhd": "OKX (Tron)",
}

HEADERS = {"User-Agent": "sygnif-tron-signals/1.0"}
# TronGrid key not yet provisioned — public v1 works without authif False and TRON_API_KEY:    HEADERS["TRON-PRO-API-KEY"] = TRON_API_KEY

_running = True
_metrics = defaultdict(int)
_metrics["started_at"] = time.time()


# ============================================================================
# HTTP
# ============================================================================
def _http_get_json(url: str, timeout: int = 15):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        _metrics["http_failures"] += 1
        print(f"  ! GET {url[:80]} — {type(e).__name__}: {e}",
              file=sys.stderr, flush=True)
        return None


def fetch_treasury_outflows(contract: str, limit: int = 50) -> list:
    """Recent transfers of `contract` token FROM Tether Treasury → external.
    These are mints being deployed to depositors."""
    qs = urllib.parse.urlencode({
        "limit":             str(limit),
        "only_from":         "true",
        "contract_address":  contract,
    })
    url = (f"{TRONGRID}/v1/accounts/{TETHER_TRON_TREASURY}"
           f"/transactions/trc20?{qs}")
    r = _http_get_json(url)
    _metrics["api_calls"] += 1
    if not r:
        return []
    if not r.get("success"):
        return []
    return r.get("data") or []


def classify_receiver(addr: str) -> tuple[str, int]:
    """Returns (entity_label, confidence_0_100) for a Tron address."""
    if addr in TRON_EXCHANGE_WALLETS:
        return (TRON_EXCHANGE_WALLETS[addr], 90)
    return ("unlabeled", 30)


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
        "schema":           "sygnif.tron_signals.v1",
        "created_at_utc":   dt.datetime.now(dt.timezone.utc).isoformat(),
        "seen_tx_ids":      [],     # last 500 tx ids we've already emitted
        "recent_mints":     [],     # last 200 mint events emitted
        "last_high_water_ts": 0,    # latest block_timestamp seen (ms)
        "metrics":          {},
    }


def save_state(state: dict) -> None:
    state["updated_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    state["seen_tx_ids"]    = state["seen_tx_ids"][-500:]
    state["recent_mints"]   = state["recent_mints"][-200:]
    state["metrics"] = dict(_metrics)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, default=str, indent=2))
    os.replace(tmp, STATE_FILE)


# ============================================================================
# Swarm
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
             "sygnif-tron-signals", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm emit failed: {type(e).__name__}: {e}",
              file=sys.stderr, flush=True)


# ============================================================================
# Scan
# ============================================================================
def scan_token_mints(state: dict, token_symbol: str, contract: str) -> int:
    """Pull recent outflows of token from Tether Treasury. Emit mints above threshold."""
    seen = set(state["seen_tx_ids"])
    new_events = 0
    txs = fetch_treasury_outflows(contract, limit=50)

    for tx in txs:
        txid = tx.get("transaction_id")
        if not txid or txid in seen:
            continue
        seen.add(txid)
        state["seen_tx_ids"].append(txid)

        # Direction filter: only outgoing from treasury
        if tx.get("from") != TETHER_TRON_TREASURY:
            continue
        try:
            raw_value = int(tx.get("value", 0))
        except (ValueError, TypeError):
            continue
        amount_usd = raw_value / 1e6   # USDT/USDC/USDD all 6 decimals on Tron
        if amount_usd < MINT_THRESHOLD_USD:
            continue

        ts_ms = int(tx.get("block_timestamp", 0))
        ts_s  = ts_ms // 1000
        receiver = tx.get("to", "")
        entity, conf = classify_receiver(receiver)

        ev = {
            "ts":           ts_s,
            "ts_utc":       dt.datetime.fromtimestamp(ts_s, dt.timezone.utc).isoformat(),
            "chain":        "TRON",
            "token":        token_symbol,
            "amount_usd":   round(amount_usd, 0),
            "txid":         txid,
            "from":         tx.get("from"),
            "to":           receiver,
            "to_entity":    entity,
            "to_entity_conf": conf,
        }
        state["recent_mints"].append(ev)
        state["last_high_water_ts"] = max(state["last_high_water_ts"], ts_ms)
        new_events += 1
        _metrics[f"mints_seen_{token_symbol}"] += 1

        head = (f"TRON_{token_symbol}_MINT {amount_usd/1e6:,.0f}M USD  "
                f"treasury → {entity}  "
                f"({receiver[:14]}...)  tx={txid[:12]}")
        emit_swarm("tron.stablecoin_mint", head, {
            **ev,
            "type":       f"TRON_{token_symbol}_TREASURY_OUT",
            "confidence": 90,
        }, ["tron", "stablecoin", "mint", token_symbol])
        print(f"  [TRON-MINT] {head}", flush=True)

    return new_events


# ============================================================================
# Main loop
# ============================================================================
def main() -> int:
    global _running
    print(f"=== sygnif_tron_signals started @ "
          f"{dt.datetime.now(dt.timezone.utc).isoformat()} ===", flush=True)
    print(f"  api key:       {'OK (TronGrid Pro)' if TRON_API_KEY else 'PUBLIC v1 only'}",
          flush=True)
    print(f"  threshold:     ${MINT_THRESHOLD_USD/1e6:.0f}M per mint event",
          flush=True)
    print(f"  poll:          {POLL_INTERVAL_S}s", flush=True)

    state = load_state()
    print(f"  loaded:        {len(state.get('seen_tx_ids',[]))} seen ids, "
          f"{len(state.get('recent_mints',[]))} recent mints", flush=True)

    def _sigterm(sig, frame):
        global _running
        print(f"  signal {sig}, shutting down", flush=True)
        _running = False
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT,  _sigterm)

    last_poll = 0.0
    last_save = 0.0
    while _running:
        now = time.time()
        if now - last_poll >= POLL_INTERVAL_S:
            try:
                n_usdt = scan_token_mints(state, "USDT", USDT_TRC20_CONTRACT)
                time.sleep(1)
                n_usdc = scan_token_mints(state, "USDC", USDC_TRC20_CONTRACT)
                time.sleep(1)
                n_usdd = scan_token_mints(state, "USDD", USDD_TRC20_CONTRACT)
                total = n_usdt + n_usdc + n_usdd
                if total:
                    print(f"  → {total} new Tron mint events this poll", flush=True)
            except Exception as e:
                print(f"  ! poll error: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_poll = now

        if now - last_save >= 180:
            try:
                save_state(state)
            except Exception as e:
                print(f"  ! save error: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_save = now

        time.sleep(5.0)

    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
