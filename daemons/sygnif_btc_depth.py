#!/usr/bin/env python3
"""sygnif_btc_depth.py — Advanced Bitcoin Market Depth Intelligence.

Uses Arkham Intel API to track aggregate institutional holdings and flows.

Tracks:
  1. ETF Balances: BlackRock, Fidelity, Bitwise, Ark, Grayscale.
  2. Exchange Balances: Aggregate BTC across top 10 CEXs.
  3. Miner Balances: Aggregate known miner cluster holdings.

Frequency: Hourly snapshots.
Topics: btc.institutional_flow, btc.exchange_netflow, btc.miner_pressure
"""

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

try:
    import sygnif_common as common
except ImportError:
    from . import sygnif_common as common

# ============================================================================
# Config
# ============================================================================
DB_PATH      = "/var/lib/sygnif/swarm.db"
STATE_FILE   = pathlib.Path("/var/lib/sygnif/btc_depth_state.json")
POLL_S       = 3600  # 1 hour
ARKHAM_KEY   = os.environ.get("SYGNIF_ARKHAM_KEY", "")

# Arkham Entity IDs for tracking
ENTITIES = {
    "ETFs": {
        "BlackRock": "blackrock",
        "Fidelity": "fidelity",
        "Bitwise": "bitwise",
        "Ark Invest": "ark-invest",
        "Grayscale": "grayscale",
    },
    "Exchanges": {
        "Binance": "binance",
        "Coinbase": "coinbase",
        "OKX": "okx",
        "Kraken": "kraken",
        "Bitfinex": "bitfinex",
    },
    "Miners": {
        "Foundry USA": "foundry-usa",
        "AntPool": "antpool",
        "F2Pool": "f2pool",
        "ViaBTC": "viabtc",
    }
}

_running = True
_metrics = defaultdict(int)

# ============================================================================
# Arkham API Helpers
# ============================================================================
def fetch_entity_btc_balance(entity_id: str) -> float:
    """Fetch BTC balance for an entity via Arkham."""
    if not ARKHAM_KEY:
        return 0.0
    url = f"https://api.arkm.com/balances/entity/{entity_id}?chain=bitcoin"
    try:
        req = urllib.request.Request(url, headers={
            "API-Key": ARKHAM_KEY,
            "User-Agent": "sygnif-btc-depth/1.0"
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            # Arkham returns list of tokens; find BTC
            for item in data:
                if item.get("tokenName") == "Bitcoin" or item.get("symbol") == "BTC":
                    return float(item.get("balance", 0))
    except Exception as e:
        print(f"  ! Arkham balance fetch failed for {entity_id}: {e}", file=sys.stderr)
    return 0.0

# ============================================================================
# Swarm Emission
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
             "sygnif-btc-depth", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm emit failed: {e}", file=sys.stderr, flush=True)

# ============================================================================
# State
# ============================================================================
def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"last_snapshot": {}, "history": []}
    try:
        return json.loads(STATE_FILE.read_text())
    except:
        return {"last_snapshot": {}, "history": []}

def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    state["history"] = state["history"][-720:] # 30 days of hourly
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ============================================================================
# Core Logic
# ============================================================================
def run_cycle(state: dict):
    print(f"--- Snapshot @ {dt.datetime.now()} ---")
    current_snap = {"ts": int(time.time()), "balances": {}}

    for category, members in ENTITIES.items():
        cat_total = 0.0
        for name, eid in members.items():
            bal = fetch_entity_btc_balance(eid)
            current_snap["balances"][f"{category}.{name}"] = bal
            cat_total += bal
            time.sleep(1.0) # respect rate limit
        current_snap["balances"][f"{category}.TOTAL"] = cat_total

    # Compare with last
    prev = state.get("last_snapshot", {}).get("balances", {})
    if prev:
        # ETF Net Flow
        etf_delta = current_snap["balances"]["ETFs.TOTAL"] - prev.get("ETFs.TOTAL", 0)
        if abs(etf_delta) >= 10: # 10 BTC threshold
            head = f"BTC_INSTITUTIONAL_FLOW: {etf_delta:+.1f} BTC across all ETFs"
            emit_swarm("btc.institutional_flow", head, {
                "type": "ETF_NET_FLOW",
                "delta": round(etf_delta, 2),
                "current_total": round(current_snap["balances"]["ETFs.TOTAL"], 1),
                "confidence": 95
            }, ["btc", "etf", "institutional"])
            print(f"  [ETF] {head}")

        # Exchange Net Flow
        exch_delta = current_snap["balances"]["Exchanges.TOTAL"] - prev.get("Exchanges.TOTAL", 0)
        if abs(exch_delta) >= 50:
            head = f"BTC_EXCHANGE_NETFLOW: {exch_delta:+.1f} BTC (inflow = bearish)"
            emit_swarm("btc.exchange_netflow", head, {
                "type": "EXCHANGE_NET_FLOW",
                "delta": round(exch_delta, 2),
                "current_total": round(current_snap["balances"]["Exchanges.TOTAL"], 1),
                "confidence": 90
            }, ["btc", "exchange", "flow"])
            print(f"  [EXCH] {head}")

        # Miner Pressure
        miner_delta = current_snap["balances"]["Miners.TOTAL"] - prev.get("Miners.TOTAL", 0)
        if miner_delta < -10:
            head = f"BTC_MINER_PRESSURE: {abs(miner_delta):.1f} BTC distributed by pools"
            emit_swarm("btc.miner_pressure", head, {
                "type": "MINER_DISTRIBUTION",
                "delta": round(miner_delta, 2),
                "confidence": 85
            }, ["btc", "miner", "pressure"])
            print(f"  [MINER] {head}")

    state["last_snapshot"] = current_snap
    state["history"].append(current_snap)
    save_state(state)

def main():
    global _running
    if not ARKHAM_KEY:
        print("Error: SYGNIF_ARKHAM_KEY not set. BTC Depth intelligence requires Arkham API.")
        return

    state = load_state()

    def _sigterm(sig, frame):
        global _running
        _running = False
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT,  _sigterm)

    while _running:
        try:
            run_cycle(state)
        except Exception as e:
            print(f"Cycle error: {e}")

        if not _running: break
        time.sleep(POLL_S)

if __name__ == "__main__":
    main()
