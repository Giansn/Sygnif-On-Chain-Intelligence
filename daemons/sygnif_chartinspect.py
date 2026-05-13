#!/usr/bin/env python3
"""sygnif_chartinspect.py — ChartInspect macro-indicator daemon.

Polls the ChartInspect API for macroeconomic indicators, computes historical percentiles
using an in-memory ring buffer, and emits swarm events.
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
import urllib.error
import urllib.request
import uuid
from collections import defaultdict

DB_PATH    = "/var/lib/sygnif/swarm.db"
STATE_FILE = pathlib.Path("/var/lib/sygnif/chartinspect_history.json")

# Limits
POLL_S        = 300  # 5 minutes
WINDOW_DAYS   = 90

# API setup
API_BASE_URL = "https://chartinspect.com/api/v1/economic/"
API_KEY      = os.environ.get("CHARTINSPECT_API_KEY", "")
HEADERS      = {
    "User-Agent": "sygnif-chartinspect/1.0",
    "x-api-key":  API_KEY,
}

ENDPOINTS = [
    "interest_fed",
    "money_m2",
    "money_m2_global",
    "inflation_cpi",
    "rates_yield_spread_10y2y",
    "sp500",
    "dxy",
    "nasdaq",
]

_running = True
_metrics = defaultdict(int)
_metrics["started_at"] = time.time()


def emit_swarm(topic, content, meta, tags):
    if not os.path.exists(DB_PATH): return
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        rid = str(uuid.uuid4())
        # swarm schema uses `created` for the unix timestamp
        c.execute(
            "INSERT OR IGNORE INTO swarm_entries "
            "(id, created, swarm_id, agent_id, topic, content, meta, tags) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rid, int(time.time()), "trading",
             "sygnif-chartinspect", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm err: {e}", file=sys.stderr, flush=True)


def load_state():
    if not STATE_FILE.exists():
        return {"schema": "sygnif.chartinspect.v1",
                "buffers": {e: [] for e in ENDPOINTS}}
    try:
        data = json.loads(STATE_FILE.read_text())
        if "buffers" not in data:
            data["buffers"] = {e: [] for e in ENDPOINTS}
        # Ensure all endpoints exist
        for e in ENDPOINTS:
            if e not in data["buffers"]:
                data["buffers"][e] = []
        return data
    except (json.JSONDecodeError, OSError):
        return {"schema": "sygnif.chartinspect.v1",
                "buffers": {e: [] for e in ENDPOINTS}}


def save_state(state):
    state["updated_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    # Prune buffers older than WINDOW_DAYS
    cutoff_ts = time.time() - (WINDOW_DAYS * 86400)
    for k in state["buffers"].keys():
        state["buffers"][k] = [x for x in state["buffers"][k] if x["ts"] >= cutoff_ts]

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, default=str, indent=2))
    os.replace(tmp, STATE_FILE)


def fetch_indicator(endpoint):
    url = f"{API_BASE_URL}{endpoint}"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            _metrics["rate_limited"] += 1
            raise  # Let the main loop handle the sleep
        if e.code == 404:
            print(f"  [warn] 404 Not Found for {endpoint}", flush=True)
            return None
        _metrics["errors"] += 1
        raise
    except Exception as e:
        _metrics["errors"] += 1
        raise


def compute_percentile(val, history):
    # history is list of dicts: {"ts": ..., "value": ...}
    if not history:
        return 50  # Default if no history
    
    values = [x["value"] for x in history]
    values.append(val)
    values.sort()
    
    # Calculate rank
    rank = values.index(val)
    n = len(values)
    
    if n == 1: return 50
    return int((rank / (n - 1)) * 100)


def main():
    global _running
    print(f"=== sygnif_chartinspect started @ "
          f"{dt.datetime.now(dt.timezone.utc).isoformat()} ===", flush=True)

    state = load_state()

    def _sigterm(sig, frame):
        global _running
        print(f"  signal {sig}", flush=True)
        _running = False
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT,  _sigterm)

    last_hb = time.time()
    
    while _running:
        cycle_start = time.time()
        
        for ep in ENDPOINTS:
            if not _running: break
            
            try:
                data = fetch_indicator(ep)
                _metrics["fetched"] += 1
                
                if data and "value" in data:
                    val = float(data["value"])
                    unit = data.get("unit", "")
                    
                    history = state["buffers"][ep]
                    pct = compute_percentile(val, history)
                    
                    # Update buffer
                    now = time.time()
                    history.append({"ts": now, "value": val})
                    
                    # Emit swarm
                    head = f"{ep} = {val} (P{pct})"
                    meta = {
                        "indicator": ep,
                        "value": val,
                        "percentile": pct,
                        "ts": int(now),
                        "unit": unit
                    }
                    emit_swarm("chart.indicator", head, meta, ["chart", "macro", ep])
                    
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    print("  [warn] HTTP 429 Rate Limited. Sleeping 60s.", flush=True)
                    time.sleep(60)
                else:
                    print(f"  [error] HTTP {e.code} for {ep}: {e}", file=sys.stderr, flush=True)
                    time.sleep(30)
            except Exception as e:
                print(f"  [error] Fetch failed for {ep}: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
                time.sleep(30)
                
            time.sleep(1) # Small sleep between endpoints to spread out requests slightly

        try:
            save_state(state)
        except Exception as e:
            print(f"  ! save err: {e}", file=sys.stderr, flush=True)

        now = time.time()
        
        # Heartbeat every 5 minutes (which is our cycle time anyway)
        if now - last_hb >= 300:
            print(f"[CHARTINSPECT HB] fetched={_metrics['fetched']} "
                  f"errors={_metrics['errors']} rate_limited={_metrics['rate_limited']}", 
                  flush=True)
            last_hb = now

        # Sleep until the next 5-minute interval
        elapsed = time.time() - cycle_start
        sleep_time = max(0, POLL_S - elapsed)
        
        # Break sleep into smaller chunks to remain responsive to signals
        while sleep_time > 0 and _running:
            time.sleep(min(1.0, sleep_time))
            sleep_time -= 1.0

    save_state(state)
    return 0

if __name__ == "__main__":
    sys.exit(main())
