#!/usr/bin/env python3
"""sygnif_health.py — Check health and freshness of SYGNIF intelligence daemons.

Scans the state files in /var/lib/sygnif/ and verifies:
  1. All expected state files exist.
  2. State files have been updated recently (heartbeat check).
  3. Optional: Systemd units are active (requires sudo/systemctl).

Output: JSON health report or human-readable summary.
"""
import datetime as dt
import json
import os
import pathlib
import sys
import time

STATE_DIR = pathlib.Path("/var/lib/sygnif")

DAEMONS = {
    "chain_intel":       "chain_state.json",
    "evm_signals":       "evm_state.json",
    "tron_signals":      "tron_state.json",
    "xchg_liquidations": "xchg_liq_state.json",
    "market_premium":    "market_premium.json",
    "evm_extras":        "evm_extras_state.json",
    "ecosystem":         "ecosystem_state.json",
}

# Thresholds for "freshness" (seconds)
FRESH_THRESHOLD = {
    "chain_intel":       300,   # 5 min (block/mempool poll)
    "evm_signals":       900,   # 15 min (mints are 5 min, reserves 1h)
    "tron_signals":      600,   # 10 min (5 min poll)
    "xchg_liquidations": 300,   # 5 min (1 min save)
    "market_premium":    300,   # 5 min (1 min poll)
    "evm_extras":        1200,  # 20 min (10 min poll)
    "ecosystem":         1200,  # 20 min (5-15 min polls)
}

def get_file_age(path: pathlib.Path) -> float:
    if not path.exists():
        return float('inf')
    return time.time() - path.stat().st_mtime

import sqlite3

def check_health():
    report = {
        "ts": int(time.time()),
        "ts_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "overall_status": "OK",
        "daemons": {},
    }
    
    any_stale = False
    any_missing = False

    # Check aggregator health via DB
    db_path = "/var/lib/sygnif/swarm.db"
    aggregator_status = "OK"
    agg_age = float('inf')

    if not os.path.exists(db_path):
        aggregator_status = "MISSING"
        any_missing = True
    else:
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT MAX(ts) FROM weighted_signals")
            row = cur.fetchone()
            if row and row[0]:
                agg_age = time.time() - row[0]
                if agg_age > 180:
                    aggregator_status = "STALE"
                    any_stale = True
            else:
                aggregator_status = "MISSING"
                any_missing = True
            conn.close()
        except sqlite3.Error:
            aggregator_status = "MISSING"
            any_missing = True

    report["daemons"]["aggregator"] = {
        "file": "swarm.db (weighted_signals)",
        "age_s": round(agg_age, 1) if agg_age != float('inf') else None,
        "threshold_s": 180,
        "status": aggregator_status,
    }

    for name, filename in DAEMONS.items():
        path = STATE_DIR / filename
        age = get_file_age(path)
        threshold = FRESH_THRESHOLD.get(name, 600)
        
        status = "OK"
        if age == float('inf'):
            status = "MISSING"
            any_missing = True
        elif age > threshold:
            status = "STALE"
            any_stale = True
            
        report["daemons"][name] = {
            "file": filename,
            "age_s": round(age, 1) if age != float('inf') else None,
            "threshold_s": threshold,
            "status": status,
        }

    if any_missing:
        report["overall_status"] = "CRITICAL"
    elif any_stale:
        report["overall_status"] = "WARNING"
        
    return report

def main():
    report = check_health()
    
    if "--json" in sys.argv:
        print(json.dumps(report, indent=2))
        return 0 if report["overall_status"] == "OK" else 1

    print(f"\n=== SYGNIF Health Report @ {report['ts_utc']} ===")
    print(f"Overall Status: {report['overall_status']}")
    print(f"{'-'*50}")
    print(f"{'Daemon':<20} {'Status':<10} {'Age':<10} {'Threshold'}")
    
    for name, data in report["daemons"].items():
        age_str = f"{data['age_s']:.0f}s" if data['age_s'] is not None else "N/A"
        print(f"{name:<20} {data['status']:<10} {age_str:<10} {data['threshold_s']}s")
    
    print(f"{'-'*50}")
    if report["overall_status"] != "OK":
        print("ALERT: One or more daemons may be down or stuck!")
        return 1
    else:
        print("All intelligence daemons are healthy.")
        return 0

if __name__ == "__main__":
    sys.exit(main())
