#!/usr/bin/env python3
"""sygnif_aggregator.py — The Signal Aggregator (Brain).

This daemon turns raw swarm.db events into a single weighted sentiment score.
It runs every 60s, querying the last hour of activity.

Weighted Logic:
- Time Decay: Signals lose 50% power every 30 minutes.
- Confluence: Rewards signals appearing across different layers (Chain + Market).
- Topic Weights: Assigns base scores to specific events (Mints, Premiums, etc).
"""

import json
import sqlite3
import time
import os
import pathlib
import datetime as dt
from collections import defaultdict

DB_PATH = "/var/lib/sygnif/swarm.db"
POLL_S  = 60

# Topic -> (Logic Function, Base Score)
# Each logic function receives the 'meta' dict and returns -1, 0, or 1
SIGNAL_LOGIC = {
    "market.premium": (
        lambda m: 1 if m.get("cb_bn_bps", 0) > 5 else (-1 if m.get("cb_bn_bps", 0) < -5 else 0),
        20
    ),
    "tron.stablecoin_mint": (
        lambda m: 1 if m.get("amount_usd", 0) >= 100_000_000 else 0,
        15
    ),
    "evm.stablecoin_mint": (
        lambda m: 1 if m.get("amount_usd", 0) >= 50_000_000 else 0,
        15
    ),
    "xchg.liquidation_cluster": (
        lambda m: 1 if m.get("side") == "SHORT_LIQ" and m.get("n_exchanges", 0) >= 2 else (-1 if m.get("side") == "LONG_LIQ" and m.get("n_exchanges", 0) >= 2 else 0),
        30
    ),
    "chain.dormancy_break": (
        lambda m: (-1 if m.get("category") == "DEPOSIT_TO_EXCHANGE" else
                   (1 if m.get("category") in ("ACCUMULATION_TO_COLD", "WITHDRAWAL_FROM_EXCHANGE") else 0)),
        40
    ),
    "chain.whale": (
        lambda m: 1 if m.get("category") in ("WITHDRAWAL_FROM_EXCHANGE", "ACCUMULATION_TO_COLD") else (-1 if m.get("category") == "DEPOSIT_TO_EXCHANGE" else 0),
        10
    )
}

def setup_db():
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS weighted_signals (
                id TEXT PRIMARY KEY,
                ts INTEGER,
                ts_utc TEXT,
                pair TEXT,
                score REAL,
                confluence INTEGER,
                meta TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS weighted_signals_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                ts_utc TEXT,
                pair TEXT,
                score REAL,
                confluence INTEGER,
                meta TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_weighted_signals_history_ts ON weighted_signals_history(ts)")
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"DB Setup error: {e}")
        return False

def calculate_weighted_sentiment(lookback_s: int = 3600):
    """Query swarm.db, apply decay and weights, return aggregated score."""
    now = int(time.time())
    since = now - lookback_s
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT topic, meta, created FROM swarm_entries WHERE created > ?",
            (since,)
        )
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"Query error: {e}")
        return 0.0, 0, {}

    total_score = 0.0
    active_topics = set()
    topic_counts = defaultdict(int)
    
    for row in rows:
        topic = row["topic"]
        if topic not in SIGNAL_LOGIC:
            continue
            
        try:
            meta = json.loads(row["meta"])
        except:
            continue
            
        logic_fn, base_weight = SIGNAL_LOGIC[topic]
        direction = logic_fn(meta)
        
        if direction == 0:
            continue
            
        # Apply Exponential Time Decay
        # (Signals lose 50% power every 30 minutes, half-life = 1800s)
        age_s = now - row["created"]
        decay = 0.5 ** (age_s / 1800.0)
        
        contribution = direction * base_weight * decay
        total_score += contribution
        active_topics.add(topic)
        topic_counts[topic] += 1

    # Confluence Multiplier: setup is more reliable if signals come from multiple sources
    # 1 source = 1.0x, 3 sources = 1.3x
    confluence = len(active_topics)
    multiplier = 1 + (0.15 * confluence)
    final_score = total_score * multiplier
    
    # Normalize to -100 / +100
    normalized = max(-100, min(100, final_score))
    
    return normalized, confluence, dict(topic_counts)

def main():
    print(f"=== SYGNIF Aggregator started @ {dt.datetime.now()} ===")
    if not setup_db():
        print(f"Error: Could not access {DB_PATH}. Ensure intel daemons are running.")
        return

    while True:
        try:
            score, confluence, details = calculate_weighted_sentiment()
            now = int(time.time())
            now_utc = dt.datetime.now(dt.timezone.utc).isoformat()
            
            # Persist the "Brain State"
            conn = sqlite3.connect(DB_PATH)

            # Retention sweep
            conn.execute("DELETE FROM weighted_signals_history WHERE ts < strftime('%s','now') - 86400*7")

            conn.execute(
                "INSERT OR REPLACE INTO weighted_signals (id, ts, ts_utc, pair, score, confluence, meta) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("BTC_GLOBAL", now, now_utc, "BTCUSDT", round(score, 2), confluence, json.dumps(details))
            )
            conn.execute(
                "INSERT INTO weighted_signals_history (ts, ts_utc, pair, score, confluence, meta) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (now, now_utc, "BTCUSDT", round(score, 2), confluence, json.dumps(details))
            )
            conn.commit()
            conn.close()
            
            print(f"[{now_utc}] Weighted Score: {score:+.2f} | Confluence: {confluence} | Topics: {len(details)}")
            
        except Exception as e:
            print(f"Aggregator Loop Error: {e}")
            
        time.sleep(POLL_S)

if __name__ == "__main__":
    main()
