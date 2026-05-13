#!/usr/bin/env python3
"""sygnif_signal_aggregator.py — Conceptual signal weighting example.

Demonstrates how to aggregate diverse swarm.db topics into a single
weighted sentiment score (-100 to +100).
"""
import json
import sqlite3
import time
from collections import defaultdict

DB_PATH = "/var/lib/sygnif/swarm.db"

# Logic for mapping topics to sentiment impact
SIGNAL_MAP = {
    "market.premium": {
        "bullish": lambda m: m.get("cb_bn_bps", 0) > 5,
        "bearish": lambda m: m.get("cb_bn_bps", 0) < -5,
        "score": 20
    },
    "tron.stablecoin_mint": {
        "bullish": lambda m: m.get("amount_usd", 0) >= 100_000_000,
        "score": 15
    },
    "xchg.liquidation_cluster": {
        "bullish": lambda m: m.get("side") == "SHORT_LIQ" and m.get("n_exchanges", 0) >= 3,
        "bearish": lambda m: m.get("side") == "LONG_LIQ" and m.get("n_exchanges", 0) >= 3,
        "score": 25
    },
    "chain.dormancy_break": {
        "bearish": lambda m: True,  # Old BTC moving is usually bearish pressure
        "score": 35
    }
}

def get_recent_signals(lookback_s: int = 3600):
    """Query swarm.db for events in the last hour."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        since = int(time.time()) - lookback_s
        cur.execute(
            "SELECT topic, meta, created FROM swarm_entries WHERE created > ?",
            (since,)
        )
        return cur.fetchall()
    except Exception as e:
        print(f"Error reading swarm.db: {e}")
        return []

def aggregate_sentiment():
    signals = get_recent_signals()
    total_score = 0.0
    active_sources = set()

    for row in signals:
        topic = row["topic"]
        if topic not in SIGNAL_MAP:
            continue

        try:
            meta = json.loads(row["meta"])
        except:
            continue

        mapping = SIGNAL_MAP[topic]
        base_score = mapping["score"]

        # Check bullish logic
        if "bullish" in mapping and mapping["bullish"](meta):
            total_score += base_score
            active_sources.add(topic)
        # Check bearish logic
        elif "bearish" in mapping and mapping["bearish"](meta):
            total_score -= base_score
            active_sources.add(topic)

    # Apply time-decay (conceptual) and confluence multiplier
    confluence_multiplier = 1 + (0.1 * len(active_sources))
    final_score = total_score * confluence_multiplier

    # Clip to bounds
    return max(-100, min(100, final_score))

def main():
    print("=== SYGNIF Signal Aggregator (Conceptual) ===")
    score = aggregate_sentiment()
    print(f"Current Weighted Sentiment: {score:+.2f}")

    if score > 60:
        print("Signal: STRONG BUY - High confluence across sources.")
    elif score < -60:
        print("Signal: STRONG SELL - Large-scale distribution detected.")
    else:
        print("Signal: NEUTRAL - No high-conviction cluster.")

if __name__ == "__main__":
    main()
