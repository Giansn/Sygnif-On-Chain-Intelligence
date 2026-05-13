#!/usr/bin/env python3
"""sygnif_normalize.py — Glassnode-style normalization daemon.

Computes rolling 30-day z-score and percentile normalizations for core
flow metrics, transforming raw values into standardized regime signals.
"""

import sqlite3
import json
import time
import math
import statistics
import os
import uuid
import traceback
import sys

DB_PATH = "/var/lib/sygnif/swarm.db"
POLL_INTERVAL = 300  # 5 minutes
WINDOW_DAYS = 30
WINDOW_SECONDS = WINDOW_DAYS * 86400
BUCKET_SECONDS = 3600  # 1 hour

TOPICS = {
    "evm.exchange_reserve": {"field": "$.delta", "agg": "sum"},
    "evm.stablecoin_mint": {"field": "$.amount", "agg": "sum"},
    "tron.stablecoin_mint": {"field": "$.amount", "agg": "sum"},
    "market.premium": {"field": "$.cb_bn_bps", "agg": "mean"},
    "xchg.liquidation": {"field": "$.value_usd", "agg": "sum"},
}

def emit_swarm_event(topic: str, content: str, meta: dict) -> None:
    """Write normalized event to swarm.db."""
    if not os.path.exists(DB_PATH):
        return
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        # Enforce WAL mode as required by memory notes
        c.execute("PRAGMA journal_mode=WAL")
        rid = str(uuid.uuid4())
        c.execute(
            "INSERT OR IGNORE INTO swarm_entries "
            "(id, created, swarm_id, agent_id, topic, content, meta, tags) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rid, time.time(), "trading",
             "sygnif-normalize", topic, content,
             json.dumps(meta, default=str), "[]")
        )
        c.commit()
        c.close()
    except Exception as e:
        print(f"Error emitting swarm event: {e}", file=sys.stderr)

def get_percentile(data: list, value: float) -> int:
    """Compute percentile rank of value within data (0-100)."""
    if not data:
        return 0
    less_than = sum(1 for x in data if x < value)
    equal_to = sum(1 for x in data if x == value)
    rank = less_than + (0.5 * equal_to)
    return int(round((rank / len(data)) * 100))

def process_topic(topic: str, config: dict, db_path: str, current_time: float) -> tuple[int, int, int]:
    """Process a single topic. Returns (processed, skipped, error)."""
    field = config["field"]
    agg = config["agg"]
    
    cutoff_time = current_time - WINDOW_SECONDS
    
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Read all entries in the last 30 days for that topic from swarm_entries.
        # CRITICAL: extracting from `meta`, not `content`.
        cursor.execute(
            """
            SELECT created, json_extract(meta, ?) as val
            FROM swarm_entries
            WHERE topic = ? AND created >= ?
            """,
            (field, topic, cutoff_time)
        )
        rows = cursor.fetchall()
        conn.close()
    except Exception as e:
        print(f"Error querying {topic}: {e}", file=sys.stderr)
        return (0, 0, 1)

    if not rows:
        print(f"Zero entries for {topic} in the last 30 days, skipping.")
        return (0, 1, 0)

    # Bucket into 1-hour windows
    buckets = {}
    for row in rows:
        created = row["created"]
        val = row["val"]
        if val is None:
            continue
        try:
            val = float(val)
        except ValueError:
            continue
        
        bucket_ts = int(created) // BUCKET_SECONDS * BUCKET_SECONDS
        if bucket_ts not in buckets:
            buckets[bucket_ts] = []
        buckets[bucket_ts].append(val)

    if not buckets:
        print(f"No valid data points for {topic} after filtering, skipping.")
        return (0, 1, 0)

    # Aggregate per bucket
    aggregated_buckets = {}
    for ts, vals in buckets.items():
        if agg == "sum":
            aggregated_buckets[ts] = sum(vals)
        elif agg == "mean":
            aggregated_buckets[ts] = statistics.mean(vals)
            
    # Skip if the 30-day window has fewer than 24 buckets
    if len(aggregated_buckets) < 24:
        return (0, 1, 0)
        
    bucket_values = list(aggregated_buckets.values())
    
    # 30d_mean and 30d_std
    mean_val = statistics.mean(bucket_values)
    std_val = statistics.stdev(bucket_values) if len(bucket_values) > 1 else 0.0
    
    # The latest bucket
    latest_ts = max(aggregated_buckets.keys())
    latest_val = aggregated_buckets[latest_ts]
    
    if std_val == 0:
        zscore = 0.0
    else:
        zscore = (latest_val - mean_val) / std_val
        
    percentile = get_percentile(bucket_values, latest_val)
    
    norm_topic = f"norm.{topic}"
    content = f"{topic} z={zscore:.2f} P{percentile}"
    meta = {
        "source_topic": topic,
        "value": latest_val,
        "zscore": zscore,
        "percentile": percentile,
        "baseline_mean": mean_val,
        "baseline_std": std_val,
        "window_days": WINDOW_DAYS,
        "bucket_ts": latest_ts
    }
    
    emit_swarm_event(norm_topic, content, meta)
    
    return (1, 0, 0)

def main():
    print("Starting sygnif_normalize daemon...")
    while True:
        current_time = time.time()
        topics_count = len(TOPICS)
        processed_count = 0
        skipped_count = 0
        errors_count = 0
        
        if not os.path.exists(DB_PATH):
            print(f"Database {DB_PATH} not found, waiting...", file=sys.stderr)
        else:
            for topic, config in TOPICS.items():
                p, s, e = process_topic(topic, config, DB_PATH, current_time)
                processed_count += p
                skipped_count += s
                errors_count += e
        
        print(f"[NORMALIZE HB] topics={topics_count} processed={processed_count} skipped_insufficient={skipped_count} errors={errors_count}", flush=True)
        
        # Sleep for the remainder of the 5 minutes
        elapsed = time.time() - current_time
        sleep_time = max(0, POLL_INTERVAL - elapsed)
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
