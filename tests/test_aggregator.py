import sqlite3
import time
import os
import json
import pytest
from unittest.mock import patch
import sys
import pathlib

# Add repository root to Python path so we can import daemons
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from daemons.sygnif_aggregator import calculate_weighted_sentiment, SIGNAL_LOGIC, setup_db

@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "swarm.db"

    # We must patch DB_PATH in the module
    with patch("daemons.sygnif_aggregator.DB_PATH", str(db_file)):
        # Create schema expected by tests
        conn = sqlite3.connect(db_file)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS swarm_entries (
                topic TEXT,
                meta TEXT,
                created INTEGER
            )
        """)
        conn.commit()
        conn.close()

        # also create the weighted_signals table for tests that might call setup_db
        setup_db()

        yield str(db_file)

def test_decay_at_t0_is_1(temp_db):
    now = int(time.time())

    conn = sqlite3.connect(temp_db)
    # topic "market.premium" with base_weight 20. condition m["cb_bn_bps"] > 5 gives 1
    # decay = 1 at age=0, so total_score should be 1 * 20 * 1 = 20.
    # confluence = 1, multiplier = 1 + 0.15*1 = 1.15
    # final_score = 20 * 1.15 = 23.0
    conn.execute("INSERT INTO swarm_entries (topic, meta, created) VALUES (?, ?, ?)",
                 ("market.premium", json.dumps({"cb_bn_bps": 10}), now))
    conn.commit()
    conn.close()

    with patch("time.time", return_value=now):
        score, confluence, details = calculate_weighted_sentiment()

    assert abs(score - 23.0) < 0.01

def test_decay_at_30min_is_0_5(temp_db):
    now = int(time.time())
    thirty_mins_ago = now - 1800

    conn = sqlite3.connect(temp_db)
    # base_weight = 20
    # decay = 0.5 at age=1800
    # total_score = 1 * 20 * 0.5 = 10
    # multiplier = 1.15
    # final_score = 11.5
    conn.execute("INSERT INTO swarm_entries (topic, meta, created) VALUES (?, ?, ?)",
                 ("market.premium", json.dumps({"cb_bn_bps": 10}), thirty_mins_ago))
    conn.commit()
    conn.close()

    with patch("time.time", return_value=now):
        score, confluence, details = calculate_weighted_sentiment()

    assert abs(score - 11.5) < 0.01

def test_cluster_short_liq_scores_positive(temp_db):
    # Just test the lambda directly
    logic_fn, base_weight = SIGNAL_LOGIC["xchg.liquidation_cluster"]
    assert logic_fn({"side": "SHORT_LIQ", "n_exchanges": 2}) == 1
    assert logic_fn({"side": "SHORT_LIQ", "n_exchanges": 1}) == 0

def test_cluster_long_liq_scores_negative(temp_db):
    logic_fn, base_weight = SIGNAL_LOGIC["xchg.liquidation_cluster"]
    assert logic_fn({"side": "LONG_LIQ", "n_exchanges": 2}) == -1
    assert logic_fn({"side": "LONG_LIQ", "n_exchanges": 1}) == 0

def test_dormancy_unknown_category_scores_zero(temp_db):
    logic_fn, base_weight = SIGNAL_LOGIC["chain.dormancy_break"]

    assert logic_fn({"category": "DEPOSIT_TO_EXCHANGE"}) == -1
    assert logic_fn({"category": "ACCUMULATION_TO_COLD"}) == 1
    assert logic_fn({"category": "WITHDRAWAL_FROM_EXCHANGE"}) == 1
    assert logic_fn({"category": "UNKNOWN_OR_MISSING"}) == 0
    assert logic_fn({}) == 0
