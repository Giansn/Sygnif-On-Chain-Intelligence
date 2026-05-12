#!/usr/bin/env python3
"""sygnif_chain_intel.py — Deep BTC chain intelligence daemon (Phase 1).

Long-running daemon. Replaces sygnif_onchain_watcher with multi-track design:

  TRACK 1: Mempool monitor
    Polls mempool.space /api/mempool/recent every 15s.
    Detects whale-sized (>=50 BTC) pending txs ~10 min BEFORE confirmation.
    Emits early-warning events to swarm: topic "chain.mempool_whale"

  TRACK 2: Block scanner with UTXO-age + CIH clustering
    On each new block, fetches full block (blockchain.info /rawblock).
    For each whale tx:
      - Resolves all input UTXO ages (via mempool.space /api/tx/{prev_txid})
      - Computes LTH/STH split (155-day threshold), dormancy-break flag (>=5yr)
      - Applies common-input heuristic: addresses spending together = one entity
      - Detects peeling chains (FIFO same-source/same-dest pattern)
    Emits events: topic "chain.whale", "chain.dormancy_break", "chain.peeling"

  TRACK 3: Confidence-scored entity registry (Arkham-style)
    Every wallet has tier + confidence (0-100) + label + cluster_id.
    Clusters merge via union-find as new evidence appears.
    Sanctioned addresses (OFAC) are marked at score 100.

State files:
  /var/lib/sygnif/chain_state.json         — main registry
  /var/lib/sygnif/chain_mempool.json       — pending tx watchlist
  /var/lib/sygnif/sanctioned_addresses.txt — OFAC list (loaded at startup)

Backwards-compatible with sygnif_onchain_watcher.py state.
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
from typing import Iterable

# ============================================================================
# Config
# ============================================================================
THRESHOLD_BTC       = float(os.environ.get("SYGNIF_CHAIN_THRESHOLD_BTC", "50"))
MEMPOOL_POLL_S      = float(os.environ.get("SYGNIF_CHAIN_MEMPOOL_POLL_S", "15"))
BLOCK_POLL_S        = float(os.environ.get("SYGNIF_CHAIN_BLOCK_POLL_S", "60"))
ADDR_LOOKUP_DELAY_S = float(os.environ.get("SYGNIF_CHAIN_ADDR_DELAY_S", "1.5"))
UTXO_LOOKUP_DELAY_S = float(os.environ.get("SYGNIF_CHAIN_UTXO_DELAY_S", "0.6"))
COLD_TX_LIMIT       = int(os.environ.get("SYGNIF_CHAIN_COLD_TX_MAX", "50"))
HOT_TX_FLOOR        = int(os.environ.get("SYGNIF_CHAIN_HOT_TX_MIN", "1000"))
PEELING_MIN_CHUNKS  = int(os.environ.get("SYGNIF_CHAIN_PEEL_MIN", "3"))
PEELING_WINDOW_S    = int(os.environ.get("SYGNIF_CHAIN_PEEL_WIN_S", "3600"))
LTH_THRESHOLD_DAYS  = int(os.environ.get("SYGNIF_CHAIN_LTH_DAYS", "155"))
DORMANCY_BREAK_DAYS = int(os.environ.get("SYGNIF_CHAIN_DORMANCY_DAYS", "1825"))  # 5yr
MAX_UTXO_LOOKUPS    = int(os.environ.get("SYGNIF_CHAIN_MAX_UTXO_PER_TX", "5"))
LN_POLL_S           = float(os.environ.get("SYGNIF_CHAIN_LN_POLL_S", "3600"))   # 1h
LN_CAPACITY_DELTA_BTC = float(os.environ.get("SYGNIF_CHAIN_LN_CAP_DELTA_BTC", "5"))
LN_NODE_DELTA       = int(os.environ.get("SYGNIF_CHAIN_LN_NODE_DELTA", "50"))
LN_AVG_DELTA_PCT    = float(os.environ.get("SYGNIF_CHAIN_LN_AVG_DELTA_PCT", "1.0"))
DB_PATH             = "/var/lib/sygnif/swarm.db"

STATE_FILE          = pathlib.Path("/var/lib/sygnif/chain_state.json")
MEMPOOL_FILE        = pathlib.Path("/var/lib/sygnif/chain_mempool.json")
SANCTIONS_FILE      = pathlib.Path("/var/lib/sygnif/sanctioned_addresses.txt")

EVENT_LIMIT         = 300
WALLET_LIMIT        = 1000
CLUSTER_LIMIT       = 300
MEMPOOL_TTL_S       = 7200  # forget mempool entries after 2h unconfirmed

USER_AGENT          = "sygnif-chain-intel/1.0"
HEADERS             = {"User-Agent": USER_AGENT}
MEMPOOL_BASE        = "https://mempool.space/api"
BCI_BASE            = "https://blockchain.info"
BLOCKSTREAM_BASE    = "https://blockstream.info/api"

# Known hot-wallet anchors (confidence-low; addresses rotate)
KNOWN_EXCHANGE = {
    "3Mvtgmu8s8FjpdABqdmKaTYhDjnRu7eERN": "Coinbase",
    "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR": "Coinbase",
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo": "Binance Cold",
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s": "Binance",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h": "Binance Hot",
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g": "Bitfinex Hot",
    "bc1qrh99vw0ujsy9plhpf95dcyzj0jvc5lfvxsq3qm": "Bybit",
    "bc1qrsuxwwzwzy9rt0xytsen8w8t4puzeuwq7p83ar": "OKX",
    "37XuVSEpWW4trkfmvWzegTHQt7BdktSKUs": "Kraken",
    "bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfc27a4":
        "Bitfinex Cold (Tether-related)",
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF": "Mt.Gox Trustee",
}

_running = True
_metrics = defaultdict(int)
_metrics["started_at"] = time.time()
_btc_price = 81850.0


# ============================================================================
# HTTP
# ============================================================================
def _http_get_json(url: str, timeout: int = 15):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        _metrics["http_failures"] += 1
        return None


def _http_get_text(url: str, timeout: int = 15) -> str | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        return urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8")
    except Exception as e:
        _metrics["http_failures"] += 1
        return None


def fetch_tip_height() -> int | None:
    r = _http_get_json(f"{MEMPOOL_BASE}/blocks/tip/height")
    return r if isinstance(r, int) else None


def fetch_tip_hash() -> str | None:
    s = _http_get_text(f"{MEMPOOL_BASE}/blocks/tip/hash")
    if s and len(s.strip()) == 64:
        return s.strip()
    return None


def fetch_prices():
    """Fetch latest BTC price from Binance."""
    global _btc_price
    try:
        r = _http_get_json("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
        if r and "price" in r:
            _btc_price = float(r["price"])
            _metrics["price_updates"] += 1
    except Exception as e:
        print(f"  ! price fetch failed: {e}", file=sys.stderr, flush=True)


def fetch_block_full(block_hash: str) -> dict | None:
    return _http_get_json(f"{BCI_BASE}/rawblock/{block_hash}")


def fetch_mempool_recent() -> list | None:
    """Recent unconfirmed txs (last ~200, mempool.space)."""
    r = _http_get_json(f"{MEMPOOL_BASE}/mempool/recent")
    return r if isinstance(r, list) else None


def fetch_tx(txid: str) -> dict | None:
    """Tx details — used for both mempool deep-dive and UTXO age resolution."""
    return _http_get_json(f"{MEMPOOL_BASE}/tx/{txid}")


def fetch_address(addr: str) -> dict | None:
    return _http_get_json(f"{MEMPOOL_BASE}/address/{addr}")


# ============================================================================
# State persistence
# ============================================================================
def load_state() -> dict:
    if not STATE_FILE.exists():
        # Also load old sygnif_onchain_watcher state if present for continuity
        old_path = pathlib.Path("/var/lib/sygnif/onchain_state.json")
        if old_path.exists():
            try:
                old = json.loads(old_path.read_text())
                return _migrate_old_state(old)
            except (json.JSONDecodeError, OSError):
                pass
        return _new_state()
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return _new_state()


def _migrate_old_state(old: dict) -> dict:
    """Migrate from sygnif_onchain_watcher.py state schema to chain_intel."""
    s = _new_state()
    s["last_block_height"] = old.get("last_block_height", 0)
    for addr, w in (old.get("wallets") or {}).items():
        s["wallets"][addr] = {
            "addr":            addr,
            "tier":            w.get("tier", "?"),
            "label":           w.get("label", ""),
            "confidence":      _tier_to_confidence(w.get("tier", "?")),
            "balance_btc":     w.get("balance_btc", 0),
            "n_tx":            w.get("n_tx", 0),
            "total_received_btc": w.get("total_received_btc", 0),
            "total_sent_btc":  w.get("total_sent_btc", 0),
            "first_seen_at":   w.get("first_seen_at"),
            "last_seen_at":    w.get("last_seen_at"),
            "last_block":      w.get("last_block"),
            "cluster_id":      None,
            "notes":           w.get("notes", ""),
        }
    s["recent_events"] = old.get("recent_events", [])
    print(f"  migrated {len(s['wallets'])} wallets, {len(s['recent_events'])} events "
          f"from onchain_watcher state", flush=True)
    return s


def _new_state() -> dict:
    return {
        "schema":            "sygnif.chain_intel.v1",
        "created_at_utc":    dt.datetime.now(dt.timezone.utc).isoformat(),
        "last_block_height": 0,
        "wallets":           {},      # addr -> wallet dict
        "clusters":          {},      # cluster_id -> {addrs: [], label, confidence, ...}
        "addr_to_cluster":   {},      # addr -> cluster_id
        "recent_events":     [],
        "peeling_chains":    {},      # source_addr -> list of recent chunks
        "ln_stats_history":  [],      # rolling LN ecosystem snapshots
        "metrics":           {},
    }


def save_state(state: dict) -> None:
    state["updated_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    state["recent_events"] = state["recent_events"][-EVENT_LIMIT:]
    if len(state["wallets"]) > WALLET_LIMIT:
        # Keep wallets by composite priority: confidence * (balance+1)
        ranked = sorted(state["wallets"].items(),
                          key=lambda kv: -kv[1].get("confidence", 0)
                                            * (kv[1].get("balance_btc", 0) + 1))
        state["wallets"] = dict(ranked[:WALLET_LIMIT])
    if len(state["clusters"]) > CLUSTER_LIMIT:
        ranked = sorted(state["clusters"].items(),
                          key=lambda kv: -len(kv[1].get("addrs", [])))
        state["clusters"] = dict(ranked[:CLUSTER_LIMIT])
        # Rebuild addr_to_cluster
        new_a2c = {}
        for cid, c in state["clusters"].items():
            for a in c.get("addrs", []):
                new_a2c[a] = cid
        state["addr_to_cluster"] = new_a2c

    state["metrics"] = dict(_metrics)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, default=str, indent=2))
    os.replace(tmp, STATE_FILE)


def load_mempool_watch() -> dict:
    if not MEMPOOL_FILE.exists():
        return {}
    try:
        return json.loads(MEMPOOL_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_mempool_watch(watch: dict) -> None:
    # GC stale entries
    now = time.time()
    pruned = {k: v for k, v in watch.items()
              if now - v.get("seen_at", 0) < MEMPOOL_TTL_S}
    MEMPOOL_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = MEMPOOL_FILE.with_suffix(MEMPOOL_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(pruned, default=str, indent=2))
    os.replace(tmp, MEMPOOL_FILE)


def load_sanctioned() -> set:
    if not SANCTIONS_FILE.exists():
        return set()
    out = set()
    try:
        for line in SANCTIONS_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                out.add(line)
    except OSError:
        pass
    return out


# ============================================================================
# Confidence + classification
# ============================================================================
def _tier_to_confidence(tier: str) -> int:
    """Map old tier names to confidence scores (0–100)."""
    return {
        "EXCHANGE":         95,
        "EXCHANGE-LIKE":    75,
        "COLD_ACCUMULATOR": 80,
        "FRESH_COLD":       70,
        "WHALE_COLD":       65,
        "WHALE":            55,
        "ACTIVE":           45,
        "FRESH":            40,
        "DISTRIBUTED":      60,
        "WALLET":           20,
    }.get(tier, 10)


def classify(addr: str, summary: dict, sanctioned: set) -> tuple[str, str, int]:
    """Return (tier, label, confidence) for an address."""
    if addr in sanctioned:
        return ("SANCTIONED", "OFAC sanctioned address", 100)
    if addr in KNOWN_EXCHANGE:
        return ("EXCHANGE", KNOWN_EXCHANGE[addr], 95)
    if summary is None:
        return ("?", "lookup-failed", 0)
    n_tx = summary.get("n_tx", 0)
    bal  = summary.get("balance_btc", 0)
    recv = summary.get("total_received_btc", 0)
    sent = summary.get("total_sent_btc", 0)
    spend_ratio = sent / max(recv, 1)

    if n_tx >= HOT_TX_FLOOR:
        return ("EXCHANGE-LIKE",
                f"hot ({n_tx:,} txs, {bal:,.0f} BTC bal)", 70)
    if n_tx <= COLD_TX_LIMIT and spend_ratio < 0.01 and bal >= 100:
        return ("COLD_ACCUMULATOR",
                f"cold ({n_tx} txs, {bal:,.1f} BTC, 0% spent)", 85)
    if n_tx <= COLD_TX_LIMIT and spend_ratio < 0.01 and bal >= 10:
        return ("FRESH_COLD",
                f"new cold ({n_tx} txs, {bal:,.1f} BTC, 0% spent)", 75)
    if bal >= 1000:
        return ("WHALE_COLD",
                f"large dormant ({n_tx} txs, {bal:,.0f} BTC)", 65)
    if bal >= 100:
        return ("WHALE",
                f"holder ({n_tx} txs, {bal:,.1f} BTC)", 55)
    if n_tx >= 100:
        return ("ACTIVE",
                f"operator ({n_tx} txs, {bal:,.1f} BTC)", 45)
    if n_tx == 1:
        return ("FRESH",
                f"first tx ({bal:,.2f} BTC)", 50)
    return ("WALLET", f"normal ({n_tx} txs, {bal:,.3f} BTC)", 20)


def address_summary(info: dict | None) -> dict:
    if not info:
        return {"balance_btc": 0, "n_tx": 0,
                "total_received_btc": 0, "total_sent_btc": 0}
    cs = info.get("chain_stats", {}) or {}
    ms = info.get("mempool_stats", {}) or {}
    funded = (cs.get("funded_txo_sum", 0) + ms.get("funded_txo_sum", 0)) / 1e8
    spent  = (cs.get("spent_txo_sum",  0) + ms.get("spent_txo_sum",  0)) / 1e8
    n_tx   = cs.get("tx_count", 0) + ms.get("tx_count", 0)
    return {
        "balance_btc":        round(funded - spent, 4),
        "n_tx":               n_tx,
        "total_received_btc": round(funded, 4),
        "total_sent_btc":     round(spent, 4),
    }


# ============================================================================
# Clustering (Common-Input Heuristic)
# ============================================================================
def merge_into_cluster(state: dict, addrs: set, source: str = "CIH") -> str:
    """Union-find merge: addresses that appear together belong to one cluster.

    Returns the cluster_id for the merged group.
    """
    if not addrs:
        return ""
    existing_cids = set()
    for a in addrs:
        cid = state["addr_to_cluster"].get(a)
        if cid:
            existing_cids.add(cid)

    if not existing_cids:
        # All new — create a fresh cluster
        new_cid = "c_" + uuid.uuid4().hex[:8]
        state["clusters"][new_cid] = {
            "id":           new_cid,
            "addrs":        sorted(addrs),
            "created_at":   dt.datetime.now(dt.timezone.utc).isoformat(),
            "merged_via":   [source],
            "size":         len(addrs),
            "label":        None,
            "confidence":   60,  # CIH default
        }
        for a in addrs:
            state["addr_to_cluster"][a] = new_cid
        _metrics["clusters_created"] += 1
        return new_cid

    if len(existing_cids) == 1:
        cid = next(iter(existing_cids))
        c = state["clusters"][cid]
        old_addrs = set(c.get("addrs", []))
        new_addrs = old_addrs | addrs
        c["addrs"] = sorted(new_addrs)
        c["size"]  = len(new_addrs)
        for a in addrs:
            state["addr_to_cluster"][a] = cid
        return cid

    # Multiple clusters need merging — pick largest as primary
    primary = max(existing_cids,
                  key=lambda c: len(state["clusters"][c].get("addrs", [])))
    primary_c = state["clusters"][primary]
    merged = set(primary_c.get("addrs", [])) | addrs
    for cid in existing_cids:
        if cid == primary:
            continue
        c = state["clusters"][cid]
        merged |= set(c.get("addrs", []))
        for a in c.get("addrs", []):
            state["addr_to_cluster"][a] = primary
        primary_c.setdefault("merged_from", []).append(cid)
        del state["clusters"][cid]
    primary_c["addrs"] = sorted(merged)
    primary_c["size"]  = len(merged)
    for a in addrs:
        state["addr_to_cluster"][a] = primary
    _metrics["clusters_merged"] += len(existing_cids) - 1
    return primary


# ============================================================================
# UTXO age — Long-term vs Short-term holder analysis
# ============================================================================
_utxo_cache: dict = {}   # prev_txid -> {block_time, value_total}

def fetch_utxo_creation_time(prev_txid: str) -> int | None:
    """Get the block_time (unix sec) when this prev tx was confirmed (mempool.space)."""
    if prev_txid in _utxo_cache:
        return _utxo_cache[prev_txid]
    tx = fetch_tx(prev_txid)
    _metrics["utxo_lookups"] += 1
    if not tx:
        _utxo_cache[prev_txid] = None
        return None
    bt = (tx.get("status") or {}).get("block_time")
    _utxo_cache[prev_txid] = bt
    return bt


def fetch_utxo_creation_time_by_index(prev_tx_index: int) -> int | None:
    """Look up tx by blockchain.info numeric tx_index → return block time.

    blockchain.info /rawblock returns prev_out.tx_index (numeric) not .hash.
    /rawtx/<n>?format=json works with the numeric index.
    """
    cache_key = f"idx:{prev_tx_index}"
    if cache_key in _utxo_cache:
        return _utxo_cache[cache_key]
    url = f"{BCI_BASE}/rawtx/{prev_tx_index}?format=json"
    tx = _http_get_json(url)
    _metrics["utxo_lookups"] += 1
    if not tx:
        _utxo_cache[cache_key] = None
        return None
    bt = tx.get("time")  # blockchain.info uses .time (unix sec)
    _utxo_cache[cache_key] = bt
    return bt


def compute_utxo_ages(tx_inputs: list, block_time: int) -> dict:
    """Walk the first MAX_UTXO_LOOKUPS inputs, resolve their ages.

    Returns: {total_inputs_btc, lth_pct, sth_pct, dormancy_break_btc,
              median_age_days, oldest_age_days, ages: [...]}
    """
    ages = []
    for inp in tx_inputs[:MAX_UTXO_LOOKUPS]:
        prev_out = inp.get("prev_out") or {}
        prev_tx_index = prev_out.get("tx_index")
        prev_hash     = prev_out.get("hash") or inp.get("txid")

        bt = None
        if prev_hash:
            bt = fetch_utxo_creation_time(prev_hash)
        if not bt and prev_tx_index is not None:
            bt = fetch_utxo_creation_time_by_index(prev_tx_index)
        time.sleep(UTXO_LOOKUP_DELAY_S)
        if not bt:
            continue
        age_s = max(0, block_time - bt)
        value_btc = prev_out.get("value", 0) / 1e8
        ages.append({"age_days": age_s / 86400, "value_btc": value_btc,
                     "src": prev_hash[:16] if prev_hash else f"idx:{prev_tx_index}"})

    if not ages:
        return {"total_inputs_btc": 0, "lth_pct": None, "sth_pct": None,
                "dormancy_break_btc": 0, "median_age_days": None,
                "oldest_age_days": None, "ages": []}

    total = sum(a["value_btc"] for a in ages)
    lth_v = sum(a["value_btc"] for a in ages if a["age_days"] >= LTH_THRESHOLD_DAYS)
    sth_v = total - lth_v
    dormancy_break = sum(a["value_btc"] for a in ages
                         if a["age_days"] >= DORMANCY_BREAK_DAYS)
    ages_sorted = sorted(a["age_days"] for a in ages)
    return {
        "total_inputs_btc":   round(total, 3),
        "lth_pct":            round(lth_v / total * 100, 1) if total > 0 else None,
        "sth_pct":            round(sth_v / total * 100, 1) if total > 0 else None,
        "dormancy_break_btc": round(dormancy_break, 3),
        "median_age_days":    round(ages_sorted[len(ages_sorted)//2], 1),
        "oldest_age_days":    round(max(a["age_days"] for a in ages), 1),
        "ages":               ages,
    }


# ============================================================================
# Peeling chain detector
# ============================================================================
def detect_peeling(state: dict, tx: dict, block_time: int) -> dict | None:
    """Detect peeling chain: 1 input → 2 outputs (small payment + large change).

    Returns event dict if pattern matches, else None.
    """
    inputs  = tx.get("inputs") or []
    outputs = tx.get("out") or []
    if len(inputs) != 1 or len(outputs) != 2:
        return None

    in_addr = (inputs[0].get("prev_out") or {}).get("addr")
    if not in_addr:
        return None
    out_a = outputs[0]
    out_b = outputs[1]
    if out_a.get("value", 0) >= out_b.get("value", 0):
        change, payment = out_a, out_b
    else:
        change, payment = out_b, out_a

    # Change must go back to input addr or related cluster
    change_addr = change.get("addr")
    payment_addr = payment.get("addr")
    if not change_addr or not payment_addr:
        return None
    same_cluster = (
        change_addr == in_addr
        or state["addr_to_cluster"].get(in_addr) ==
           state["addr_to_cluster"].get(change_addr)
    )
    if not same_cluster:
        return None

    # Track per source
    key = in_addr
    chain = state["peeling_chains"].setdefault(key, [])
    chain.append({
        "ts":            block_time,
        "tx":            tx.get("hash"),
        "payment_addr":  payment_addr,
        "payment_btc":   round(payment.get("value", 0) / 1e8, 4),
        "remaining_btc": round(change.get("value", 0) / 1e8, 4),
    })
    # Trim to last hour
    chain[:] = [c for c in chain if block_time - c["ts"] <= PEELING_WINDOW_S]
    state["peeling_chains"][key] = chain

    # Confirmed when 3+ chunks to same destination within window
    if len(chain) >= PEELING_MIN_CHUNKS:
        recent_dests = [c["payment_addr"] for c in chain[-PEELING_MIN_CHUNKS:]]
        if len(set(recent_dests)) == 1:
            dest = recent_dests[0]
            total_paid = sum(c["payment_btc"]
                              for c in chain[-PEELING_MIN_CHUNKS:])
            return {
                "type":         "PEELING_CHAIN_CONFIRMED",
                "source_addr":  key,
                "dest_addr":    dest,
                "n_chunks":     len(chain),
                "total_btc":    round(total_paid, 3),
                "window_s":     block_time - chain[0]["ts"],
                "first_block_ts": chain[0]["ts"],
            }
    return None


# ============================================================================
# Sanction list bootstrap (one-time, run by seeding script)
# ============================================================================
OFAC_SOURCES = [
    "https://raw.githubusercontent.com/ultrasoundmoney/ofac-sanctioned-digital-currency-addresses/main/data/sanctioned_addresses_BTC.txt",
    "https://raw.githubusercontent.com/0xB10C/ofac-sanctioned-digital-currency-addresses/lists/sanctioned_addresses_BTC.txt",
    # Fallback to a known mirror — actual repos may move
]


# ============================================================================
# Swarm emission
# ============================================================================
def emit_swarm_event(topic: str, content: str, meta: dict,
                      tags: list, min_confidence: int = 60) -> None:
    """Write event to swarm.db. Filtered by min_confidence to reduce noise."""
    if meta.get("confidence", 0) < min_confidence:
        return
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
             "sygnif-chain-intel", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm emit failed: {type(e).__name__}: {e}",
              file=sys.stderr, flush=True)


# ============================================================================
# TRACK 1: Mempool monitor
# ============================================================================
def scan_mempool(mempool_watch: dict, state: dict) -> None:
    """Poll mempool.space for recent pending txs. Detect whales BEFORE confirmation."""
    recent = fetch_mempool_recent()
    if not recent:
        return
    now = time.time()
    new_alerts = 0
    for entry in recent:
        txid = entry.get("txid")
        if not txid:
            continue
        if txid in mempool_watch:
            continue
        value_sat = entry.get("value", 0)
        # Quick filter: if total tx value < threshold, skip without deep fetch
        if value_sat / 1e8 < THRESHOLD_BTC:
            continue

        # Confirm via full tx fetch
        tx = fetch_tx(txid)
        if not tx:
            continue
        total_out = sum(o.get("value", 0) for o in (tx.get("vout") or [])) / 1e8
        if total_out < THRESHOLD_BTC:
            continue

        from_addrs = []
        to_addrs   = []
        for inp in (tx.get("vin") or [])[:5]:
            a = (inp.get("prevout") or {}).get("scriptpubkey_address")
            v = (inp.get("prevout") or {}).get("value", 0) / 1e8
            if a:
                from_addrs.append({"addr": a, "v": round(v, 2)})
        for o in (tx.get("vout") or [])[:5]:
            a = o.get("scriptpubkey_address")
            v = o.get("value", 0) / 1e8
            if a:
                to_addrs.append({"addr": a, "v": round(v, 2)})

        mempool_watch[txid] = {
            "txid":           txid,
            "value_btc":      round(total_out, 2),
            "seen_at":        now,
            "from":           from_addrs,
            "to":             to_addrs,
            "confirmed":      False,
            "fee_sat":        tx.get("fee", 0),
            "feerate":        round(tx.get("fee", 0) / max(tx.get("size", 1), 1), 2),
        }
        new_alerts += 1
        _metrics["mempool_whales_seen"] += 1

        # Emit early-warning event
        head = (f"MEMPOOL_WHALE pending: {total_out:,.1f} BTC "
                f"(~${total_out*_btc_price/1e6:,.1f}M) "
                f"tx={txid[:10]}... fee={tx.get('fee',0)} sat")
        emit_swarm_event("chain.mempool_whale", head, {
            "type":      "MEMPOOL_WHALE",
            "txid":      txid,
            "value_btc": round(total_out, 2),
            "value_usd": round(total_out * _btc_price, 0),
            "from":      from_addrs,
            "to":        to_addrs,
            "fee_sat":   tx.get("fee", 0),
            "confidence": 90,  # high — actual tx broadcast
        }, ["chain", "mempool", "whale"])
        print(f"  [MEMPOOL] {head}", flush=True)
    if new_alerts:
        save_mempool_watch(mempool_watch)


# ============================================================================
# TRACK 2: Block scanner
# ============================================================================
def scan_block(state: dict, mempool_watch: dict, sanctioned: set, block: dict) -> int:
    """Walk one block, return number of high-confidence events emitted."""
    txs = block.get("tx") or []
    height = block.get("height")
    block_ts = block.get("time")
    emits = 0

    # Pre-filter: find whale txs
    whale_txs = []
    for t in txs:
        outs = t.get("out") or []
        total_out = sum(o.get("value", 0) for o in outs) / 1e8
        if total_out >= THRESHOLD_BTC:
            whale_txs.append((t, total_out))

    if not whale_txs:
        _metrics["blocks_scanned"] += 1
        state["last_block_height"] = height
        return 0

    # CIH clustering pass: for every tx in block with multiple input addresses,
    # cluster them (regardless of whale status — clustering is general)
    for t in txs:
        in_addrs = set()
        for inp in (t.get("inputs") or []):
            a = (inp.get("prev_out") or {}).get("addr")
            if a:
                in_addrs.add(a)
        if len(in_addrs) >= 2:
            merge_into_cluster(state, in_addrs, source=f"CIH:blk{height}")

    # Address-lookup queue for whales (skip if already in state)
    addrs_to_lookup = set()
    for t, _ in whale_txs:
        for inp in (t.get("inputs") or [])[:5]:
            a = (inp.get("prev_out") or {}).get("addr")
            if a and a not in state["wallets"]:
                addrs_to_lookup.add(a)
        for o in (t.get("out") or [])[:5]:
            a = o.get("addr")
            if a and a not in state["wallets"]:
                addrs_to_lookup.add(a)

    print(f"  block {height}: {len(whale_txs)} whales, "
          f"{len(addrs_to_lookup)} new addrs", flush=True)

    # Look up new addresses
    for addr in addrs_to_lookup:
        if not _running:
            break
        info = fetch_address(addr)
        summary = address_summary(info)
        tier, label, confidence = classify(addr, summary, sanctioned)
        cluster_id = state["addr_to_cluster"].get(addr)
        wallet = {
            "addr":           addr,
            "tier":           tier,
            "label":          label,
            "confidence":     confidence,
            "cluster_id":     cluster_id,
            "first_seen_at":  dt.datetime.now(dt.timezone.utc).isoformat(),
            "last_seen_at":   dt.datetime.now(dt.timezone.utc).isoformat(),
            "last_block":     height,
            **summary,
        }
        state["wallets"][addr] = wallet
        # If sanctioned, lift cluster confidence
        if tier == "SANCTIONED" and cluster_id:
            state["clusters"][cluster_id]["confidence"] = 100
            state["clusters"][cluster_id]["label"] = "SANCTIONED (OFAC)"
        time.sleep(ADDR_LOOKUP_DELAY_S)

    # Whale-tx event emission with UTXO age + peeling + cluster context
    for t, total_out in whale_txs:
        h = t.get("hash")
        # Check mempool: was this announced first?
        was_mempool_pre = h in mempool_watch
        if was_mempool_pre:
            mempool_watch[h]["confirmed"] = True
            mempool_watch[h]["confirmed_block"] = height

        # Resolve UTXO ages (the new heavy lift)
        utxo_age = compute_utxo_ages(t.get("inputs") or [], block_ts)

        # Address gather
        in_addrs = []
        for inp in (t.get("inputs") or [])[:5]:
            a = (inp.get("prev_out") or {}).get("addr")
            v = (inp.get("prev_out") or {}).get("value", 0) / 1e8
            if a:
                in_addrs.append({
                    "addr":  a,
                    "v":     round(v, 2),
                    "tier":  (state["wallets"].get(a) or {}).get("tier"),
                    "label": (state["wallets"].get(a) or {}).get("label"),
                    "cid":   state["addr_to_cluster"].get(a),
                })
        out_addrs = []
        for o in (t.get("out") or [])[:5]:
            a = o.get("addr"); v = o.get("value", 0) / 1e8
            if a:
                out_addrs.append({
                    "addr":  a,
                    "v":     round(v, 2),
                    "tier":  (state["wallets"].get(a) or {}).get("tier"),
                    "label": (state["wallets"].get(a) or {}).get("label"),
                    "cid":   state["addr_to_cluster"].get(a),
                })

        # Category
        def has_tier(addrs, tier_set):
            return any((x.get("tier") in tier_set) for x in addrs)
        EXCH = {"EXCHANGE", "EXCHANGE-LIKE"}
        COLD = {"COLD_ACCUMULATOR", "FRESH_COLD", "WHALE_COLD"}
        from_exch = has_tier(in_addrs, EXCH)
        to_exch   = has_tier(out_addrs, EXCH)
        to_cold   = has_tier(out_addrs, COLD)
        has_sanc  = (has_tier(in_addrs, {"SANCTIONED"})
                     or has_tier(out_addrs, {"SANCTIONED"}))

        if has_sanc:
            category, base_conf = "SANCTIONED_FLOW", 100
        elif to_cold and not from_exch:
            category, base_conf = "ACCUMULATION_TO_COLD", 85
        elif to_exch and not from_exch:
            category, base_conf = "DEPOSIT_TO_EXCHANGE", 75
        elif from_exch and not to_exch:
            category, base_conf = "WITHDRAWAL_FROM_EXCHANGE", 70
        elif from_exch and to_exch:
            category, base_conf = "EXCHANGE_INTERNAL", 30
        else:
            category, base_conf = "WHALE_OFF_EXCHANGE", 55

        # Lift confidence from UTXO age signals
        age_flags = []
        if utxo_age.get("dormancy_break_btc", 0) > 50:
            age_flags.append("DORMANCY_BREAK_5YR")
            base_conf = max(base_conf, 95)
        elif utxo_age.get("lth_pct") is not None and utxo_age["lth_pct"] >= 80:
            age_flags.append("LTH_SPEND_HEAVY")
            base_conf = max(base_conf, 80)
        elif utxo_age.get("lth_pct") is not None and utxo_age["lth_pct"] >= 50:
            age_flags.append("LTH_SPEND")
            base_conf = max(base_conf, 70)
        if was_mempool_pre:
            age_flags.append("MEMPOOL_PRE_CONFIRMED")
            base_conf = max(base_conf, 80)

        # Peeling chain check
        peel_event = detect_peeling(state, t, block_ts)
        if peel_event:
            age_flags.append("PEELING_CHAIN")
            base_conf = max(base_conf, 90)

        event = {
            "type":          "WHALE_TX",
            "category":      category,
            "ts":            block_ts,
            "ts_utc":        dt.datetime.fromtimestamp(block_ts,
                                                         dt.timezone.utc).isoformat(),
            "block":         height,
            "tx_hash":       h,
            "value_btc":     round(total_out, 2),
            "value_usd":     round(total_out * _btc_price, 0),
            "from":          in_addrs,
            "to":            out_addrs,
            "confidence":    base_conf,
            "flags":         age_flags,
            "utxo_age":      utxo_age,
            "was_in_mempool_pre": was_mempool_pre,
            "peeling_event": peel_event,
        }
        state["recent_events"].append(event)
        _metrics["whale_txs_seen"] += 1

        head_parts = [
            f"{category} {total_out:,.1f} BTC (~${total_out*_btc_price/1e6:,.1f}M)",
            f"blk={height}",
        ]
        if utxo_age.get("median_age_days") is not None:
            head_parts.append(f"medAge={utxo_age['median_age_days']:.0f}d")
        if utxo_age.get("lth_pct") is not None:
            head_parts.append(f"LTH={utxo_age['lth_pct']:.0f}%")
        if age_flags:
            head_parts.append(",".join(age_flags))
        if was_mempool_pre:
            wait_s = block_ts - mempool_watch[h]["seen_at"]
            head_parts.append(f"pre+{int(wait_s)}s")
        head = " | ".join(head_parts)

        emit_swarm_event("chain.whale", head, event,
                          ["chain", "whale", category, *age_flags])

        # Separate event for dormancy breaks and peeling
        if "DORMANCY_BREAK_5YR" in age_flags:
            emit_swarm_event("chain.dormancy_break",
                              f"DORMANCY: {utxo_age['oldest_age_days']:.0f}d-old "
                              f"UTXO spent in {total_out:,.1f} BTC tx blk={height}",
                              event, ["chain", "dormancy"])
        if peel_event:
            emit_swarm_event("chain.peeling_chain",
                              f"PEELING_CHAIN: {peel_event['n_chunks']} chunks "
                              f"{peel_event['total_btc']:,.1f} BTC "
                              f"in {peel_event['window_s']}s",
                              peel_event, ["chain", "peeling"])
        emits += 1
        if base_conf >= 80:
            print(f"  [WHALE] {head}", flush=True)

    # --- v2: CIH expansion for newly-found high-confidence wallets ---
    # For each whale address we just discovered, walk its tx history
    # to find ALL its co-spending addresses (the FULL entity).
    expanded_this_block = 0
    for t, _ in whale_txs[:5]:  # cap at 5 whale-tx expansions per block
        for inp in (t.get("inputs") or [])[:2]:  # 2 input addrs per tx
            a = (inp.get("prev_out") or {}).get("addr")
            if not a:
                continue
            w = state["wallets"].get(a)
            if not w or w.get("tier") in ("?", "WALLET", "FRESH"):
                continue
            try:
                added = expand_cluster_via_history(state, a)
                expanded_this_block += added
            except Exception as e:
                print(f"    history-expansion err for {a[:18]}: {e}",
                      file=sys.stderr, flush=True)
            time.sleep(0.5)

    # --- v2: Label propagation across all clusters ---
    propagated = propagate_cluster_labels(state)

    # --- v2: Mixing detection on whale txs ---
    for t, _ in whale_txs:
        mix = detect_mixing(t)
        if mix:
            _metrics["mixing_detected"] += 1
            head = (f"MIXING_DETECTED [{mix['type']}] "
                    f"anon_set={mix.get('anonymity_set','?')} "
                    f"value={mix.get('value_btc',0):,.2f} BTC blk={height}")
            emit_swarm_event("chain.mixing", head, {
                "type":   "MIXING",
                "detail": mix,
                "tx_hash": t.get("hash"),
                "block":   height,
                "confidence": 80,
            }, ["chain", "mixing", mix["type"]])
            print(f"  [MIXING] {head}", flush=True)

    if expanded_this_block:
        print(f"    + {expanded_this_block} addrs via CIH-history expansion "
              f"+ {propagated} labels propagated", flush=True)

    _metrics["blocks_scanned"] += 1
    state["last_block_height"] = height
    return emits



# ============================================================================
# Module: CIH expansion via address history (blockstream.info /address/{addr}/txs)
# ============================================================================
_history_cache: dict = {}  # addr -> last fetched timestamp

def expand_cluster_via_history(state: dict, addr: str,
                                 max_txs: int = 25) -> int:
    """Fetch this address\'s recent tx history. For every tx where it was an
    input alongside other addresses, merge all those addresses into one cluster.

    This is how we discover the FULL entity behind a single discovered address.
    Returns: number of additional addresses merged into the cluster.

    Cache: 1 hour per address to avoid hammering blockstream.info.
    """
    now = time.time()
    last = _history_cache.get(addr, 0)
    if now - last < 3600:
        return 0
    _history_cache[addr] = now

    url = f"{BLOCKSTREAM_BASE}/address/{addr}/txs"
    txs = _http_get_json(url)
    _metrics["history_lookups"] += 1
    if not isinstance(txs, list):
        return 0

    new_merges = 0
    for tx in txs[:max_txs]:
        # blockstream.info Esplora format: vin[].prevout.scriptpubkey_address
        in_addrs = set()
        for v in (tx.get("vin") or []):
            a = (v.get("prevout") or {}).get("scriptpubkey_address")
            if a:
                in_addrs.add(a)
        if addr in in_addrs and len(in_addrs) >= 2:
            cid_before = state["addr_to_cluster"].get(addr)
            size_before = (len(state["clusters"].get(cid_before, {})
                                .get("addrs", [])) if cid_before else 0)
            merge_into_cluster(state, in_addrs, source="HIST")
            cid_after = state["addr_to_cluster"].get(addr)
            size_after = len(state["clusters"].get(cid_after, {})
                              .get("addrs", []))
            new_merges += max(0, size_after - size_before)
    time.sleep(0.3)  # gentle on blockstream
    return new_merges


# ============================================================================
# Module: Label propagation across clusters
# ============================================================================
def propagate_cluster_labels(state: dict) -> int:
    """For each cluster: if any address has tier=EXCHANGE or SANCTIONED,
    propagate that label + confidence to all other addresses in the cluster.

    Confidence is reduced one notch on propagated labels (it\'s a prediction,
    not direct attribution) — Arkham-style verified vs predicted distinction.

    Returns: number of addresses re-labeled.
    """
    relabeled = 0
    for cid, cluster in list(state["clusters"].items()):
        addrs = cluster.get("addrs") or []
        if len(addrs) < 2:
            continue
        # Find any anchor (known EXCHANGE, SANCTIONED, or COLD_ACCUMULATOR)
        anchor = None
        anchor_priority = -1
        TIER_PRIORITY = {
            "SANCTIONED":         100,
            "EXCHANGE":           95,
            "COLD_ACCUMULATOR":   80,
            "EXCHANGE-LIKE":      70,
        }
        for a in addrs:
            w = state["wallets"].get(a)
            if not w:
                continue
            pri = TIER_PRIORITY.get(w.get("tier", ""), 0)
            if pri > anchor_priority:
                anchor = w
                anchor_priority = pri
        if not anchor or anchor_priority < 70:
            continue
        anchor_tier = anchor.get("tier")
        anchor_label = anchor.get("label", "")
        # Propagate to peers
        for a in addrs:
            w = state["wallets"].get(a)
            if not w:
                continue
            if w.get("tier") == anchor_tier:
                continue
            # Don\'t downgrade a known-verified label
            if w.get("confidence", 0) >= 95 and w.get("tier") not in ("?", "WALLET", "FRESH"):
                continue
            old_tier = w.get("tier")
            w["tier"] = anchor_tier
            w["label"] = f"{anchor_label} (via cluster)"
            w["confidence"] = max(0, anchor_priority - 15)  # propagated = -15
            w["propagated_from"] = anchor.get("addr")
            relabeled += 1
        cluster["label"] = anchor_label
        cluster["confidence"] = anchor_priority
    if relabeled:
        _metrics["labels_propagated"] += relabeled
    return relabeled


# ============================================================================
# Module: CoinJoin / mixing detection (Wasabi v1/v2, Whirlpool, JoinMarket)
# ============================================================================
def detect_mixing(tx: dict) -> dict | None:
    """Detect mixing-service patterns. Returns event dict or None.

    Heuristics (Meiklejohn, Möser et al + Wallet docs):

    Whirlpool (Samourai): exactly 5 inputs + 5 outputs, all OUTPUTS same value.
      Pools: 0.001, 0.01, 0.05, 0.5 BTC (denominations).

    Wasabi v1: 10-50 inputs + outputs, output values cluster around base
      denomination (0.1 BTC by default) with change outputs of varying size.

    Wasabi v2 (WabiSabi): 50-400+ inputs and outputs, free amounts but heavy
      structural symmetry between in/out counts.

    JoinMarket: 3+ inputs from distinct senders, 3+ outputs of similar size.
    """
    inputs  = tx.get("inputs") or []
    outputs = tx.get("out") or []
    n_in = len(inputs)
    n_out = len(outputs)
    if n_in < 3 or n_out < 3:
        return None

    in_values = [(i.get("prev_out") or {}).get("value", 0) / 1e8 for i in inputs]
    out_values = [o.get("value", 0) / 1e8 for o in outputs]
    in_addrs = set((i.get("prev_out") or {}).get("addr") for i in inputs)
    in_addrs.discard(None)
    out_addrs = set(o.get("addr") for o in outputs)
    out_addrs.discard(None)

    # --- Whirlpool: exactly 5+5, all outputs equal ---
    WHIRLPOOL_DENOMS = [0.001, 0.01, 0.05, 0.5]
    if n_in == 5 and n_out == 5:
        if len(set(round(v, 6) for v in out_values)) == 1:
            out_v = out_values[0]
            for denom in WHIRLPOOL_DENOMS:
                if abs(out_v - denom) < 0.0005:
                    return {
                        "type":        "WHIRLPOOL",
                        "denom_btc":   denom,
                        "n_participants": 5,
                        "anonymity_set":  5,
                        "value_btc":   round(sum(out_values), 4),
                    }

    # --- Wasabi v1: 10-50 inputs/outputs, outputs cluster near base denom ---
    if 10 <= n_in <= 80 and 10 <= n_out <= 80:
        from collections import Counter
        # Round outputs to nearest 0.001 BTC, find most common bucket
        buckets = Counter(round(v, 3) for v in out_values)
        top_bucket, top_count = buckets.most_common(1)[0]
        if top_count >= 5 and 0.01 <= top_bucket <= 1.0:
            return {
                "type":           "WASABI_v1_LIKE",
                "denom_btc":      top_bucket,
                "anonymity_set":  top_count,
                "n_in":           n_in,
                "n_out":          n_out,
                "value_btc":      round(sum(out_values), 2),
            }

    # --- Wasabi v2 (WabiSabi): 50+ in/out, symmetric ---
    if n_in >= 50 and n_out >= 50 and abs(n_in - n_out) <= 0.3 * n_in:
        return {
            "type":           "WASABI_v2_LIKE",
            "n_in":            n_in,
            "n_out":           n_out,
            "anonymity_set":   min(n_in, n_out),
            "value_btc":       round(sum(out_values), 2),
        }

    # --- JoinMarket: 3+ distinct input senders, 3+ equal-ish outputs ---
    if 3 <= n_in <= 12 and 3 <= n_out <= 12 and len(in_addrs) >= 3:
        # Look for 2+ outputs of identical (or very near) value
        from collections import Counter
        buckets = Counter(round(v, 4) for v in out_values)
        top_bucket, top_count = buckets.most_common(1)[0]
        if top_count >= 2 and top_bucket >= 0.01:
            return {
                "type":          "JOINMARKET_LIKE",
                "n_participants": len(in_addrs),
                "denom_btc":     top_bucket,
                "anonymity_set": top_count,
                "value_btc":     round(sum(out_values), 2),
            }

    return None


# ============================================================================
# Module: Behavioral timing fingerprint
# ============================================================================
def classify_timing_pattern(hours: list) -> tuple[str, int]:
    """Given a list of tx hours (0-23 UTC), classify the actor.

    Returns (label, confidence_0_100).
    """
    if not hours or len(hours) < 5:
        return ("insufficient_data", 0)

    from collections import Counter
    counter = Counter(hours)
    total = len(hours)

    # Time-zone windows (UTC):
    #   US business:    13:00-21:00 UTC  (8:00-16:00 ET)
    #   Asian:          00:00-08:00 UTC  (8:00-16:00 China time)
    #   European:       07:00-15:00 UTC  (8:00-16:00 CET)
    us_window     = sum(counter[h] for h in range(13, 21))
    asia_window   = sum(counter[h] for h in range(0, 8))
    europe_window = sum(counter[h] for h in range(7, 15))

    us_pct     = us_window     / total * 100
    asia_pct   = asia_window   / total * 100
    europe_pct = europe_window / total * 100

    # Compute "uniformity" — how evenly spread across all 24 hours
    expected_per_hour = total / 24
    variance = sum((counter[h] - expected_per_hour) ** 2 for h in range(24)) / 24
    coef_var = (variance ** 0.5) / max(expected_per_hour, 1e-9)
    # Low coef_var = uniform (retail); high = concentrated (institutional)

    if us_pct >= 60:
        return ("INSTITUTIONAL_US", min(95, int(60 + us_pct/2)))
    if asia_pct >= 60:
        return ("INSTITUTIONAL_ASIA", min(95, int(60 + asia_pct/2)))
    if europe_pct >= 60:
        return ("INSTITUTIONAL_EU", min(95, int(60 + europe_pct/2)))
    if coef_var < 0.4:
        return ("RETAIL_DISTRIBUTED", 70)
    return ("MIXED", 40)


def compute_timing_fingerprint_for_cluster(state: dict, cluster_id: str,
                                              limit: int = 25) -> dict | None:
    """For a cluster, fetch recent tx times for all member addresses and
    classify the actor.

    Returns: {label, confidence, n_txs, hours_distribution} or None.
    Network-light: limit total tx fetches to `limit`.
    """
    cluster = state["clusters"].get(cluster_id)
    if not cluster:
        return None
    addrs = cluster.get("addrs", [])[:5]  # check up to 5 addresses

    hours = []
    fetched = 0
    for addr in addrs:
        if fetched >= limit:
            break
        url = f"{BLOCKSTREAM_BASE}/address/{addr}/txs"
        txs = _http_get_json(url)
        if not isinstance(txs, list):
            continue
        for tx in txs:
            bt = (tx.get("status") or {}).get("block_time")
            if bt:
                hours.append(dt.datetime.fromtimestamp(bt, dt.timezone.utc).hour)
                fetched += 1
                if fetched >= limit:
                    break
        time.sleep(0.3)

    if not hours:
        return None
    label, conf = classify_timing_pattern(hours)
    from collections import Counter
    dist = Counter(hours)
    fp = {
        "label":              label,
        "confidence":         conf,
        "n_txs":              len(hours),
        "hours_distribution": dict(dist),
        "computed_at":        dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    cluster["timing_fingerprint"] = fp
    _metrics["timing_fingerprints"] += 1
    return fp




# ============================================================================
# Module: Lightning Network ecosystem stats poller
# ============================================================================
def scan_ln_stats(state: dict) -> int:
    """Poll mempool.space LN stats. Emit delta events vs previous snapshot.

    Cheap: 1 HTTP call/hour. No infrastructure dependency.
    """
    url = f"{MEMPOOL_BASE}/v1/lightning/statistics/latest"
    r = _http_get_json(url)
    _metrics["ln_polls"] += 1
    if not r:
        return 0
    latest = r.get("latest") or {}
    if not latest:
        return 0

    now = int(time.time())
    snap = {
        "ts":              now,
        "ts_utc":          dt.datetime.now(dt.timezone.utc).isoformat(),
        "as_of":           latest.get("added"),
        "node_count":      latest.get("node_count", 0),
        "channel_count":   latest.get("channel_count", 0),
        "total_capacity_sat": latest.get("total_capacity", 0),
        "total_capacity_btc": round(latest.get("total_capacity", 0) / 1e8, 2),
        "avg_capacity_sat":   latest.get("avg_capacity", 0),
        "med_capacity_sat":   latest.get("med_capacity", 0),
        "tor_nodes":          latest.get("tor_nodes", 0),
        "clearnet_nodes":     latest.get("clearnet_nodes", 0),
        "avg_fee_rate":       latest.get("avg_fee_rate", 0),
        "med_fee_rate":       latest.get("med_fee_rate", 0),
    }

    history = state.get("ln_stats_history") or []
    prev = history[-1] if history else None
    history.append(snap)
    state["ln_stats_history"] = history[-30:]

    if not prev:
        # First snapshot — emit baseline
        cap_btc = snap["total_capacity_btc"]
        nc = snap["node_count"]; cc = snap["channel_count"]
        avg_m = snap["avg_capacity_sat"] / 1e5
        head = (f"LN_BASELINE  capacity={cap_btc:,.0f} BTC  "
                f"nodes={nc:,}  channels={cc:,}  avg_ch={avg_m:.2f}M sat")
        emit_swarm_event("chain.ln_stats", head, {
            **snap, "type": "LN_BASELINE", "confidence": 95,
        }, ["chain", "ln", "baseline"])
        print(f"  [LN] {head}", flush=True)
        return 1

    # Compute deltas
    cap_d_btc     = snap["total_capacity_btc"] - prev["total_capacity_btc"]
    node_d        = snap["node_count"]    - prev["node_count"]
    ch_d          = snap["channel_count"] - prev["channel_count"]
    avg_d_sat     = snap["avg_capacity_sat"] - prev["avg_capacity_sat"]
    avg_d_pct     = (avg_d_sat / max(prev["avg_capacity_sat"], 1)) * 100

    significant = (
        abs(cap_d_btc) >= LN_CAPACITY_DELTA_BTC
        or abs(node_d) >= LN_NODE_DELTA
        or abs(avg_d_pct) >= LN_AVG_DELTA_PCT
    )

    if significant:
        direction = "growing" if cap_d_btc >= 0 else "shrinking"
        head = (f"LN_ECOSYSTEM Δ {direction}  "
                f"cap{cap_d_btc:+,.1f} BTC  "
                f"nodes{node_d:+,d}  channels{ch_d:+,d}  "
                f"avg{avg_d_pct:+.2f}%")
        flags = []
        if abs(cap_d_btc) >= LN_CAPACITY_DELTA_BTC:
            flags.append("CAPACITY_SHIFT")
        if abs(node_d) >= LN_NODE_DELTA:
            flags.append("NODE_GROWTH" if node_d > 0 else "NODE_DECLINE")
        if avg_d_pct >= LN_AVG_DELTA_PCT:
            flags.append("WHALE_CHANNELS_APPEARING")
        elif avg_d_pct <= -LN_AVG_DELTA_PCT:
            flags.append("CHANNELS_CONSOLIDATING")

        emit_swarm_event("chain.ln_stats", head, {
            "type":            "LN_DELTA",
            "current":         snap,
            "previous":        prev,
            "delta": {
                "capacity_btc":   cap_d_btc,
                "node_count":     node_d,
                "channel_count":  ch_d,
                "avg_capacity_pct": round(avg_d_pct, 2),
            },
            "flags":           flags,
            "confidence":      90,
        }, ["chain", "ln", "delta", *flags])
        print(f"  [LN] {head}  flags={flags}", flush=True)
        _metrics["ln_deltas_emitted"] += 1
        return 1
    return 0


# ============================================================================
# Main loop
# ============================================================================
def main() -> int:
    global _running
    print(f"=== sygnif_chain_intel started @ "
          f"{dt.datetime.now(dt.timezone.utc).isoformat()} ===", flush=True)
    print(f"  threshold:  {THRESHOLD_BTC} BTC", flush=True)
    print(f"  mempool poll: {MEMPOOL_POLL_S}s", flush=True)
    print(f"  block poll:   {BLOCK_POLL_S}s", flush=True)
    print(f"  LTH threshold: {LTH_THRESHOLD_DAYS} days", flush=True)
    print(f"  dormancy threshold: {DORMANCY_BREAK_DAYS} days", flush=True)
    print(f"  state: {STATE_FILE}", flush=True)

    state = load_state()
    mempool_watch = load_mempool_watch()
    sanctioned = load_sanctioned()
    print(f"  loaded {len(state['wallets'])} wallets, "
          f"{len(state['clusters'])} clusters, "
          f"{len(state['recent_events'])} events, "
          f"{len(mempool_watch)} mempool, "
          f"{len(sanctioned)} sanctioned addrs", flush=True)
    print(f"  last_block_height: {state['last_block_height']}", flush=True)

    def _sigterm(sig, frame):
        global _running
        print(f"  signal {sig}, shutting down", flush=True)
        _running = False
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT,  _sigterm)

    last_mempool_poll = 0.0
    last_block_poll = 0.0
    last_save = 0.0
    last_ln_poll = 0.0
    last_price_poll = 0.0

    while _running:
        now = time.time()

        # Update prices every 5 min
        if now - last_price_poll >= 300:
            fetch_prices()
            last_price_poll = now

        # TRACK 1: Mempool monitor (every 15s)
        if now - last_mempool_poll >= MEMPOOL_POLL_S:
            try:
                scan_mempool(mempool_watch, state)
            except Exception as e:
                print(f"  ! mempool scan error: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_mempool_poll = now

        # TRACK 2: Block scanner (every 60s)
        if now - last_block_poll >= BLOCK_POLL_S:
            try:
                tip = fetch_tip_height()
                if tip and tip != state["last_block_height"]:
                    new_blocks = (1 if state["last_block_height"] == 0
                                  else min(6, tip - state["last_block_height"]))
                    print(f"\n  ↪ new tip {tip} "
                          f"(was {state['last_block_height']}, "
                          f"scanning {new_blocks} blocks)", flush=True)
                    current_hash = fetch_tip_hash()
                    scanned = 0
                    while (current_hash and scanned < new_blocks and _running):
                        block = fetch_block_full(current_hash)
                        if not block:
                            break
                        scan_block(state, mempool_watch, sanctioned, block)
                        scanned += 1
                        current_hash = block.get("prev_block")
                        time.sleep(0.5)
                    save_state(state)
                    save_mempool_watch(mempool_watch)
            except Exception as e:
                print(f"  ! block scan error: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_block_poll = now

        # TRACK 4: LN ecosystem stats (every 1h)
        if now - last_ln_poll >= LN_POLL_S:
            try:
                scan_ln_stats(state)
            except Exception as e:
                print(f"  ! ln stats scan error: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_ln_poll = now

        # Periodic state save (every 5 min)
        if now - last_save >= 300:
            try:
                save_state(state)
                save_mempool_watch(mempool_watch)
            except Exception as e:
                print(f"  ! save error: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_save = now

        time.sleep(1.0)

    print(f"  flushing state on shutdown", flush=True)
    save_state(state)
    save_mempool_watch(mempool_watch)
    return 0


if __name__ == "__main__":
    sys.exit(main())
