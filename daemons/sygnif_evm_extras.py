#!/usr/bin/env python3
"""sygnif_evm_extras.py — DEX + cross-chain bridge signal daemon.

Augments sygnif_evm_signals with:

  TRACK 1 — Large DEX swaps (Uniswap V3 USDC/USDT/WBTC pools)
    Polls Alchemy alchemy_getAssetTransfers on the main pool contracts.
    Emits on ≥ $1M individual swap value.
    USDC → WBTC swap = on-chain spot BTC buying with stablecoin
    WBTC → USDC swap = exiting tokenized BTC

  TRACK 2 — Bridge events (Stargate, Across)
    Tracks large transfers through bridge router contracts.
    Stargate router event logs via Etherscan V2.
    Cross-chain capital migration = positioning intent.

  TRACK 3 — ETF AP creation/redemption (stub — needs per-ETF research)
    Placeholder for future expansion. Each ETF's share-creation contract
    emits events visible on-chain. Wiring depends on knowing each ETF's
    exact creation/redemption contract.

State file: /var/lib/sygnif/evm_extras_state.json
Swarm topics:
  evm.dex_swap     large DEX swap (≥$1M)
  evm.bridge_flow  large bridge transfer (≥$1M)
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

ETHERSCAN_KEY  = os.environ.get("SYGNIF_ETHERSCAN_KEY", "")
ALCHEMY_KEY    = os.environ.get("SYGNIF_ALCHEMY_KEY", "")

DEX_SWAP_THRESHOLD_USD    = float(os.environ.get("SYGNIF_DEX_SWAP_USD", "1000000"))
BRIDGE_FLOW_THRESHOLD_USD = float(os.environ.get("SYGNIF_BRIDGE_USD", "1000000"))
POLL_S                    = float(os.environ.get("SYGNIF_EVM_EXTRAS_POLL_S", "600"))   # 10 min

DB_PATH    = "/var/lib/sygnif/swarm.db"
STATE_FILE = pathlib.Path("/var/lib/sygnif/evm_extras_state.json")
ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
ALCHEMY_RPC  = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}" if ALCHEMY_KEY else None

HEADERS = {"User-Agent": "sygnif-evm-extras/1.0"}

# ============================================================================
# Contracts of interest
# ============================================================================
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
WBTC = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"

# Uniswap V3 pools — main BTC/stablecoin venues
UNISWAP_POOLS = {
    "WBTC/USDC-0.3%":   "0x99ac8cAfBacC4eA6db8378FE89B3A1B91Bb02C8e",
    "WBTC/USDC-0.05%":  "0x9a772018FbD77fcD2d25657e5C547BAfF3Fd7D16",
    "WBTC/USDT-0.3%":   "0x9Db9e0e53058C89e5B94e29621a205198648425B",
    "WBTC/WETH-0.3%":   "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD",
    "WBTC/WETH-0.05%":  "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0",
}

# Bridge routers
BRIDGE_ROUTERS = {
    "Stargate Router":  "0x8731d54E9D02c286767d56ac03e8037C07e01e98",
    "Across SpokePool": "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",
    "LayerZero Endpoint": "0x66A71Dcef29A0fFBDBE3c6a460a3B5BC225Cd675",
    "Wormhole Token Bridge": "0x3ee18B2214AEFb5f6180c54e3E36c98a37AbB9d3",
}

_running = True
_metrics = defaultdict(int)
_metrics["started_at"] = time.time()


def _http_get_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        _metrics["http_failures"] += 1
        return None


def _http_post_json(url, body, timeout=15):
    try:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST",
                                       headers={**HEADERS,
                                                  "Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        _metrics["http_failures"] += 1
        return None


def alchemy_rpc(method, params):
    if not ALCHEMY_RPC:
        return None
    return _http_post_json(ALCHEMY_RPC, {
        "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
    })


def emit_swarm(topic, content, meta, tags):
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
             "sygnif-evm-extras", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm err: {e}", file=sys.stderr, flush=True)


# ============================================================================
# State
# ============================================================================
def load_state():
    if not STATE_FILE.exists():
        return _new_state()
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return _new_state()


def _new_state():
    return {
        "schema":          "sygnif.evm_extras.v1",
        "created_at_utc":  dt.datetime.now(dt.timezone.utc).isoformat(),
        "last_block":      {"dex": 0, "bridge": 0},
        "recent_dex":      [],
        "recent_bridge":   [],
        "metrics":         {},
    }


def save_state(state):
    state["updated_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    state["recent_dex"]    = state.get("recent_dex", [])[-200:]
    state["recent_bridge"] = state.get("recent_bridge", [])[-200:]
    state["metrics"] = dict(_metrics)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, default=str, indent=2))
    os.replace(tmp, STATE_FILE)


# ============================================================================
# TRACK 1 — DEX large swaps via Alchemy transfers
# ============================================================================
def scan_dex_swaps(state):
    """For each Uniswap V3 pool we track, fetch recent erc20 transfers
    to/from the pool. Large transfers (≥ $1M) = the swap volume.

    Each swap involves 2 transfers (token_in → pool, pool → token_out).
    We count one entry per pair via dedupe by tx hash.
    """
    last_block = state.get("last_block", {}).get("dex", 0)
    new_count = 0
    seen_tx = set()
    max_block_seen = last_block

    for pool_label, pool_addr in UNISWAP_POOLS.items():
        # Pull recent transfers involving this pool
        r = alchemy_rpc("alchemy_getAssetTransfers", [{
            "fromBlock":     hex(last_block + 1) if last_block else "0x14F0000",
            "toBlock":       "latest",
            "toAddress":     pool_addr,
            "category":      ["erc20"],
            "maxCount":      "0x32",
            "order":         "desc",
            "withMetadata":  True,
        }])
        _metrics["dex_scans"] += 1
        if not r or "result" not in r:
            continue
        transfers = (r["result"] or {}).get("transfers", [])
        for tx in transfers:
            txid = tx.get("hash")
            if not txid or txid in seen_tx:
                continue
            seen_tx.add(txid)
            try:
                blk = int(tx.get("blockNum", "0x0"), 16)
            except (ValueError, TypeError):
                continue
            max_block_seen = max(max_block_seen, blk)
            asset = (tx.get("asset") or "").upper()
            try:
                value = float(tx.get("value", 0))
            except (ValueError, TypeError):
                continue

            # Convert to USD
            if asset in ("USDC", "USDT", "DAI"):
                value_usd = value
            elif asset == "WBTC":
                value_usd = value * 81850
            elif asset == "WETH":
                value_usd = value * 3000   # rough; should look up but ok for filter
            else:
                continue

            if value_usd < DEX_SWAP_THRESHOLD_USD:
                continue

            ev = {
                "ts":         int(time.time()),
                "ts_utc":     dt.datetime.now(dt.timezone.utc).isoformat(),
                "block":      blk,
                "pool":       pool_label,
                "pool_addr":  pool_addr,
                "asset":      asset,
                "value_native": value,
                "value_usd":  round(value_usd, 0),
                "tx_hash":    txid,
                "from":       tx.get("from"),
                "to":         tx.get("to"),
                "direction":  f"into_pool ({asset})",
            }
            state.setdefault("recent_dex", []).append(ev)
            new_count += 1
            _metrics["dex_events"] += 1
            head = (f"DEX_SWAP {pool_label}  {asset} {value:,.4f}  "
                    f"~${value_usd/1e6:.2f}M  block={blk}")
            emit_swarm("evm.dex_swap", head, {
                **ev,
                "type":       "DEX_LARGE_SWAP",
                "confidence": 75,
            }, ["evm", "dex", "uniswap", asset])
            print(f"  [DEX] {head}", flush=True)
        time.sleep(0.3)

    if max_block_seen > last_block:
        state.setdefault("last_block", {})["dex"] = max_block_seen
    return new_count


# ============================================================================
# TRACK 2 — Bridge events via Etherscan
# ============================================================================
def scan_bridge_flows(state):
    """For each bridge router, pull recent ERC20 transfers to/from the router
    address. Large transfers = bridge capital migration.
    """
    last_block = state.get("last_block", {}).get("bridge", 0)
    new_count = 0
    max_block_seen = last_block

    # Limit to USDT/USDC/WBTC tokens for relevance
    for token_addr, token_decimals, token_symbol in [
        (USDT, 6, "USDT"),
        (USDC, 6, "USDC"),
        (WBTC, 8, "WBTC"),
    ]:
        for bridge_label, bridge_addr in BRIDGE_ROUTERS.items():
            qs = urllib.parse.urlencode({
                "chainid":          "1",
                "module":           "account",
                "action":           "tokentx",
                "contractaddress":  token_addr,
                "address":          bridge_addr,
                "startblock":       str(last_block) if last_block else "0",
                "endblock":         "99999999",
                "sort":             "desc",
                "page":             "1",
                "offset":           "20",
                "apikey":           ETHERSCAN_KEY,
            })
            r = _http_get_json(f"{ETHERSCAN_V2}?{qs}")
            _metrics["bridge_scans"] += 1
            time.sleep(0.3)
            if not r:
                continue
            txs = r.get("result") if isinstance(r.get("result"), list) else []
            for tx in txs[:20]:
                try:
                    blk = int(tx.get("blockNumber", 0))
                except (ValueError, TypeError):
                    continue
                max_block_seen = max(max_block_seen, blk)
                if blk <= last_block:
                    continue
                try:
                    raw_value = int(tx.get("value", 0))
                except (ValueError, TypeError):
                    continue
                value_native = raw_value / 10**token_decimals
                if token_symbol == "WBTC":
                    value_usd = value_native * 81850
                else:
                    value_usd = value_native
                if value_usd < BRIDGE_FLOW_THRESHOLD_USD:
                    continue
                from_a = (tx.get("from") or "").lower()
                to_a   = (tx.get("to") or "").lower()
                direction = ("incoming" if to_a == bridge_addr.lower()
                              else "outgoing")
                ts = int(tx.get("timeStamp", 0))
                ev = {
                    "ts":           ts,
                    "ts_utc":       dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat(),
                    "block":        blk,
                    "bridge":       bridge_label,
                    "bridge_addr":  bridge_addr,
                    "token":        token_symbol,
                    "value_native": value_native,
                    "value_usd":    round(value_usd, 0),
                    "direction":    direction,
                    "tx_hash":      tx.get("hash"),
                    "from":         tx.get("from"),
                    "to":           tx.get("to"),
                }
                state.setdefault("recent_bridge", []).append(ev)
                new_count += 1
                _metrics["bridge_events"] += 1
                head = (f"BRIDGE_{direction.upper()} {bridge_label}  "
                        f"{token_symbol} {value_native:,.2f}  "
                        f"~${value_usd/1e6:.2f}M  block={blk}")
                emit_swarm("evm.bridge_flow", head, {
                    **ev,
                    "type":       f"BRIDGE_{direction.upper()}",
                    "confidence": 80,
                }, ["evm", "bridge", bridge_label, token_symbol])
                print(f"  [BRIDGE] {head}", flush=True)

    if max_block_seen > last_block:
        state.setdefault("last_block", {})["bridge"] = max_block_seen
    return new_count


# ============================================================================
# Main
# ============================================================================
def main():
    global _running
    print(f"=== sygnif_evm_extras started @ "
          f"{dt.datetime.now(dt.timezone.utc).isoformat()} ===", flush=True)
    print(f"  etherscan:  {'OK' if ETHERSCAN_KEY else 'MISSING'}", flush=True)
    print(f"  alchemy:    {'OK' if ALCHEMY_KEY else 'MISSING'}", flush=True)
    print(f"  dex pools:  {len(UNISWAP_POOLS)}", flush=True)
    print(f"  bridges:    {len(BRIDGE_ROUTERS)}", flush=True)
    print(f"  poll:       {POLL_S}s", flush=True)
    print(f"  dex thr:    ${DEX_SWAP_THRESHOLD_USD/1e6:.1f}M", flush=True)
    print(f"  bridge thr: ${BRIDGE_FLOW_THRESHOLD_USD/1e6:.1f}M", flush=True)

    state = load_state()
    print(f"  loaded: dex_block={state.get('last_block',{}).get('dex')} "
          f"bridge_block={state.get('last_block',{}).get('bridge')}", flush=True)

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
                if ALCHEMY_KEY:
                    scan_dex_swaps(state)
                if ETHERSCAN_KEY:
                    scan_bridge_flows(state)
            except Exception as e:
                print(f"  ! scan err: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_poll = now
        if now - last_save >= 120:
            try:
                save_state(state)
            except Exception as e:
                print(f"  ! save err: {e}", file=sys.stderr, flush=True)
            last_save = now
        time.sleep(3)

    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
