#!/usr/bin/env python3
"""sygnif_evm_signals.py — EVM-side market-depth signals daemon.

Bridges our BTC-focused chain-intel with EVM signals that lead/confirm BTC moves:

  TRACK 1 — Stablecoin mints
    USDT mints (Tether Treasury → external) — historically leads BTC bottoms 2-3d
    USDC mints (Circle Treasury → external) — institutional capital queuing
    Threshold: emit on ≥$10M individual events; cumulative tracked hourly

  TRACK 2 — WBTC mint/burn
    WBTC mint (to = 0x0…0) = BTC tokenized into ETH-side DeFi
    WBTC burn (from = 0x0…0) = WBTC redeemed back to native BTC
    Threshold: ≥10 BTC individual events

  TRACK 3 — Exchange reserves (USDT + USDC + WETH balances)
    Track top 6 CEX hot wallets on Ethereum mainnet.
    Snapshot hourly. Compute deltas.
    Rising USDT reserves at exchange = deposits queuing = buy pressure incoming
    Falling = withdrawals = users moving stablecoin out (less bullish)

Output: state file + swarm events.

API usage:
  - Etherscan V2 (free 5 req/sec, 100k/day) — primary
  - Alchemy (free 300M CU/month) — fallback / Enhanced API for asset transfers

State file: /var/lib/sygnif/evm_state.json
Log:        /var/lib/log/sygnif/evm-signals.log
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
ETHERSCAN_KEY = os.environ.get("SYGNIF_ETHERSCAN_KEY", "")
ALCHEMY_KEY   = os.environ.get("SYGNIF_ALCHEMY_KEY", "")

STABLECOIN_MINT_THRESHOLD_USD = float(os.environ.get("SYGNIF_STABLE_MINT_USD", "10000000"))   # $10M
WBTC_FLOW_THRESHOLD_BTC       = float(os.environ.get("SYGNIF_WBTC_THRESHOLD", "10"))
MINT_POLL_S                   = float(os.environ.get("SYGNIF_EVM_MINT_POLL_S", "300"))   # 5 min
RESERVE_POLL_S                = float(os.environ.get("SYGNIF_EVM_RESERVE_POLL_S", "3600"))   # 1h

DB_PATH   = "/var/lib/sygnif/swarm.db"
STATE_FILE = pathlib.Path("/var/lib/sygnif/evm_state.json")

ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
ALCHEMY_RPC  = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}" if ALCHEMY_KEY else None
HEADERS      = {"User-Agent": "sygnif-evm-signals/1.0"}

# ============================================================================
# Contracts + entities
# ============================================================================
USDT_CONTRACT      = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
USDC_CONTRACT      = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
WBTC_CONTRACT      = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
DAI_CONTRACT       = "0x6B175474E89094C44Da98b954EedeAC495271d0F"

# Treasury / mint-capable wallets
TETHER_TREASURY    = "0xC6CDE7C39eB2f0F0095F41570af89eFC2C1Ea828"
CIRCLE_TREASURY    = "0x55FE002aefF02F77364de339a1292923A15844B8"

NULL_ADDR          = "0x0000000000000000000000000000000000000000"

# Exchange hot wallets we track for reserve balances
EXCHANGE_WALLETS = {
    "Binance 14":         "0x28C6c06298d514Db089934071355E5743bf21d60",
    "Binance 7":          "0x564286362092D8e7936f0549571a803B203aAceD",
    "Binance Cold":       "0xF977814e90dA44bFA03b6295A0616a897441aceC",
    "Coinbase 1":         "0x71660c4005BA85c37ccec55d0C4493E66Fe775d3",
    "Coinbase 2":         "0x503828976D22510aad0201ac7EC88293211D23Da",
    "Kraken":             "0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0",
    "Bitfinex":           "0x742d35Cc6634C0532925a3b844Bc9e7595f5b9e1",
    "Bitfinex 2":         "0x876EabF441B2EE5B5b0554Fd502a8E0600950cFa",
    "OKX":                "0x6cC5F688a315f3dC28A7781717a9A798a59fDA7b",
}

# Tokens to balance-track per exchange
TRACKED_TOKENS = {
    "USDT":  (USDT_CONTRACT, 6),
    "USDC":  (USDC_CONTRACT, 6),
    "WBTC":  (WBTC_CONTRACT, 8),
    "DAI":   (DAI_CONTRACT, 18),
}

_running = True
_metrics = defaultdict(int)
_metrics["started_at"] = time.time()


# ============================================================================
# HTTP
# ============================================================================
def _http_get_json(url: str, timeout: int = 15) -> dict | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        _metrics["http_failures"] += 1
        print(f"  ! GET {url[:80]} — {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        return None


def _http_post_json(url: str, body: dict, timeout: int = 15) -> dict | None:
    try:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST",
                                       headers={**HEADERS,
                                                  "Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        _metrics["http_failures"] += 1
        return None


def es(action: str, params: dict, module: str = "account") -> dict | None:
    """Etherscan V2 API call. chainid=1 (Ethereum mainnet)."""
    q = {
        "chainid":  "1",
        "module":   module,
        "action":   action,
        "apikey":   ETHERSCAN_KEY,
        **params,
    }
    url = f"{ETHERSCAN_V2}?{urllib.parse.urlencode(q)}"
    r = _http_get_json(url)
    if r is None:
        return None
    if r.get("status") != "1" and r.get("message") != "OK":
        # Some endpoints return status=0 with empty result for no-data cases
        msg = r.get("message", "")
        if "No transactions" in msg or "No records" in msg:
            return {"status": "0", "result": []}
        print(f"  ! Etherscan {module}.{action} status={r.get('status')} msg={msg[:80]}",
              file=sys.stderr, flush=True)
    return r


def alchemy_rpc(method: str, params: list) -> dict | None:
    if not ALCHEMY_RPC:
        return None
    return _http_post_json(ALCHEMY_RPC, {
        "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
    })


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
        "schema":          "sygnif.evm_signals.v1",
        "created_at_utc":  dt.datetime.now(dt.timezone.utc).isoformat(),
        "last_mint_scan_block":   {"USDT": 0, "USDC": 0, "WBTC": 0},
        "exchange_reserves":      {},   # exchange → {token → {balance, ts}}
        "reserve_history":        [],   # rolling deltas
        "recent_mints":           [],   # rolling list of mint events
        "recent_wbtc":            [],
        "metrics":                {},
    }


def save_state(state: dict) -> None:
    state["updated_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    # Trim
    state["recent_mints"]   = state["recent_mints"][-200:]
    state["recent_wbtc"]    = state["recent_wbtc"][-100:]
    state["reserve_history"] = state["reserve_history"][-200:]
    state["metrics"] = dict(_metrics)

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, default=str, indent=2))
    os.replace(tmp, STATE_FILE)


# ============================================================================
# Swarm emission
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
             "sygnif-evm-signals", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm emit failed: {type(e).__name__}: {e}",
              file=sys.stderr, flush=True)


# ============================================================================
# TRACK 1 — Stablecoin mints (USDT, USDC)
# ============================================================================
def scan_stablecoin_mints(state: dict, token_symbol: str, treasury_addr: str,
                            contract: str, decimals: int) -> int:
    """Scan recent token transfers FROM the treasury wallet → external.

    Tether/Circle mint by minting to treasury, then sending out to depositors.
    The visible signal: treasury → external transfer = freshly issued tokens
    being deployed.

    Threshold: ≥ $10M per event for swarm emission.
    """
    last_block = (state["last_mint_scan_block"] or {}).get(token_symbol, 0)
    r = es("tokentx", {
        "contractaddress": contract,
        "address":         treasury_addr,
        "startblock":      str(last_block),
        "endblock":        "99999999",
        "sort":            "desc",
        "page":            "1",
        "offset":          "100",
    })
    _metrics[f"mint_scans_{token_symbol}"] += 1
    if not r or not isinstance(r.get("result"), list):
        return 0

    txs = r["result"]
    new_events = 0
    max_block_seen = last_block
    for tx in txs:
        try:
            blk = int(tx.get("blockNumber", 0))
        except (ValueError, TypeError):
            continue
        max_block_seen = max(max_block_seen, blk)
        if blk <= last_block:
            continue
        # Only count treasury → external direction (outgoing = deployed)
        if (tx.get("from", "") or "").lower() != treasury_addr.lower():
            continue
        try:
            raw_value = int(tx.get("value", 0))
        except (ValueError, TypeError):
            continue
        value_usd = raw_value / 10**decimals  # USDT/USDC are ~$1
        if value_usd < STABLECOIN_MINT_THRESHOLD_USD:
            continue
        ts = int(tx.get("timeStamp", 0))
        ev = {
            "ts":         ts,
            "ts_utc":     dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat(),
            "block":      blk,
            "token":      token_symbol,
            "amount_usd": round(value_usd, 0),
            "tx_hash":    tx.get("hash"),
            "from":       tx.get("from"),
            "to":         tx.get("to"),
            "direction":  "treasury_to_external",
        }
        state["recent_mints"].append(ev)
        new_events += 1
        _metrics[f"mints_seen_{token_symbol}"] += 1

        head = (f"{token_symbol}_MINT {value_usd/1e6:,.0f}M USD  "
                f"treasury → {(tx.get('to','') or '')[:14]}...  "
                f"block={blk}")
        emit_swarm("evm.stablecoin_mint", head, {
            **ev,
            "type":       f"{token_symbol}_TREASURY_OUT",
            "confidence": 90,  # confirmed mint event from treasury
        }, ["evm", "stablecoin", "mint", token_symbol])
        print(f"  [MINT] {head}", flush=True)

    if max_block_seen > last_block:
        state["last_mint_scan_block"][token_symbol] = max_block_seen
    return new_events


# ============================================================================
# TRACK 2 — WBTC mint / burn flows
# ============================================================================
def scan_wbtc_flows(state: dict) -> int:
    """WBTC: mint events appear with from = 0x0…0, burn with to = 0x0…0.

    Alchemy's alchemy_getAssetTransfers can filter by these directions
    efficiently.
    """
    last_block_hex = hex((state["last_mint_scan_block"] or {}).get("WBTC", 0) + 1)
    r = alchemy_rpc("alchemy_getAssetTransfers", [{
        "fromBlock":     last_block_hex if last_block_hex != "0x1" else "0x14F0000",
        "toBlock":       "latest",
        "contractAddresses": [WBTC_CONTRACT],
        "category":      ["erc20"],
        "maxCount":      "0x32",   # 50
        "order":         "desc",
    }])
    _metrics["wbtc_scans"] += 1
    if not r or "result" not in r:
        return 0

    transfers = r["result"].get("transfers", [])
    new_events = 0
    max_block_seen = (state["last_mint_scan_block"] or {}).get("WBTC", 0)
    for tx in transfers:
        try:
            blk = int(tx.get("blockNum", "0x0"), 16)
        except (ValueError, TypeError):
            continue
        max_block_seen = max(max_block_seen, blk)
        if blk <= state["last_mint_scan_block"].get("WBTC", 0):
            continue
        from_a = (tx.get("from") or "").lower()
        to_a   = (tx.get("to") or "").lower()
        is_mint = from_a == NULL_ADDR
        is_burn = to_a == NULL_ADDR
        if not (is_mint or is_burn):
            continue
        try:
            value_btc = float(tx.get("value", 0))
        except (ValueError, TypeError):
            continue
        if value_btc < WBTC_FLOW_THRESHOLD_BTC:
            continue
        direction = "MINT" if is_mint else "BURN"
        ev = {
            "ts":         int(time.time()),
            "ts_utc":     dt.datetime.now(dt.timezone.utc).isoformat(),
            "block":      blk,
            "direction":  direction,
            "value_btc":  value_btc,
            "value_usd":  round(value_btc * 81850, 0),
            "tx_hash":    tx.get("hash"),
            "counterparty": to_a if is_mint else from_a,
        }
        state["recent_wbtc"].append(ev)
        new_events += 1
        _metrics[f"wbtc_{direction.lower()}_seen"] += 1
        head = (f"WBTC_{direction}  {value_btc:,.2f} BTC (~${value_btc*81850/1e6:.1f}M)  "
                f"counterparty={ev['counterparty'][:14]}...  block={blk}")
        emit_swarm("evm.wbtc_flow", head, {
            **ev,
            "type":       f"WBTC_{direction}",
            "confidence": 95,
        }, ["evm", "wbtc", direction.lower()])
        print(f"  [WBTC] {head}", flush=True)

    if max_block_seen > state["last_mint_scan_block"].get("WBTC", 0):
        state["last_mint_scan_block"]["WBTC"] = max_block_seen
    return new_events


# ============================================================================
# TRACK 3 — Exchange reserves snapshot
# ============================================================================
def snapshot_exchange_reserves(state: dict) -> int:
    """For each exchange hot wallet, fetch USDT/USDC/WBTC/DAI balances.
    Compare to previous snapshot, emit deltas. Cache in state.
    """
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    now_ts = int(time.time())
    prev = state.get("exchange_reserves") or {}
    snapshot = {}
    emits = 0

    for exch_name, wallet in EXCHANGE_WALLETS.items():
        snapshot[exch_name] = {"wallet": wallet, "balances": {}, "ts": now_ts}
        for token_sym, (contract, decimals) in TRACKED_TOKENS.items():
            r = es("tokenbalance", {
                "contractaddress": contract,
                "address":         wallet,
                "tag":             "latest",
            })
            _metrics["balance_queries"] += 1
            if not r:
                continue
            try:
                raw = int(r.get("result", "0"))
            except (ValueError, TypeError):
                continue
            balance = raw / 10**decimals
            snapshot[exch_name]["balances"][token_sym] = round(balance, 2)
            # Compute delta vs previous
            prev_bal = ((prev.get(exch_name) or {}).get("balances") or {}).get(token_sym)
            if prev_bal is not None:
                delta = balance - prev_bal
                delta_pct = (delta / max(prev_bal, 1)) * 100
                # Emit on significant deltas: >$5M absolute or >3% relative
                usd_value = balance * (81850 if token_sym == "WBTC" else 1)
                delta_usd = delta * (81850 if token_sym == "WBTC" else 1)
                if abs(delta_usd) >= 5_000_000 or abs(delta_pct) >= 3.0:
                    head = (f"{exch_name} {token_sym} balance Δ "
                            f"{delta:+,.0f} ({delta_pct:+.1f}%)  "
                            f"current ${balance/1e6:,.1f}M")
                    emit_swarm("evm.exchange_reserve", head, {
                        "exchange":     exch_name,
                        "wallet":       wallet,
                        "token":        token_sym,
                        "previous":     prev_bal,
                        "current":      balance,
                        "delta":        delta,
                        "delta_pct":    delta_pct,
                        "delta_usd":    delta_usd,
                        "type":         f"RESERVE_{token_sym}_DELTA",
                        "ts":           now_ts,
                        "confidence":   95,
                    }, ["evm", "reserve", exch_name, token_sym])
                    print(f"  [RESERVE] {head}", flush=True)
                    emits += 1
            time.sleep(0.25)  # respect 5 req/sec Etherscan limit

    state["exchange_reserves"] = snapshot
    # Append to history (compact: just totals per snapshot)
    totals = {}
    for token in TRACKED_TOKENS:
        totals[token] = sum(s["balances"].get(token, 0)
                              for s in snapshot.values())
    state["reserve_history"].append({
        "ts":     now_ts,
        "ts_utc": now_iso,
        "totals": totals,
    })
    print(f"  [SNAPSHOT] reserves saved — totals USDT={totals.get('USDT',0)/1e6:,.0f}M "
          f"USDC={totals.get('USDC',0)/1e6:,.0f}M "
          f"WBTC={totals.get('WBTC',0):,.0f}", flush=True)
    return emits


# ============================================================================
# Main loop
# ============================================================================
def main() -> int:
    global _running
    print(f"=== sygnif_evm_signals started @ "
          f"{dt.datetime.now(dt.timezone.utc).isoformat()} ===", flush=True)
    print(f"  etherscan: {'OK' if ETHERSCAN_KEY else '✗ MISSING'}", flush=True)
    print(f"  alchemy:   {'OK' if ALCHEMY_KEY else '✗ MISSING'}", flush=True)
    print(f"  mint threshold:   ${STABLECOIN_MINT_THRESHOLD_USD/1e6:.0f}M", flush=True)
    print(f"  wbtc threshold:   {WBTC_FLOW_THRESHOLD_BTC} BTC", flush=True)
    print(f"  mint poll:        {MINT_POLL_S}s", flush=True)
    print(f"  reserve poll:     {RESERVE_POLL_S}s", flush=True)
    print(f"  tracking {len(EXCHANGE_WALLETS)} exchanges × {len(TRACKED_TOKENS)} tokens",
          flush=True)

    state = load_state()
    print(f"  loaded state: last_block={state.get('last_mint_scan_block')}", flush=True)

    def _sigterm(sig, frame):
        global _running
        print(f"  signal {sig}, shutting down", flush=True)
        _running = False
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT,  _sigterm)

    last_mint_poll = 0.0
    last_reserve_poll = 0.0
    last_save = 0.0

    while _running:
        now = time.time()

        if now - last_mint_poll >= MINT_POLL_S:
            try:
                if ETHERSCAN_KEY:
                    scan_stablecoin_mints(state, "USDT", TETHER_TREASURY,
                                            USDT_CONTRACT, 6)
                    time.sleep(0.5)
                    scan_stablecoin_mints(state, "USDC", CIRCLE_TREASURY,
                                            USDC_CONTRACT, 6)
                if ALCHEMY_KEY:
                    time.sleep(0.5)
                    scan_wbtc_flows(state)
            except Exception as e:
                print(f"  ! mint/wbtc scan error: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_mint_poll = now

        if now - last_reserve_poll >= RESERVE_POLL_S and ETHERSCAN_KEY:
            try:
                snapshot_exchange_reserves(state)
            except Exception as e:
                print(f"  ! reserve snapshot error: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_reserve_poll = now

        if now - last_save >= 300:
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
