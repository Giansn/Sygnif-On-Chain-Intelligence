#!/usr/bin/env python3
"""sygnif_ecosystem.py — Tonight's final build. Unified market-depth daemon.

Three data sources, three cadences:

  TRACK A — DefiLlama (every 15min, no key)
    /v2/historicalChainTvl/{chain}       chain-level TVL over time
    /stablecoins                         total stablecoin caps per chain
    /stablecoinprices                    USDT/USDC peg deltas
    Why: "where is the money sitting" — when USDT supply on Ethereum jumps
    +$2B, capital is queueing on ETH. Cross-chain rotation = early signal.

  TRACK B — CoinGecko (every 5min, no key for these endpoints)
    /global                              total mkt cap + BTC dominance %
    /companies/public_treasury/bitcoin   public-company BTC holdings (MSTR etc)
    Why: BTC dominance is THE alt-vs-BTC season indicator. When dominance
    rolls over from peak, alts run; when it bottoms, BTC takes back.

  TRACK C — Goldrush / Covalent (every 1h, free-tier key)
    /v1/{chain}/address/{addr}/balances_v2/  per-address portfolio
    /v1/allchains/address/{addr}/balances/   single-call cross-chain view
    Why: unified BTC + EVM + 56 other chains. Lets us track institutional
    addresses (BlackRock IBIT, MicroStrategy known wallets, Saylor's hot
    wallets) with one API and get $USD-denominated portfolio snapshots.

State file: /var/lib/sygnif/ecosystem_state.json
Swarm topics:
  ecosystem.stablecoin_supply    per-chain stablecoin cap deltas
  ecosystem.btc_dominance        dominance shifts
  ecosystem.entity_portfolio     known-entity multi-chain balance changes
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

GOLDRUSH_KEY        = os.environ.get("SYGNIF_GOLDRUSH_KEY", "")
DEFILLAMA_POLL_S    = float(os.environ.get("SYGNIF_DEFILLAMA_POLL_S", "900"))   # 15 min
COINGECKO_POLL_S    = float(os.environ.get("SYGNIF_COINGECKO_POLL_S", "300"))   # 5 min
GOLDRUSH_POLL_S     = float(os.environ.get("SYGNIF_GOLDRUSH_POLL_S", "3600"))   # 1 h

STABLE_DELTA_USD    = float(os.environ.get("SYGNIF_STABLE_DELTA_USD", "100000000"))   # $100M
DOMINANCE_DELTA_PCT = float(os.environ.get("SYGNIF_DOMINANCE_DELTA_PCT", "0.3"))

DB_PATH       = "/var/lib/sygnif/swarm.db"
STATE_FILE    = pathlib.Path("/var/lib/sygnif/ecosystem_state.json")

DEFILLAMA_BASE = "https://api.llama.fi"
STABLECOINS_BASE = "https://stablecoins.llama.fi"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
GOLDRUSH_BASE  = "https://api.covalenthq.com/v1"

HEADERS = {"User-Agent": "sygnif-ecosystem/1.0"}
GR_HEADERS = {**HEADERS, "Authorization": f"Bearer {GOLDRUSH_KEY}"} if GOLDRUSH_KEY else HEADERS

# Known institutional addresses we want cross-chain balance snapshots for
# These get a Goldrush all-chains lookup each hour
TRACKED_ENTITIES = {
    "MicroStrategy (cold)":        ("eth-mainnet",   "0xCa15A4E22Eed7C3aA8DD7a3d3D0c3a35C2bE0e84"),
    "Bitfinex BTC cold":           ("btc-mainnet",   "bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfc27a4"),
    "Tonight's cold-accumulator":  ("btc-mainnet",   "bc1qgwp9hkue8wy3entawhs96mf2ptpvdg44823gj7"),
    "Binance hot (USDT)":          ("eth-mainnet",   "0x28C6c06298d514Db089934071355E5743bf21d60"),
    "Binance cold (ETH)":          ("eth-mainnet",   "0xF977814e90dA44bFA03b6295A0616a897441aceC"),
}

# Chains we care about for stablecoin supply tracking
TRACKED_CHAINS = ["Ethereum", "Tron", "Solana", "BSC", "Arbitrum", "Avalanche",
                   "Polygon", "Base", "Optimism"]

_running = True
_metrics = defaultdict(int)
_metrics["started_at"] = time.time()


# ============================================================================
# HTTP
# ============================================================================
def _jget(url: str, headers=None, timeout: int = 15):
    try:
        h = headers or HEADERS
        req = urllib.request.Request(url, headers=h)
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        _metrics["http_failures"] += 1
        return None


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
             "sygnif-ecosystem", topic, content,
             json.dumps(meta, default=str), json.dumps(tags)))
        c.commit()
        c.close()
        _metrics["swarm_emits"] += 1
    except Exception as e:
        print(f"  ! swarm err: {e}", file=sys.stderr, flush=True)


def load_state():
    if not STATE_FILE.exists():
        return _new_state()
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return _new_state()


def _new_state():
    return {
        "schema":              "sygnif.ecosystem.v1",
        "created_at_utc":      dt.datetime.now(dt.timezone.utc).isoformat(),
        "stablecoin_history":  [],      # rolling snapshots
        "dominance_history":   [],
        "entity_portfolios":   {},      # entity → last snapshot
        "metrics":             {},
    }


def save_state(state):
    state["updated_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    state["stablecoin_history"] = state.get("stablecoin_history", [])[-200:]
    state["dominance_history"]  = state.get("dominance_history", [])[-720:]
    state["metrics"] = dict(_metrics)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state, default=str, indent=2))
    os.replace(tmp, STATE_FILE)


# ============================================================================
# TRACK A — DefiLlama stablecoin supply per chain
# ============================================================================
def scan_stablecoin_chains(state):
    """Pull total stablecoin supply per chain. Detect ≥$100M deltas."""
    r = _jget(f"{STABLECOINS_BASE}/stablecoinchains")
    _metrics["defillama_polls"] += 1
    if not isinstance(r, list):
        return 0

    # Filter to our tracked chains
    snap = {}
    for entry in r:
        name = entry.get("name")
        if name not in TRACKED_CHAINS:
            continue
        circ = entry.get("totalCirculatingUSD", {})
        peg_total = sum(circ.values()) if isinstance(circ, dict) else 0
        prev_circ = entry.get("totalCirculatingUSD_old", {})
        prev_total = sum(prev_circ.values()) if isinstance(prev_circ, dict) else 0
        snap[name] = {
            "total_usd":      round(peg_total, 0),
            "prev_total_usd": round(prev_total, 0),
            "by_peg":         {k: round(v, 0) for k, v in (circ or {}).items()},
        }

    history = state.get("stablecoin_history", [])
    prev = history[-1] if history else None
    now = int(time.time())
    snap_entry = {
        "ts":     now,
        "ts_utc": dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        "chains": snap,
    }
    history.append(snap_entry)
    state["stablecoin_history"] = history

    if prev:
        for chain, data in snap.items():
            prev_chain = (prev.get("chains") or {}).get(chain)
            if not prev_chain:
                continue
            delta_usd = data["total_usd"] - prev_chain.get("total_usd", 0)
            if abs(delta_usd) >= STABLE_DELTA_USD:
                head = (f"STABLECOIN_SUPPLY {chain} "
                        f"{delta_usd/1e6:+,.0f}M USD  "
                        f"now ${data['total_usd']/1e9:,.2f}B")
                emit_swarm("ecosystem.stablecoin_supply", head, {
                    "chain":      chain,
                    "delta_usd":  delta_usd,
                    "current_usd": data["total_usd"],
                    "by_peg":     data["by_peg"],
                    "type":       "STABLECOIN_DELTA",
                    "confidence": 90,
                }, ["ecosystem", "stablecoin", chain])
                print(f"  [STABLE] {head}", flush=True)
                _metrics["stablecoin_alerts"] += 1
    else:
        # Baseline emit
        total_tracked = sum(c["total_usd"] for c in snap.values())
        head = (f"STABLECOIN_BASELINE  {len(snap)} chains  "
                f"total tracked: ${total_tracked/1e9:,.1f}B")
        emit_swarm("ecosystem.stablecoin_supply", head, {
            "type": "STABLECOIN_BASELINE",
            "chains_snapshot": snap,
            "confidence": 95,
        }, ["ecosystem", "stablecoin", "baseline"])
        print(f"  [STABLE] {head}", flush=True)
    return len(snap)


def scan_stablecoin_pegs(state):
    """USDT/USDC peg deltas (depeg alarm)."""
    r = _jget(f"{STABLECOINS_BASE}/stablecoinprices")
    if not isinstance(r, list):
        return 0
    important = {"tether": "USDT", "usd-coin": "USDC", "dai": "DAI"}
    for entry in r[-1:]:   # most recent timestamp snapshot
        prices = entry.get("prices", {})
        for cg_id, sym in important.items():
            price = prices.get(cg_id)
            if not price:
                continue
            deviation = (price - 1.0) * 10000   # bps from $1.00
            if abs(deviation) >= 50:   # ≥50bps depeg alert
                head = f"STABLECOIN_DEPEG {sym}  ${price:.4f}  ({deviation:+.0f}bps off peg)"
                emit_swarm("ecosystem.stablecoin_peg", head, {
                    "stablecoin":  sym,
                    "price":       price,
                    "deviation_bps": deviation,
                    "type":        "STABLECOIN_DEPEG",
                    "confidence":  100,
                }, ["ecosystem", "stablecoin", "depeg", sym])
                print(f"  [DEPEG] {head}", flush=True)
                _metrics["depeg_alerts"] += 1
    return 0


# ============================================================================
# TRACK B — CoinGecko global + dominance + public treasuries
# ============================================================================
def scan_coingecko_global(state):
    """BTC dominance + total mkt cap. Emit on ≥0.3% dominance shift."""
    r = _jget(f"{COINGECKO_BASE}/global")
    _metrics["coingecko_polls"] += 1
    if not r:
        return 0
    data = r.get("data", {})
    if not data:
        return 0
    mcap_pct = data.get("market_cap_percentage", {})
    btc_dom = mcap_pct.get("btc")
    eth_dom = mcap_pct.get("eth")
    total_mcap_usd = (data.get("total_market_cap", {}) or {}).get("usd", 0)
    total_vol = (data.get("total_volume", {}) or {}).get("usd", 0)
    mcap_24h_chg = data.get("market_cap_change_percentage_24h_usd", 0)

    now = int(time.time())
    snap = {
        "ts":           now,
        "ts_utc":       dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
        "btc_dom":      round(btc_dom, 3) if btc_dom else None,
        "eth_dom":      round(eth_dom, 3) if eth_dom else None,
        "total_mcap":   total_mcap_usd,
        "total_vol_24h": total_vol,
        "mcap_chg_24h_pct": round(mcap_24h_chg, 3),
    }
    history = state.get("dominance_history", [])
    prev = history[-1] if history else None
    history.append(snap)
    state["dominance_history"] = history

    if prev and btc_dom and prev.get("btc_dom"):
        delta = btc_dom - prev["btc_dom"]
        if abs(delta) >= DOMINANCE_DELTA_PCT:
            direction = "BTC_GAINING" if delta > 0 else "ALTS_GAINING"
            head = (f"BTC_DOMINANCE {btc_dom:.2f}% ({delta:+.2f}pp)  "
                    f"mkt cap ${total_mcap_usd/1e12:.2f}T  signal={direction}")
            emit_swarm("ecosystem.btc_dominance", head, {
                **snap,
                "delta_pp":   delta,
                "direction":  direction,
                "type":       "DOMINANCE_SHIFT",
                "confidence": 90,
            }, ["ecosystem", "dominance", direction])
            print(f"  [DOMINANCE] {head}", flush=True)
            _metrics["dominance_alerts"] += 1
    elif not prev:
        head = (f"DOMINANCE_BASELINE  BTC={btc_dom:.2f}%  ETH={eth_dom:.2f}%  "
                f"total ${total_mcap_usd/1e12:.2f}T  24h Δ {mcap_24h_chg:+.2f}%")
        emit_swarm("ecosystem.btc_dominance", head, {
            **snap,
            "type":       "DOMINANCE_BASELINE",
            "confidence": 95,
        }, ["ecosystem", "dominance", "baseline"])
        print(f"  [DOMINANCE] {head}", flush=True)
    return 1


def scan_public_treasuries(state):
    """Public-company BTC holdings (MSTR, Tesla, etc). Daily ish — 15-min poll OK."""
    r = _jget(f"{COINGECKO_BASE}/companies/public_treasury/bitcoin")
    if not r:
        return 0
    companies = r.get("companies", [])
    total_btc = r.get("total_holdings", 0)
    total_pct = r.get("total_value_usd", 0)
    market_cap_dominance = r.get("market_cap_dominance", 0)
    snap = {
        "ts":            int(time.time()),
        "total_holdings_btc":  total_btc,
        "total_holdings_usd":  total_pct,
        "market_cap_dominance": market_cap_dominance,
        "top_5":               [
            {"name": c.get("name"), "btc": c.get("total_holdings"),
             "country": c.get("country_code")}
            for c in companies[:5]
        ],
    }
    state["public_treasury_snapshot"] = snap
    return len(companies)


# ============================================================================
# TRACK C — Goldrush cross-chain entity portfolios
# ============================================================================
def scan_entity_portfolios(state):
    """For each tracked entity, fetch its on-chain balance via Goldrush.
    Emit on significant value deltas (≥$1M change)."""
    if not GOLDRUSH_KEY:
        return 0
    emits = 0
    portfolios = state.get("entity_portfolios", {})
    for label, (chain, addr) in TRACKED_ENTITIES.items():
        url = f"{GOLDRUSH_BASE}/{chain}/address/{addr}/balances_v2/?quote-currency=USD"
        r = _jget(url, headers=GR_HEADERS, timeout=20)
        _metrics["goldrush_polls"] += 1
        if not r or "data" not in r:
            continue
        items = (r.get("data") or {}).get("items") or []
        total_usd = 0
        holdings = []
        for it in items[:20]:
            q = it.get("quote") or 0
            if q < 1:
                continue
            sym = it.get("contract_ticker_symbol", "?")
            raw_bal = it.get("balance", "0")
            decimals = it.get("contract_decimals", 18)
            try:
                native_bal = int(raw_bal) / 10**decimals
            except (ValueError, TypeError):
                native_bal = 0
            total_usd += q
            holdings.append({
                "symbol":   sym,
                "balance":  native_bal,
                "quote_usd": round(q, 2),
            })

        prev = portfolios.get(label, {})
        prev_usd = prev.get("total_usd", 0)
        delta_usd = total_usd - prev_usd

        snap = {
            "label":     label,
            "chain":     chain,
            "address":   addr,
            "total_usd": round(total_usd, 0),
            "holdings":  holdings,
            "ts":        int(time.time()),
        }
        portfolios[label] = snap

        if prev and abs(delta_usd) >= 1_000_000:
            head = (f"ENTITY_PORTFOLIO {label}  Δ${delta_usd/1e6:+,.1f}M  "
                    f"now ${total_usd/1e6:,.1f}M  on {chain}")
            emit_swarm("ecosystem.entity_portfolio", head, {
                **snap,
                "previous_usd": prev_usd,
                "delta_usd":    delta_usd,
                "type":         "ENTITY_BALANCE_DELTA",
                "confidence":   95,
            }, ["ecosystem", "entity", label.split()[0]])
            print(f"  [ENTITY] {head}", flush=True)
            emits += 1
            _metrics["entity_alerts"] += 1
        elif not prev:
            head = (f"ENTITY_BASELINE {label}  ${total_usd/1e6:,.1f}M  on {chain}  "
                    f"({len(holdings)} tokens)")
            emit_swarm("ecosystem.entity_portfolio", head, {
                **snap,
                "type":       "ENTITY_BASELINE",
                "confidence": 95,
            }, ["ecosystem", "entity", "baseline"])
            print(f"  [ENTITY] {head}", flush=True)
            emits += 1
        time.sleep(0.5)

    state["entity_portfolios"] = portfolios
    return emits


# ============================================================================
# Main
# ============================================================================
def main():
    global _running
    print(f"=== sygnif_ecosystem started @ "
          f"{dt.datetime.now(dt.timezone.utc).isoformat()} ===", flush=True)
    print(f"  goldrush:        {'OK' if GOLDRUSH_KEY else 'MISSING'}", flush=True)
    print(f"  defillama poll:  {DEFILLAMA_POLL_S}s", flush=True)
    print(f"  coingecko poll:  {COINGECKO_POLL_S}s", flush=True)
    print(f"  goldrush poll:   {GOLDRUSH_POLL_S}s", flush=True)
    print(f"  stablecoin Δ:    ${STABLE_DELTA_USD/1e6:.0f}M", flush=True)
    print(f"  dominance Δ:     {DOMINANCE_DELTA_PCT}pp", flush=True)
    print(f"  tracked entities: {len(TRACKED_ENTITIES)}", flush=True)
    print(f"  tracked chains:   {len(TRACKED_CHAINS)}", flush=True)

    state = load_state()

    def _sigterm(sig, frame):
        global _running
        print(f"  signal {sig}", flush=True)
        _running = False
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT,  _sigterm)

    last_defillama = 0.0
    last_coingecko = 0.0
    last_goldrush = 0.0
    last_save = 0.0
    while _running:
        now = time.time()

        if now - last_defillama >= DEFILLAMA_POLL_S:
            try:
                scan_stablecoin_chains(state)
                scan_stablecoin_pegs(state)
            except Exception as e:
                print(f"  ! defillama err: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_defillama = now

        if now - last_coingecko >= COINGECKO_POLL_S:
            try:
                scan_coingecko_global(state)
                # public treasuries every 4th poll (~20min)
                if int(now) % 4 == 0:
                    scan_public_treasuries(state)
            except Exception as e:
                print(f"  ! coingecko err: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_coingecko = now

        if now - last_goldrush >= GOLDRUSH_POLL_S:
            try:
                scan_entity_portfolios(state)
            except Exception as e:
                print(f"  ! goldrush err: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            last_goldrush = now

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
