#!/usr/bin/env python3
"""sygnif_chain_report.py — Query the chain intelligence registry.

Usage:
  python3 sygnif_chain_report.py                   # full report
  python3 sygnif_chain_report.py --top 30          # wallets only
  python3 sygnif_chain_report.py --events          # whale events only
  python3 sygnif_chain_report.py --mempool         # mempool watch only
  python3 sygnif_chain_report.py --clusters        # cluster digest only
  python3 sygnif_chain_report.py --dormancy        # dormancy break alerts only
  python3 sygnif_chain_report.py --peeling         # confirmed peeling chains
  python3 sygnif_chain_report.py --sanctions       # sanctioned wallet activity
  python3 sygnif_chain_report.py --json            # raw json dump
"""
import json, pathlib, sys, time
from collections import Counter, defaultdict

STATE = pathlib.Path("/var/lib/sygnif/chain_state.json")
MEMPOOL = pathlib.Path("/var/lib/sygnif/chain_mempool.json")
SANCTIONS = pathlib.Path("/var/lib/sygnif/sanctioned_addresses.txt")


def load():
    s = {}
    if STATE.exists():
        try: s = json.loads(STATE.read_text())
        except: pass
    m = {}
    if MEMPOOL.exists():
        try: m = json.loads(MEMPOOL.read_text())
        except: pass
    sanc = set()
    if SANCTIONS.exists():
        for line in SANCTIONS.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                sanc.add(line)
    return s, m, sanc


def fmt_btc(b): return f"{b:>11,.2f}"
def fmt_usd(b): return f"${b*81850/1e6:>6,.1f}M"


def main():
    args = sys.argv[1:]
    s, m, sanc = load()

    if "--json" in args:
        print(json.dumps(s, indent=2, default=str)); return 0

    top_n = 30
    for i, a in enumerate(args):
        if a == "--top" and i+1 < len(args):
            try: top_n = int(args[i+1])
            except: pass

    only_events  = "--events" in args
    only_mempool = "--mempool" in args
    only_clust   = "--clusters" in args
    only_dorm    = "--dormancy" in args
    only_peel    = "--peeling" in args
    only_sanc    = "--sanctions" in args

    show_all = not any([only_events, only_mempool, only_clust,
                         only_dorm, only_peel, only_sanc])

    print(f"\n=== SYGNIF chain intelligence @ {s.get('updated_at_utc','?')} ===")
    metrics = s.get("metrics", {}) or {}
    print(f"  blocks_scanned={metrics.get('blocks_scanned',0)}  "
          f"whale_txs={metrics.get('whale_txs_seen',0)}  "
          f"mempool_whales={metrics.get('mempool_whales_seen',0)}  "
          f"emits={metrics.get('swarm_emits',0)}")
    print(f"  utxo_lookups={metrics.get('utxo_lookups',0)}  "
          f"http_failures={metrics.get('http_failures',0)}")
    print(f"  wallets tracked: {len(s.get('wallets',{}))}  "
          f"clusters: {len(s.get('clusters',{}))}  "
          f"events: {len(s.get('recent_events',[]))}  "
          f"sanctioned addresses: {len(sanc)}  "
          f"mempool pending: {len(m)}")
    print(f"  last_block_height: {s.get('last_block_height',0)}")

    # =========== WALLET LEADERBOARD ===========
    if show_all or "--top" in args:
        print(f"\n— WALLET LEADERBOARD (top {top_n} by confidence × balance) —")
        wallets = list((s.get("wallets") or {}).values())
        ranked = sorted(wallets,
                          key=lambda w: (-w.get("confidence",0),
                                          -w.get("balance_btc",0)))
        print(f"  {'tier':<18s} {'conf':>5s} {'balance':>11s} {'n_tx':>9s} "
              f"{'cid':<11s} addr  label")
        for w in ranked[:top_n]:
            cid = (w.get("cluster_id") or "-")[:10]
            print(f"  {w.get('tier','?'):<18s} "
                  f"{w.get('confidence',0):>5d} "
                  f"{fmt_btc(w.get('balance_btc',0))} BTC "
                  f"{w.get('n_tx',0):>9,d} "
                  f"{cid:<11s} "
                  f"{w.get('addr','?')[:18]}...  "
                  f"{w.get('label','')[:48]}")

        ctr = Counter(w.get("tier") for w in wallets)
        print(f"\n  tier distribution:")
        for t, n in ctr.most_common():
            print(f"    {t}: {n}")

    # =========== CLUSTERS ===========
    if show_all or only_clust:
        print(f"\n— CLUSTERS (top 10 by size) —")
        clusters = (s.get("clusters") or {}).values()
        ranked = sorted(clusters, key=lambda c: -c.get("size", 0))
        for c in list(ranked)[:10]:
            label = c.get("label") or "unlabeled"
            print(f"  {c.get('id','?')[:10]}  size={c.get('size',0):>4d}  "
                  f"conf={c.get('confidence',0):>3d}  {label[:60]}")
            for a in (c.get("addrs") or [])[:5]:
                print(f"      · {a}")
            if len(c.get("addrs") or []) > 5:
                print(f"      · ...+{len(c['addrs'])-5} more")

    # =========== MEMPOOL ===========
    if show_all or only_mempool:
        print(f"\n— MEMPOOL WATCH (pending whale txs, last 2h) —")
        now = time.time()
        if not m:
            print("  (none)")
        else:
            for txid, w in sorted(m.items(),
                                    key=lambda kv: -kv[1].get("seen_at",0))[:15]:
                age_s = now - w.get("seen_at", 0)
                age_str = (f"{int(age_s)}s ago" if age_s < 600
                            else f"{int(age_s/60)}m ago")
                conf_str = ("CONFIRMED blk=" + str(w.get("confirmed_block"))
                            if w.get("confirmed") else "pending")
                print(f"  {age_str:>10s} {w.get('value_btc',0):>8,.1f} BTC "
                      f"{fmt_usd(w.get('value_btc',0))}  {conf_str:>16s}  "
                      f"tx={txid[:14]}...")
                for a in (w.get("from") or [])[:2]:
                    print(f"     FROM {a.get('v',0):>10,.2f}  {a.get('addr','?')[:18]}...")
                for a in (w.get("to") or [])[:2]:
                    print(f"     TO   {a.get('v',0):>10,.2f}  {a.get('addr','?')[:18]}...")

    # =========== EVENTS (recent whale txs) ===========
    if show_all or only_events:
        print(f"\n— RECENT WHALE EVENTS (last 15, sorted by recency) —")
        events = (s.get("recent_events") or [])[-15:][::-1]
        for e in events:
            ts = e.get("ts_utc","?")[:19]
            cat = e.get("category","?")
            v = e.get("value_btc",0)
            conf = e.get("confidence",0)
            flags = ",".join(e.get("flags") or [])
            print(f"  {ts}  {cat:<22s} {v:>7,.1f} BTC  "
                  f"{fmt_usd(v)}  conf={conf:>3d}  "
                  f"blk={e.get('block')}  {flags[:50]}")
            ua = e.get("utxo_age") or {}
            if ua.get("median_age_days") is not None:
                print(f"     UTXO age: median={ua.get('median_age_days',0):.0f}d  "
                      f"oldest={ua.get('oldest_age_days',0):.0f}d  "
                      f"LTH={ua.get('lth_pct')}%  "
                      f"dormancy_break={ua.get('dormancy_break_btc',0):,.1f} BTC")

    # =========== DORMANCY BREAKS ===========
    if only_dorm or show_all:
        print(f"\n— DORMANCY BREAKS (5+ year UTXOs spent) —")
        breaks = [e for e in (s.get("recent_events") or [])
                  if "DORMANCY_BREAK_5YR" in (e.get("flags") or [])]
        if not breaks:
            print("  (none observed)")
        else:
            for e in breaks[-10:][::-1]:
                ua = e.get("utxo_age") or {}
                print(f"  {e.get('ts_utc','?')[:19]}  blk={e.get('block')}  "
                      f"{e.get('value_btc',0):>7,.1f} BTC  "
                      f"oldest UTXO age={ua.get('oldest_age_days',0):,.0f}d "
                      f"({ua.get('oldest_age_days',0)/365:.1f}yr)")
                print(f"     tx={e.get('tx_hash','?')[:24]}  "
                      f"dormancy_break_btc={ua.get('dormancy_break_btc',0):,.1f}")

    # =========== PEELING CHAINS ===========
    if only_peel or show_all:
        print(f"\n— CONFIRMED PEELING CHAINS —")
        peels = [e for e in (s.get("recent_events") or [])
                 if e.get("peeling_event")]
        if not peels:
            print("  (none confirmed)")
        else:
            for e in peels[-10:][::-1]:
                p = e.get("peeling_event") or {}
                print(f"  {e.get('ts_utc','?')[:19]}  "
                      f"src={p.get('source_addr','?')[:18]}...  "
                      f"→ dst={p.get('dest_addr','?')[:18]}...  "
                      f"{p.get('n_chunks',0)} chunks  "
                      f"{p.get('total_btc',0):,.1f} BTC  "
                      f"({p.get('window_s',0)}s window)")

    # =========== SANCTIONS ===========
    if only_sanc or show_all:
        print(f"\n— SANCTIONED WALLET ACTIVITY —")
        wallets = (s.get("wallets") or {}).values()
        sanc_wallets = [w for w in wallets if w.get("tier") == "SANCTIONED"]
        print(f"  total sanctioned addresses loaded: {len(sanc)}")
        print(f"  sanctioned wallets seen on-chain: {len(sanc_wallets)}")
        if sanc_wallets:
            for w in sanc_wallets[:5]:
                print(f"     {w.get('addr','?')[:30]}... bal={w.get('balance_btc',0):,.2f} "
                      f"n_tx={w.get('n_tx',0):,}")

    # =========== CATEGORY SUMMARY ===========
    if show_all:
        events = s.get("recent_events") or []
        cat_count = Counter(e.get("category") for e in events)
        cat_btc = defaultdict(float)
        for e in events:
            cat_btc[e.get("category")] += e.get("value_btc", 0)
        print(f"\n— CATEGORY SUMMARY (lifetime) —")
        for c, n in cat_count.most_common():
            print(f"  {c:<24s} count={n:>4d}  total={cat_btc[c]:>10,.1f} BTC "
                  f"~{fmt_usd(cat_btc[c])}")
        # Bullish vs bearish net
        bullish = (cat_btc.get("WITHDRAWAL_FROM_EXCHANGE", 0)
                    + cat_btc.get("ACCUMULATION_TO_COLD", 0))
        bearish = cat_btc.get("DEPOSIT_TO_EXCHANGE", 0)
        net = bullish - bearish
        print(f"\n  net visible flow: {net:+,.1f} BTC ({fmt_usd(abs(net))} "
              f"{'BULLISH' if net >= 0 else 'BEARISH'})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
