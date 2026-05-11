#!/usr/bin/env python3
"""sygnif_chain_seed.py — One-shot bootstrap for chain intelligence.

Run once at install time, then optionally daily to refresh sanctions list.

Tasks:
  1. Pull OFAC-sanctioned BTC addresses from public github mirrors.
     Try multiple sources, dedupe, write to /var/lib/sygnif/sanctioned_addresses.txt
  2. Hardcoded fallback set of known Tornado Cash / Lazarus-attributed BTC addrs.
  3. Bootstrap known-exchange registry into chain_state.json wallets section
     if state is empty (otherwise leave it alone).
"""
import json, pathlib, urllib.request, datetime as dt, time, sys

SANCTIONS_FILE = pathlib.Path("/var/lib/sygnif/sanctioned_addresses.txt")
STATE_FILE     = pathlib.Path("/var/lib/sygnif/chain_state.json")
HEADERS        = {"User-Agent": "sygnif-chain-seed/1.0"}

OFAC_SOURCES = [
    "https://raw.githubusercontent.com/0xB10C/ofac-sanctioned-digital-currency-addresses/lists/sanctioned_addresses_BTC.txt",
    "https://raw.githubusercontent.com/0xB10C/ofac-sanctioned-digital-currency-addresses/master/sanctioned_addresses_BTC.txt",
    "https://raw.githubusercontent.com/ultrasoundmoney/ofac-sanctioned-digital-currency-addresses/main/data/sanctioned_addresses_BTC.txt",
    "https://raw.githubusercontent.com/0xB10C/ofac-sanctioned-digital-currency-addresses/lists/sanctioned_addresses_XBT.txt",
]

# Manual fallback: Lazarus group, North Korea, Tornado Cash adjacency
HARDCODED_KNOWN_BAD = [
    # Lazarus group BTC clusters (US OFAC, multiple sources)
    "1F1tAaz5x1HUXrCNLbtMDqcw6o5GNn4xqX",   # Ronin bridge exploit cluster
    "12iEvAhPMNGbjANCqxyrTaCi5xMNTfvHra",
    "1Hap9XLZdfTfQU7uHzGZ7sBSDX3RhGY3vC",
    "12NoUyVjRGCBmm1iCmaGEgCM9bcQzbCsCv",
    # Hydra Market (sanctioned 2022)
    "bc1q4ydcl5fce4u36am5d4r5h6w9tw7q5n9f4tnv6e",  # cluster-attributed
    # Tornado Cash BTC-bridged contracts (lower confidence)
]


def jget_text(url: str, timeout: int = 15) -> str | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        return urllib.request.urlopen(req, timeout=timeout).read().decode(
            "utf-8", errors="ignore")
    except Exception as e:
        print(f"  ! GET {url[:80]} — {type(e).__name__}: {e}", file=sys.stderr)
        return None


def import_sanctions() -> int:
    """Fetch sanctioned addresses from public sources, dedupe, write to file."""
    addrs = set()
    sources_used = []
    for src in OFAC_SOURCES:
        body = jget_text(src)
        if not body:
            continue
        before = len(addrs)
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            # Basic sanity check: BTC addresses are 26-62 chars
            if 26 <= len(line) <= 64 and (line[0] in "13b" or line.startswith("bc1")):
                addrs.add(line)
        added = len(addrs) - before
        if added > 0:
            sources_used.append((src, added))
            print(f"  + {added} from {src[:60]}...")
        time.sleep(0.5)

    # Add hardcoded fallback
    before = len(addrs)
    for a in HARDCODED_KNOWN_BAD:
        addrs.add(a)
    print(f"  + {len(addrs)-before} from hardcoded known-bad list")

    # Write
    SANCTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# SYGNIF sanctioned BTC addresses",
        f"# generated: {dt.datetime.now(dt.timezone.utc).isoformat()}",
        f"# sources used: {len(sources_used)} github mirrors + hardcoded",
        "",
    ]
    for s, n in sources_used:
        lines.append(f"# - {s} ({n} addrs)")
    lines.append("")
    lines.extend(sorted(addrs))
    SANCTIONS_FILE.write_text("\n".join(lines) + "\n")
    print(f"  → wrote {len(addrs)} addresses to {SANCTIONS_FILE}")
    return len(addrs)


def bootstrap_state():
    """If chain_state.json doesn't exist, seed with known exchange addresses.
    If it exists, do nothing (don't overwrite running state)."""
    if STATE_FILE.exists():
        # Check if it's empty / new
        try:
            s = json.loads(STATE_FILE.read_text())
            if len(s.get("wallets", {})) > 5:
                print(f"  state has {len(s['wallets'])} wallets — skip bootstrap")
                return
        except Exception:
            pass

    # Build initial state with known anchors
    KNOWN = {
        "3Mvtgmu8s8FjpdABqdmKaTYhDjnRu7eERN": ("EXCHANGE", "Coinbase", 95),
        "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR": ("EXCHANGE", "Coinbase", 95),
        "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo": ("EXCHANGE", "Binance Cold", 95),
        "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s": ("EXCHANGE", "Binance", 95),
        "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h": ("EXCHANGE", "Binance Hot", 90),
        "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g": ("EXCHANGE", "Bitfinex Hot", 90),
        "bc1qrh99vw0ujsy9plhpf95dcyzj0jvc5lfvxsq3qm": ("EXCHANGE", "Bybit", 80),
        "bc1qrsuxwwzwzy9rt0xytsen8w8t4puzeuwq7p83ar": ("EXCHANGE", "OKX", 80),
        "37XuVSEpWW4trkfmvWzegTHQt7BdktSKUs": ("EXCHANGE", "Kraken", 85),
        "bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfc27a4":
            ("EXCHANGE", "Bitfinex Cold (Tether-related)", 95),
        "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF": ("EXCHANGE", "Mt.Gox Trustee", 90),
    }
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    wallets = {}
    for addr, (tier, label, conf) in KNOWN.items():
        wallets[addr] = {
            "addr":           addr,
            "tier":           tier,
            "label":          label,
            "confidence":     conf,
            "cluster_id":     None,
            "balance_btc":    0,
            "n_tx":           0,
            "total_received_btc": 0,
            "total_sent_btc": 0,
            "first_seen_at":  now,
            "last_seen_at":   now,
            "last_block":     0,
            "notes":          "seeded from known-exchange registry",
        }
    state = {
        "schema":              "sygnif.chain_intel.v1",
        "created_at_utc":      now,
        "last_block_height":   0,
        "wallets":             wallets,
        "clusters":            {},
        "addr_to_cluster":     {},
        "recent_events":       [],
        "peeling_chains":      {},
        "metrics":             {},
    }
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, default=str, indent=2))
    print(f"  → bootstrapped state with {len(wallets)} known anchors")


def main():
    print(f"=== sygnif_chain_seed @ {dt.datetime.now(dt.timezone.utc).isoformat()} ===")
    print(f"\n[1] Bootstrap state file")
    bootstrap_state()
    print(f"\n[2] Import OFAC sanctioned BTC addresses")
    n = import_sanctions()
    print(f"\n  done: {n} sanctioned addresses loaded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
