# SYGNIF · On-Chain Intelligence

Self-hosted blockchain intelligence stack — seven Python daemons that ingest
free-tier blockchain and exchange data, classify it with Arkham-style
confidence scoring, and emit structured signals.

Built to feed a perp-trading system, but the data layer is exchange-agnostic.

---

## What this stack actually collects

| Daemon | Source(s) | Cadence | Signal |
|---|---|---|---|
| `sygnif_chain_intel` | mempool.space + blockchain.info + blockstream.info | 60s block, 15s mempool, 1h LN | BTC whale txs (≥50 BTC), UTXO age (LTH/STH split), dormancy breaks (≥5y), CIH wallet clustering, peeling-chain detection, CoinJoin/Wasabi/Whirlpool/JoinMarket fingerprints, OFAC sanctioned address tagging, Lightning ecosystem stats |
| `sygnif_evm_signals` | Etherscan V2 + Alchemy | 5m mints, 1h reserves | USDT / USDC mints from Tether/Circle treasuries, WBTC mint/burn (wrapped-BTC bridge flows), 9 exchange-cluster reserve snapshots (USDT/USDC/WBTC/DAI) |
| `sygnif_tron_signals` | TronGrid public v1 | 5m | USDT/USDC/USDD mint events on Tron (where most Tether liquidity actually lives) |
| `sygnif_xchg_liquidations` | Binance + OKX public WS | real-time | Forced liquidations across exchanges; cross-exchange cluster detection (when ≥2 venues liquidate same asset+side within 60s) |
| `sygnif_market_premium` | Coinbase + Binance + Bybit public REST | 60s | Coinbase USD vs Binance USDT premium (US institutional bid signal); Binance spot vs Bybit perp basis (perp backwardation = bullish) |
| `sygnif_evm_extras` | Etherscan V2 + Alchemy | 10m | Uniswap V3 large swaps (USDC/USDT/WBTC pools); cross-chain bridge flows (Stargate, Across, LayerZero, Wormhole) |
| `sygnif_ecosystem` | DefiLlama + CoinGecko + Goldrush/Covalent | 5m / 15m / 1h | Per-chain stablecoin supply, USDT/USDC peg deltas, BTC dominance & total market cap, cross-chain entity portfolios with USD valuation |

Every daemon writes:
- a structured JSON state file to `/var/lib/sygnif/`
- detected events to a SQLite swarm db at `/var/lib/sygnif/swarm.db` under
  daemon-specific topics (`chain.whale`, `evm.stablecoin_mint`,
  `tron.stablecoin_mint`, `xchg.liquidation`, `xchg.liquidation_cluster`,
  `market.premium`, `evm.dex_swap`, `evm.bridge_flow`,
  `ecosystem.stablecoin_supply`, `ecosystem.btc_dominance`,
  `ecosystem.entity_portfolio`, etc.)

Each event carries a `confidence` score (0–100, Arkham-style):
verified labels ≥95, predicted ≥80, heuristic ≥50.

---

## Why these primitives

**Bitcoin's UTXO model exposes coin age natively.** Each spent UTXO knows
when it was created. This lets `chain_intel` compute:

- **LTH (Long-Term Holder) %** — fraction of input value from UTXOs ≥155 days old
- **Dormancy breaks** — UTXOs ≥5 years old being spent (often precede cycle tops)
- **Median input age** — quick gut-check of who's spending today

This is the same metric Glassnode charges $39-799/mo for. We build it from
public blockchain.info data with `/rawtx/{tx_index}?format=json` lookups.

**Common-Input Heuristic (Meiklejohn et al)** — when N addresses spend
together in one transaction, they belong to one entity. `chain_intel`
applies this on every block scan, building a union-find cluster graph.
We extend it by walking each whale address's tx history via
`blockstream.info/api/address/{addr}/txs` to find ALL co-spenders, not
just today's block. This is how Arkham/Chainalysis/Nansen discover hidden
wallets — except we do it for free.

**Mempool pre-confirmation** — `chain_intel` polls
`mempool.space/api/mempool/recent` every 15s. Large pending txs are
detected ~30s–10min BEFORE block confirmation. This gives us a forward
view that whale-alert services and on-chain dashboards lack.

**Stablecoin mints are leading indicators** — Tether/Circle mint when
they have inbound USD wires from authorised participants (often
exchanges). A $500M USDT mint on Tron typically precedes a buy program
1–3 days later. Tracking mint events on both Ethereum AND Tron is
essential because >50% of Tether activity is on Tron.

**Multi-exchange liquidation clusters** — single-exchange liquidations
are noise. When ≥2 venues liquidate the same asset+side within 60s,
that's a cascade event with tradeable implications. Binance and OKX
both expose free public WebSocket liquidation streams.

**Cross-venue premium/basis** — Coinbase vs Binance gap tells you about
US institutional appetite. Binance spot vs Bybit perp basis tells you
about backwardation (perp discount = structurally bullish).

---

## Architecture

```
┌────────────────────────── data sources (all free tier) ──────────────────────────┐
│                                                                                  │
│  BTC chain     mempool.space, blockchain.info, blockstream.info                  │
│  ETH chain     Etherscan V2 + Alchemy                                            │
│  Tron chain    TronGrid public v1                                                │
│  Multi-EVM     Goldrush/Covalent (100+ chains, unified schema)                   │
│  Exchanges     Binance WS, OKX WS, Coinbase REST, Bybit REST                     │
│  Markets       DefiLlama (no key), CoinGecko (no key)                            │
│  Sanctions     OFAC sanctioned BTC list (github.com/0xB10C/...)                  │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────── 7 long-running daemons ───────────────────────────────┐
│                                                                                  │
│  chain-intel      ──▶ block + mempool + LN + UTXO age + CIH + mixing + OFAC      │
│  evm-signals      ──▶ stablecoin mints + WBTC + exchange reserves                │
│  tron-signals     ──▶ Tron stablecoin mints                                      │
│  xchg-liq         ──▶ multi-exchange liquidation aggregator                      │
│  market-premium   ──▶ Coinbase/Binance/Bybit basis                               │
│  evm-extras       ──▶ DEX large swaps + bridge flows                             │
│  ecosystem        ──▶ stablecoin caps + dominance + entity portfolios            │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────── output layer ────────────────────────────────────┐
│                                                                                  │
│   /var/lib/sygnif/chain_state.json         (full registry + clusters + events)   │
│   /var/lib/sygnif/evm_state.json           (mints + reserves)                    │
│   /var/lib/sygnif/tron_state.json          (mints)                               │
│   /var/lib/sygnif/xchg_liq_state.json      (liquidations + clusters)             │
│   /var/lib/sygnif/market_premium.json      (rolling premium snapshots)           │
│   /var/lib/sygnif/evm_extras_state.json    (DEX + bridges)                       │
│   /var/lib/sygnif/ecosystem_state.json     (stablecoins + dominance + entities)  │
│                                                                                  │
│   /var/lib/sygnif/swarm.db                 (SQLite — all topic events)           │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## What needs paid keys vs what's truly free

| Source | Auth | Free tier |
|---|---|---|
| mempool.space / blockchain.info / blockstream.info | none | unlimited (just don't hammer it) |
| DefiLlama | none | generous, no signup |
| CoinGecko `/global` `/coins/markets` | none | ~10-50 req/min |
| Binance/OKX public WS | none | unlimited |
| Coinbase Exchange ticker | none | unlimited |
| TronGrid v1 | none | works without key, key adds higher rate limit |
| Etherscan V2 | free signup → key | 5 req/sec, 100k/day |
| Alchemy | free signup → key | 300M compute units/month |
| Goldrush/Covalent | free signup → key | trial credits, then paid |

---

## Deploy

Each daemon ships with a matching `systemd` unit in `systemd/`. To install:

```bash
# 1. Place daemon source
sudo cp daemons/sygnif_*.py /opt/sygnif-services/
sudo chmod 755 /opt/sygnif-services/sygnif_*.py

# 2. Place systemd units
sudo cp systemd/sygnif-*.service /etc/systemd/system/

# 3. Provision keys (where required)
sudo mkdir -p /etc/sygnif
sudo tee /etc/sygnif/evm-keys.env > /dev/null <<EOF
SYGNIF_ETHERSCAN_KEY=your_key_here
SYGNIF_ALCHEMY_KEY=your_key_here
EOF
sudo tee /etc/sygnif/tron-keys.env > /dev/null <<EOF
SYGNIF_TRON_KEY=your_key_here
EOF
sudo tee /etc/sygnif/goldrush-keys.env > /dev/null <<EOF
SYGNIF_GOLDRUSH_KEY=your_key_here
EOF
sudo chmod 600 /etc/sygnif/*-keys.env

# 4. Seed: pull OFAC sanctioned BTC list + bootstrap known-entity registry
sudo /opt/sygnif/.venv/bin/python /opt/sygnif-services/sygnif_chain_seed.py

# 5. Start all daemons
sudo systemctl daemon-reload
for s in sygnif-chain-intel sygnif-evm-signals sygnif-tron-signals \
         sygnif-xchg-liquidations sygnif-market-premium \
         sygnif-evm-extras sygnif-ecosystem; do
  sudo systemctl enable --now $s.service
done
```

Python deps: stdlib only, except `websocket-client` for the liquidation
aggregator (`pip install websocket-client`).

---

## Query the data

```bash
# BTC chain — top wallets, recent whale events, dormancy breaks, peeling chains
sudo /opt/sygnif/.venv/bin/python /opt/sygnif-services/sygnif_chain_report.py
sudo /opt/sygnif/.venv/bin/python /opt/sygnif-services/sygnif_chain_report.py --top 30
sudo /opt/sygnif/.venv/bin/python /opt/sygnif-services/sygnif_chain_report.py --events
sudo /opt/sygnif/.venv/bin/python /opt/sygnif-services/sygnif_chain_report.py --peeling

# Pull recent swarm events
sqlite3 /var/lib/sygnif/swarm.db "
  SELECT datetime(created,'unixepoch'), topic, substr(content,1,120)
  FROM swarm_entries
  WHERE topic LIKE 'chain.%' OR topic LIKE 'evm.%' OR topic LIKE 'tron.%'
        OR topic LIKE 'xchg.%' OR topic LIKE 'market.%'
        OR topic LIKE 'ecosystem.%'
  ORDER BY created DESC LIMIT 30;"
```

---

## Confidence scoring (Arkham-style)

Every event carries a `confidence` field 0–100:

| Range | Meaning | Source |
|---|---|---|
| 100 | Hard ground truth | OFAC sanctioned, official treasury, mint contract event |
| 95 | Verified | Direct match against publicly-disclosed entity address |
| 80–90 | Strong heuristic | Behavioural pattern + supporting evidence |
| 50–79 | Predicted | Clustering inference, label propagation |
| 20–49 | Weak signal | Single-tx heuristic, low corroboration |
| 0–19 | Unknown | Insufficient data |

The swarm filter `confidence ≥ 60` removes noise; downstream consumers
can use stricter thresholds.

---

## Limitations & honest gaps

- **Address tagging is heuristic.** ~11 hardcoded exchange anchors plus
  cluster propagation. Arkham-grade entity attribution is paid ($900/mo+).
- **Predictive liquidation heatmap** (Glassnode-style) is not built.
  We only realize liquidations. Predictive needs per-position leverage data.
- **ETF daily flow per fund** is missing. CoinShares/Farside paid sources only.
- **The Graph subgraphs / Pyth Network** not integrated. Would add more
  DEX granularity + oracle confidence intervals.
- **No on-chain MEV / sandwich detection.**
- **Lightning Network visibility limited to ecosystem stats**, not channel-level.

---

## License

MIT
