# Profitable Bot Technical Blueprint

This document details how to implement the **SYGNIF Signal Aggregator** and integrate it with a Freqtrade bot.

## 1. Schema for `swarm.db` Signal Table
Create a view or table to store aggregated scores:
```sql
CREATE TABLE IF NOT EXISTS signal_scores (
    ts INTEGER,
    pair TEXT,
    source TEXT,
    score REAL, -- (-100.0 to 100.0)
    weight REAL,
    meta TEXT
);
```

## 2. Signal Mapping Table
| Topic | Logic | Base Score |
|---|---|---|
| `market.premium` | `cb_bn_bps > 5` | +20 |
| `tron.stablecoin_mint` | `$USD > 100M` | +15 |
| `xchg.liquidation_cluster` | `SHORT_LIQ, n_exchanges>=2` | +30 (short squeeze, bullish) |
| `xchg.liquidation_cluster` | `LONG_LIQ, n_exchanges>=2` | -30 (long capitulation, bearish) |
| `evm.wbtc_flow` | `MINT > 50 BTC` | +10 |
| `chain.dormancy_break` | `age > 5y AND value > 500` | -40 (whale exit) |

## 3. Asynchronous Sentiment Refactoring
To fix the performance risk in `SygnifStrategy.py`:

### Step A: The Background Scraper
A Python script (`daemons/sygnif_sentiment_daemon.py`) runs independently:
1. Polls news for whitelist pairs.
2. Calls Claude API (or Local LLM) for a score.
3. Writes to `signal_scores` table.

### Step B: The Strategy Update
```python
def populate_entry_trend(self, df, metadata):
    # Instead of calling Claude:
    score = self.read_sentiment_from_db(metadata['pair'])
    if score > 65 and ta_signal:
        enter_long()
```

## 4. Signal Compounding Logic
Use a non-linear compounding formula for the Aggregator:
`FinalScore = Sum(IndividualScores) * (1 + 0.1 * NumberOfUniqueSources)`
This rewards setups that have confluence across Bitcoin chain, EVM, and Exchange data.

## 5. Deployment Recommendation
- Run the Intel Stack and Aggregator on a **low-latency VPS** (e.g., AWS eu-central-1 for Binance/OKX proximity).
- Use **SQLite WAL mode** to allow concurrent reads from the Trading Bot and writes from the Daemons.

## 6. Implementation Reference

The stack now includes the functional "Brain" and "Execution" reference scripts:

1.  **Aggregator** (`daemons/sygnif_aggregator.py`):
    - Reads all `swarm.db` events from the last hour.
    - Applies time-decay (newer = more weight).
    - Applies confluence weighting (Chain + Market + EVM).
    - Writes `BTC_GLOBAL` sentiment to `weighted_signals` table.

2.  **Signal Bot** (`daemons/sygnif_signal_bot.py`):
    - Reads the aggregated sentiment from `swarm.db`.
    - Executes simulated trades when `score > 65` (Long) or `score < -65` (Short).
    - Serves as the "Execution Layer" blueprint for a full CCXT integration.
