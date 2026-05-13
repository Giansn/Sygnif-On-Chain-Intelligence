# Proposal: Signal Aggregator v1 — Concrete Spec for the Meta-Strategy Brain

This document concretes the Signal Aggregator component of the SYGNIF Hierarchical Signal Model. It defines the exact transformations from raw `swarm_entries` to a unified sentiment score and specifies the mathematical framework for time-decay and confluence.

## 1. Signal-to-Score Mapping

For each of the top 5 most active topics in the SYGNIF system, we define a transformation $T(\text{meta}) \to S \in [-100, +100]$.

### 1.1 `chain.whale` (Freq: 1150/24h)
**Logic**: Score based on flow direction and BTC volume, adjusted by confidence.
- **Bullish**: `category` in `["WITHDRAWAL_FROM_EXCHANGE", "ACCUMULATION_TO_COLD"]`
- **Bearish**: `category` == `"DEPOSIT_TO_EXCHANGE"`
- **Base Score ($S_{base}$)**: $\min(100, \text{value\_btc} \times 1.5)$
- **Final Score**: $S = S_{base} \times (\text{confidence} / 100) \times \text{direction}$
- *Grounding*: `daemons/sygnif_chain_intel.py` line 849 uses `confidence` 70-85 for these categories.

### 1.2 `market.premium` (Freq: 864/24h)
**Logic**: Captures US institutional bid vs Asian/Perp imbalance.
- **Primary Driver**: `cb_bn_bps` (Coinbase USD vs Binance USDT).
- **Secondary Driver**: `bn_bb_bps` (Binance Spot vs Bybit Perp).
- **Formula**: $S = \text{clip}((\text{cb\_bn\_bps} \times 10) + (\text{bn\_bb\_bps} \times 5), -100, 100) \times (\text{confidence} / 100)$
- *Grounding*: `daemons/sygnif_market_premium.py` line 125 defines `US_BUY` signal when bps > 5.

### 1.3 `decision.snapshot` (Freq: 668/24h)
**Logic**: Tracks the planner's internal state. This is a **Regime Signal**.
- **Transformation**:
  - `TREND_UP` / `NORMAL` $\to +20$
  - `TREND_DOWN` $\to -30$
  - `HIGH_VOL_SHOCK` $\to 0$ (Volatility blocks directionality)
- *Note*: This is a low-amplitude "baseline" score. It provides context for other events.
- *Grounding*: `CLAUDE.md` §3.3 regime classification.

### 1.4 `chain.mempool_whale` (Freq: 332/24h)
**Logic**: Early-warning leading indicator.
- **Formula**: $S = \text{clip}(\text{value\_btc} \times 2, 0, 100) \times 0.8 \times \text{direction}$
- **Discount**: 0.8 multiplier reflects the unconfirmed nature of mempool txs.
- *Grounding*: `daemons/sygnif_chain_intel.py` line 675 emits this topic.

### 1.5 `agent.commentary` (Freq: 321/24h)
**Logic**: Sentiment derived from brain-narrated market state.
- **Transformation**: Requires NLP keyword matching (e.g., "bullish", "risk", "outflow").
- **Starting Guess**: $\pm 25$ based on the detected sentiment of the narrated state.
- **Refinement**: If the brain identifies a `risk_event`, score is $-50$.

---

## 2. Time-Decay Function

The relevance of a signal decreases over time. We use an exponential decay function:
$$S(t) = S_0 \times e^{-\lambda \Delta t}$$
Where $\lambda = \frac{\ln(2)}{T_{1/2}}$ (Half-life coefficient).

| Signal Class | Half-Life ($T_{1/2}$) | Justification |
|---|---|---|
| `xchg.*`, `market.*` | 300s (5m) | Real-time exchange data is noise after one 5m candle. |
| `chain.mempool_whale` | 600s (10m) | Mempool events either confirm or become stale within ~1 block. |
| `chain.whale` | 3600s (1h) | Large chain moves reflect positional intent that lasts hours. |
| `ecosystem.*` | 14400s (4h) | Macro rotations (stablecoin mints) have longer-term impact. |

---

## 3. Cross-Corroboration Formula

To reward confluence, we apply a **Confluence Multiplier** ($M_c$) to the summed scores.
$$S_{weighted} = \left( \sum_{i=1}^n S_i(t) \right) \times M_c$$
$$M_c = 1 + (\alpha \times (U - 1))$$
Where $U$ is the number of **Unique Signal Classes** (Chain, EVM, Market, Ecosystem, Agent) and $\alpha = 0.2$ (Starting guess).

### Worked Examples
1.  **Single Signal**: `market.premium` at +60. $U=1, M_c=1.0 \to S=+60$.
2.  **Corroborated**: `market.premium` (+60) AND `chain.whale` (+40).
    - $U=2, M_c = 1 + (0.2 \times 1) = 1.2$
    - $S = (60 + 40) \times 1.2 = +120 \to \text{clip at } +100$.
3.  **Opposing**: `market.premium` (+60) AND `chain.whale` (-40).
    - $U=2, M_c = 1.2$
    - $S = (60 - 40) \times 1.2 = +24$.

---

## 4. `weighted_sentiment` Row Schema

Jules will write a single row to `swarm_entries` every 60s under the topic `system.brain.weighted_sentiment`.

| Field | Type | Description |
|---|---|---|
| `created` | INTEGER | Unix epoch. |
| `topic` | TEXT | `system.brain.weighted_sentiment`. |
| `content` | TEXT | Human-readable summary: "BTC Bullish (72) | Confluence: 3 sources (Market, Chain, EVM)". |
| `meta.score` | REAL | The final calculated score in $[-100, +100]$. |
| `meta.confidence` | INTEGER | $0-100$. High when $U \ge 3$ and $n > 5$. |
| `meta.breakdown` | JSON | `{ "market.premium": 42, "chain.whale": 30, ... }` (Audit trail). |

---

## 5. Acceptance Test

**Criterion**: Weighted sentiment is a valid leading indicator of BTC price action.
- **N (Success Rate)**: 65%.
- **M (Baseline)**: 51% (Random walk).
- **Target**: `weighted_sentiment > +50` must be followed by a positive price delta in `BTCUSDT` within 30 minutes.
- **Sample Size**: Minimum 100 observations over 14 days to achieve statistical significance ($p < 0.05$).

---

## 6. The Biggest Open Question

**The Missing Feedback Loop.**

META_STRATEGY.md treats signal weights as static (or manually tuned). In a real production environment, the system must learn which signals are lying.
- **The Gap**: If `chain.whale` distribution events are followed by price *increases* (bullish absorption) 5 times in a row, the Aggregator will continue outputting bearish scores while the market remains bullish.
- **Proposed Solution**: A `signal_evaluator` daemon must join `weighted_sentiment` with `rt.close` events and dynamically adjust the base weights ($\alpha$) in the mapping table via a reinforcement loop. Without this, the aggregator is a "fancy snapshot" that will eventually drift into obsolescence.
