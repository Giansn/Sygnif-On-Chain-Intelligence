# Proposal: Intel Dashboard v3 — Glassnode-style On-Chain Regime Indicators

This proposal outlines the next evolution of the SYGNIF Intelligence dashboard (v3). It builds upon the v2 architecture by introducing normalized, Glassnode-style regime indicators. While v2 excels at surfacing raw flows and event logs, v3 will compute statistical normalizations (e.g., Z-scores, rolling averages) to help traders distinguish between noise and structural shifts in market regimes.

By shifting from absolute values to relative oscillators, traders can more effectively identify overbought/oversold conditions and capitulation zones.

## Proposed Visualizations

### 1. Exchange Netflow Regime Oscillator (Z-Score)
*   **Chart Type:** ECharts Line chart with `markArea` (shaded zones) for ±2 standard deviations.
*   **Data Source / Query:** Computed server-side from `swarm_entries` topic `evm.exchange_reserve`.
    ```sql
    -- Conceptual query (server-side logic will calculate the rolling mean/stdev):
    SELECT cast(created/3600 as int)*3600 as hour_bucket, json_extract(content, '$.delta') as netflow
    FROM swarm_entries
    WHERE topic = 'evm.exchange_reserve' AND created > unixepoch() - (86400 * 30)
    ```
*   **Alpha Hypothesis:** Raw exchange netflows are volatile. By plotting the 24-hour netflow as a Z-score relative to the 30-day rolling mean, a trader can instantly spot 2-sigma outlier events (e.g., a massive sudden deposit). A spike above +2 suggests imminent sell pressure, while drops below -2 suggest aggressive accumulation withdrawals.

### 2. Whale Flow Confluence Heatmap
*   **Chart Type:** ECharts Heatmap (X-axis: Time/Hours, Y-axis: Signal Topic, Color Intensity: Normalized Volume).
*   **Data Source / Query:** Computed from multiple `chain.whale`, `evm.stablecoin_mint`, and `xchg.liquidation_cluster` topics.
    ```sql
    SELECT topic, cast(created/3600 as int)*3600 as hour_bucket, count(*) as event_count, sum(json_extract(content, '$.amount')) as vol
    FROM swarm_entries
    WHERE created > unixepoch() - 86400
    GROUP BY topic, hour_bucket
    ```
*   **Alpha Hypothesis:** Single signals can be false positives. This heatmap normalizes the volume of different whale actions into a 0-100 scale per hour. When stablecoin mints, whale chain movements, and liquidation clusters all "light up" simultaneously in a single hour bucket, it signals a high-confidence regime shift (confluence), prompting the trader to enter a position.

### 3. Stablecoin Supply Velocity Indicator
*   **Chart Type:** ECharts Combo Chart (Bar for 1h mints, Line for 7-day cumulative trend).
*   **Data Source / Query:** `evm.stablecoin_mint` and `tron.stablecoin_mint`.
    ```sql
    SELECT cast(created/3600 as int)*3600 as hour_bucket, sum(json_extract(content, '$.amount')) as minted
    FROM swarm_entries
    WHERE topic LIKE '%stablecoin_mint%' AND created > unixepoch() - (86400 * 7)
    GROUP BY hour_bucket
    ORDER BY hour_bucket ASC
    ```
*   **Alpha Hypothesis:** It is not enough to know that $50M USDT was minted; traders need to know the *velocity*. This chart visualizes the hourly mint rate overlaid on a 7-day cumulative supply curve. A steepening curve (high velocity) is a classic leading indicator for a bullish market regime, as it represents fresh fiat entering the crypto ecosystem preparing to deploy.

### 4. Market Premium Divergence
*   **Chart Type:** ECharts Line Chart (Dual Y-Axis).
*   **Data Source / Query:** `market.premium` and proxied Bybit price data (`/v5/market/kline`).
    ```sql
    SELECT created, json_extract(content, '$.cb_bn_bps') as cb_premium
    FROM swarm_entries
    WHERE topic = 'market.premium' AND created > unixepoch() - 86400
    ```
*   **Alpha Hypothesis:** Divergence is alpha. If the BTC price is making a lower low, but the Coinbase-Binance premium (US institutional demand) is making a higher high, it indicates hidden institutional absorption. Visualizing these two normalized lines directly on top of each other allows traders to spot these structural divergences instantly.
