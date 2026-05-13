# ChartInspect Integration Proposal

## API Discovery & Constraints

We successfully identified the ChartInspect API base URL structure as `https://chartinspect.com/api/v1/`. The API requires authentication using an `x-api-key` header instead of a standard Bearer token. 

However, upon probing the API with the provided key (`ci_live_ea38afc04d3dbdaa1844a5fb640bb956d71767d4086c7b6e59bcf93c08177959`), we hit a paywall. The response to an on-chain data request explicitly states:
`{"success":false,"error":"Pro subscription required. On-chain, market indicators, exchange/ETF, and derivatives data are available exclusively to Pro users. Free tier includes crypto prices and economic data.","category":"onchain","currentTier":"free","upgradeUrl":"https://chartinspect.com/pricing"}`

Because on-chain indicators require a **Pro subscription**, we are pivoting to integrate 8 of the most useful **free-tier economic and financial indicators** that are highly relevant to crypto traders.

## 8 Free-Tier Indicators for Trader Dashboard

| Indicator Name | API Endpoint | What It Measures | Relevant Value Range | Update Frequency |
|---|---|---|---|---|
| **Federal Funds Effective Rate** | `/api/v1/economic/interest_fed` | US Federal Reserve base interest rate | High/Rising = Bearish (tight liquidity), Low/Falling = Bullish | Daily |
| **M2 Money Supply (US)** | `/api/v1/economic/money_m2` | Total money supply in the US economy | Accelerating > 5% YoY = Bullish, Decelerating = Bearish | Monthly |
| **Global M2 Money Supply** | `/api/v1/economic/money_m2_global` | Global aggregate money supply proxy | Rising = Bullish liquidity expansion | Monthly |
| **Consumer Price Index** | `/api/v1/economic/inflation_cpi` | Broad US consumer inflation rate | High > 3% = Macro risk, Approaching 2% = Easing risk | Monthly |
| **10Y-2Y Treasury Yield Spread** | `/api/v1/economic/rates_yield_spread_10y2y` | Yield curve inversion metric | < 0 (Inverted) = Recession warning, Steepening = Market shift | Daily |
| **S&P 500 Index** | `/api/v1/economic/sp500` | Broad US equity market performance | Up-trend = Favorable risk environment for crypto | Daily |
| **US Dollar Index (DXY)** | `/api/v1/economic/dxy` | Strength of USD vs basket of fiat currencies | > 105 = Bearish for crypto, < 100 = Bullish for crypto | Daily |
| **NASDAQ Composite** | `/api/v1/economic/nasdaq` | US tech equity market performance | Correlates closely with high-beta crypto assets | Daily |

## Rate Limits & Free Tier Limitations

- **Free Tier:** 100 requests/hour.
- **Pro Tier:** 1,000 requests/hour.
- Polling these 8 endpoints at a 5-minute cadence will result in 96 requests/hour (8 requests * 12 times per hour). This keeps us strictly within the 100 requests/hour free-tier limit, though it leaves little room for error.

## Integration Design

A new daemon `daemons/sygnif_chartinspect.py` will poll the 8 defined economic indicators from `https://chartinspect.com/api/v1/economic/` on a 5-min cadence. The daemon will use the `x-api-key` header for authentication. Upon fetching the data, it will extract the latest value and period-over-period change, then write to the `swarm.db` topic `chart.indicator` with a meta payload structure: `{indicator, value, percentile, ts}`. This design closely follows the `emit_swarm` pattern established in `daemons/sygnif_chain_intel.py` and `daemons/sygnif_market_premium.py`.

## Dashboard Panel Proposal

Based on the live v3 dashboard structure, we propose integrating these indicators across the following sections:

**Market Section (Dedicated Panels):**
- **Global Liquidity Panel (Line Chart):** Combining `money_m2` and `money_m2_global` to show the macroeconomic liquidity backdrop, which is a primary driver of crypto cycles.
- **Risk Environment Panel (Line Chart with Overlays):** Displaying `sp500`, `nasdaq`, and inversely plotting `dxy` to provide a clear view of traditional finance risk appetite and dollar strength.

**Trader Section (Heatmap/Event Stream):**
- **Macro Alerts:** `interest_fed`, `inflation_cpi`, and `rates_yield_spread_10y2y` are slower-moving and better suited as contextual alerts or heatmap items. When significant changes occur (e.g., FOMC rate changes, CPI prints), these can feed into the event stream to inform traders of shifting macro winds without cluttering the screen with slow-moving charts.
