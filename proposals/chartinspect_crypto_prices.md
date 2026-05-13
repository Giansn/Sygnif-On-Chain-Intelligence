# ChartInspect Free-Tier Crypto Prices Proposal

## API Discovery & Constraints

Following the initial API discovery in PR #8, we know that the ChartInspect free tier allows access to economic data and crypto prices, but blocks access to on-chain and market indicators. We probed the ChartInspect API using the provided key (`ci_live_ea38afc04d3dbdaa1844a5fb640bb956d71767d4086c7b6e59bcf93c08177959`) to determine the exact surface area of the free-tier crypto-prices endpoints.

### Endpoint Enumeration Results

- `/api/v1/crypto/prices/list`: **200 OK.** Returns a list of available symbols (e.g., "BTC", "ETH", "SOL", "DOGE", "ADA", "XRP", "LTC", "ETHBTC").
- `/api/v1/crypto/prices/{symbol}`: **200 OK.** Works for all major coins tested (BTC, ETH, SOL, DOGE, XRP, LTC, ADA). Returns historical daily Open, High, Low, Close, and Volume data.
- `/api/v1/market-indicators/dominance`: **403 Paywall.** Pro subscription required.
- `/api/v1/market-indicators/fear-greed`: **403 Paywall.** Pro subscription required.
- `/api/v1/crypto/marketcap`: **404 Not Found.** (Returns HTML).
- `/api/v1/crypto/volume`: **404 Not Found.** (Returns HTML).
- `/api/v1/crypto/historical?symbol=BTC&days=30`: **404 Not Found.** (Returns HTML).
- `/api/v1/crypto/prices`: **404 Not Found.** (Returns HTML).
- `/api/v1/prices`: **404 Not Found.** (Returns HTML).

### Working Endpoints Documentation

**Endpoint:** `/api/v1/crypto/prices/{symbol}`
- **What it returns:** Historical price OHLCV (Open, High, Low, Close, Volume) data for the requested symbol.
- **Update frequency:** Daily.
- **Sample Payload:**
  ```json
  {"success":true,"symbol":"BTC","name":"Bitcoin","count":5780,"data":[{"timestamp":1279324800,"open":0.04951,"high":0.04951,"low":0.04951,"close":0.04951,"volume":20},{"timestamp":1279411200,"open":0.04951,"high":0.08585,"low":0.04951,"close":0.08584,"volume":75.01}...]}
  ```

**Endpoint:** `/api/v1/crypto/prices/list`
- **What it returns:** A flat array of strings representing all symbols supported by the prices API.
- **Sample Payload:**
  ```json
  {"success":true,"availableSymbols":["AAVE","ADA","APT","ARB","ASTER","ATOM","AVAX","BCH","BGB","BNB","BTC","CC","COAI"...]}
  ```

## 4 Free-Tier Crypto-Data Points

Since most advanced metrics (dominance, fear & greed) are paywalled, and basic market cap isn't exposed directly via these free endpoints, we must extract value from the raw OHLCV series provided by the free tier. We will target the following 4 useful data points:

1. **BTC Daily Volume Profile:** Extracted from `/api/v1/crypto/prices/BTC`. While we get real-time exchange data elsewhere, ChartInspect's aggregated historical daily volume helps provide a baseline for "abnormal volume" alerts.
2. **ETH/BTC Ratio Trend:** Extracted from `/api/v1/crypto/prices/ETHBTC`. A critical indicator for altcoin season probability and overall market risk appetite.
3. **SOL Momentum (Close vs. Open):** Extracted from `/api/v1/crypto/prices/SOL`. Used as a proxy for high-beta L1 strength relative to BTC.
4. **DOGE Speculation Index:** Extracted from `/api/v1/crypto/prices/DOGE`. DOGE volume and price spikes often signal late-stage market exuberance (local tops).

## Integration Design

We will extend the existing `daemons/sygnif_chartinspect.py` (which currently polls economic data) to also poll the `/api/v1/crypto/prices/{symbol}` endpoint for BTC, ETHBTC, SOL, and DOGE on a daily or 12-hour cadence (since this specific data is daily resolution). The daemon will calculate the rolling metrics (e.g. 24h change, volume spikes) and write them to `swarm.db` under a new topic `chart.crypto_metric` with the payload `{symbol, close, volume, changePercent, ts}` to disambiguate from the macro economic indicators.

## Honest Gap Report

The free tier is severely limited when it comes to high-value crypto metrics. We successfully retrieved raw daily OHLCV data, but **crucial trader-facing indicators like BTC Dominance (`/api/v1/market-indicators/dominance`) and the Fear & Greed Index (`/api/v1/market-indicators/fear-greed`) are strictly locked behind the Pro subscription.**

**Recommendation:** The operator should contact ChartInspect support and ask if "Market Indicators" (specifically BTC Dominance and Fear & Greed) can be included in the free tier, as raw OHLCV prices are already freely available from standard exchange APIs (like Binance/Bybit). If not, upgrading to Pro is highly recommended to unlock these key features.
