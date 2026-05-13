# SYGNIF Profitable Signal Patterns

Based on the multi-track data ingested by the SYGNIF stack, here are high-probability setups that a profitable bot would exploit.

## 1. "The Institutional Accumulation" (Long)
**Condition**: US Institutional buyers are bidding BTC, often preceding a major leg up.
- **Signal A**: `market.premium` — Coinbase USD vs Binance USDT premium > 5bps for 3+ hours.
- **Signal B**: `tron.stablecoin_mint` — Large USDT mint ($500M+) on Tron, followed by transfer to Binance/OKX.
- **Signal C**: `chain.whale` — Whale outflows from Exchange clusters to known Cold clusters.
- **Confidence**: 95/100.
- **Action**: Increase position size or enter Long.

## 2. "The Liquidation Flush" (Mean Reversion)
**Condition**: Over-leveraged participants are being wiped out, creating a local price extreme.
- **Signal A**: `xchg.liquidation_cluster` — ≥3 exchanges liquidating the same side within 60s.
- **Signal B**: `market.premium` — Spot vs Perp basis flips negative (backwardation).
- **Signal C**: `chain.mempool_whale` — Large buy orders appearing in the mempool during the flush.
- **Confidence**: 85/100.
- **Action**: Scalp Long (if Longs liquidated) or Short (if Shorts liquidated) for a quick bounce.

## 3. "The Smart Money Rotation" (DeFi-side)
**Condition**: Capital is moving from stablecoins into tokenized BTC (WBTC) on Ethereum.
- **Signal A**: `evm.dex_swap` — Multi-million dollar USDC -> WBTC swaps in Uniswap V3 pools.
- **Signal B**: `evm.bridge_flow` — Large stablecoin inflows ($10M+) through Stargate/Across routers.
- **Signal C**: `evm.wbtc_flow` — Fresh WBTC mints (native BTC -> tokenized).
- **Confidence**: 80/100.
- **Action**: Enter Long on ETH-side or Native BTC.

## 4. "The Whale Exit" (Short/Warning)
**Condition**: Long-term holders (LTH) are moving coins to exchanges after years of dormancy.
- **Signal A**: `chain.dormancy_break` — 5yr+ old UTXOs (500+ BTC) spent to an Exchange hot wallet.
- **Signal B**: `ecosystem.btc_dominance` — BTC dominance rolling over from a local peak while altcoin portfolios grow.
- **Signal C**: `market.premium` — Negative Coinbase premium (US selling pressure).
- **Confidence**: 90/100.
- **Action**: Hedge positions or enter Short.
