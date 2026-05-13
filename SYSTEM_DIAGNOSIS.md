# SYGNIF System Diagnosis: `Giansn/SYGNIF`

This diagnosis covers the Freqtrade-based trading bot and its integration with the on-chain intelligence stack.

## 1. Intelligence Stack Alignment
The intelligence daemons in the standalone repo (`Sygnif-On-Chain-Intelligence`) were found to be ahead of the snapshots located in `ec2-snapshot/services/` of the main bot repo.
- **Fixed**: Hardcoded prices ($81,850 BTC) and the missing Bitget integration.
- **Fixed**: Syntax error in `sygnif_tron_signals.py`.
- **Refactored**: Centralized price fetching into `sygnif_common.py`.

## 2. Strategy Performance Analysis (`SygnifStrategy.py`)
- **Blocking Calls**: The strategy currently performs synchronous HTTP requests to RSS feeds and the Claude API within `populate_entry_trend`.
- **Risk**: Freqtrade's loop must complete within the candle interval (5m). If multiple pairs trigger a sentiment check simultaneously, or if the API is slow, the bot will lag, potentially missing entries or causing "out of sync" errors.
- **Critical Fix Path**:
  1.  **Sentiment Daemon**: Create a new background daemon that polls news and requests Claude sentiment for the top 50 volume pairs every 15 minutes.
  2.  **State Storage**: This daemon writes scores to a new `sentiment` table in `swarm.db`.
  3.  **Strategy Refactor**: Modify `SygnifStrategy.py` to query `swarm.db` for the latest score for its pair, rather than calling `self.claude.analyze_sentiment`.
  4.  **Local Cache**: Use a simple local dictionary in the strategy to cache these scores for 5-10 minutes to minimize SQLite overhead.

## 3. Bitget Integration Correctness
- **Finding**: Initial implementation of Bitget support had inverted liquidation side-mapping.
- **Resolution**: Fixed in `daemons/sygnif_xchg_liquidations.py` to correctly map `buy` events to `SHORT_LIQ` (standard for liquidation streams).

## 4. Trade Overseer Integration
- The `trade-overseer` is designed to be the "brain," but the current wiring to the intel stack is heuristic-based (reading raw `swarm_entries`).
- **Improvement**: Standardize the "Signal" format in `swarm.db`. A new `utils/sygnif_health.py` utility has been added to the intel stack to provide structured health data for the Overseer.

## 5. Environment & Deployment
- The systemd unit files use `/opt/sygnif-services`. Ensure the deployment script (or Dockerfile) correctly maps these paths.
- The use of a virtual environment at `/opt/sygnif/.venv` is assumed by all unit files.

## 6. Next Steps for Full System
1. Update `ec2-snapshot/` in the `Giansn/SYGNIF` repo with the latest daemons from this stack.
2. Implement an asynchronous sentiment cache to prevent `SygnifStrategy.py` from making live web calls.
3. Use the new `sygnif_health.py` to add a "System Status" section to the bot's dashboards.
