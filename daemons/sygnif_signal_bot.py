#!/usr/bin/env python3
"""sygnif_signal_bot.py — Reference Intelligence-Based Execution Bot.

This bot reads the 'weighted_signals' table (populated by sygnif_aggregator.py)
and makes execution decisions based on institutional and retail flows.

Modes:
- Dry Run (Default): Logs signals and simulated trades.
- Live: (Placeholder) Would connect to CCXT for actual execution.
"""

import sqlite3
import time
import json
import os
import datetime as dt
from collections import deque

DB_PATH = "/var/lib/sygnif/swarm.db"
POLL_S  = 15

# Thresholds
ENTRY_SCORE_LONG  = 65.0
ENTRY_SCORE_SHORT = -65.0
EXIT_SCORE_NEUTRAL = 15.0  # Exit long if score falls below 15

class SygnifBot:
    def __init__(self):
        self.active_position = None  # {'side': 'long'|'short', 'entry_price': float, 'entry_ts': int}
        self.balance_usd = 10000.0
        self._history = deque(maxlen=20)

    def get_latest_signal(self):
        if not os.path.exists(DB_PATH):
            return None
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT score, confluence, meta, ts FROM weighted_signals WHERE id = 'BTC_GLOBAL'")
            row = cur.fetchone()
            conn.close()
            return row
        except Exception as e:
            print(f"Error reading signal: {e}")
            return None

    def log_trade(self, action, side, score, price=None):
        ts = dt.datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] TRADER: {action.upper()} {side.upper()} (score: {score:+.1f})")

    def run_cycle(self):
        sig = self.get_latest_signal()
        if not sig:
            return

        score = sig["score"]
        conf = sig["confluence"]

        # --- Entry Logic ---
        if self.active_position is None:
            if score >= ENTRY_SCORE_LONG and conf >= 2:
                self.active_position = {'side': 'long', 'entry_ts': int(time.time()), 'score': score}
                self.log_trade("entry", "long", score)

            elif score <= ENTRY_SCORE_SHORT and conf >= 2:
                self.active_position = {'side': 'short', 'entry_ts': int(time.time()), 'score': score}
                self.log_trade("entry", "short", score)

        # --- Exit Logic ---
        else:
            side = self.active_position['side']
            should_exit = False

            if side == 'long':
                if score < EXIT_SCORE_NEUTRAL:
                    should_exit = True
                elif score < -40: # Rapid reversal
                    should_exit = True

            elif side == 'short':
                if score > -EXIT_SCORE_NEUTRAL:
                    should_exit = True
                elif score > 40:
                    should_exit = True

            if should_exit:
                self.log_trade("exit", side, score)
                self.active_position = None

def main():
    print("=== SYGNIF Reference Signal Bot (DRY RUN) ===")
    print(f"Long Threshold:  >{ENTRY_SCORE_LONG}")
    print(f"Short Threshold: <{ENTRY_SCORE_SHORT}")

    bot = SygnifBot()

    while True:
        try:
            bot.run_cycle()
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Bot Loop Error: {e}")
        time.sleep(POLL_S)

if __name__ == "__main__":
    main()
