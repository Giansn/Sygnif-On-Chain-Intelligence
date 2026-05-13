#!/usr/bin/env python3
"""sygnif_common.py — Shared utilities for SYGNIF intelligence daemons."""

import json
import logging
import urllib.request
from typing import Optional, Dict

logger = logging.getLogger("sygnif.common")

def fetch_ticker_price(symbol: str = "BTCUSDT") -> Optional[float]:
    """Fetch latest price for a symbol from Binance public ticker API."""
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "sygnif-intel/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data and "price" in data:
                return float(data["price"])
    except Exception as e:
        # We don't use logger here as most daemons use print
        return None
    return None

def fetch_prices_multi(symbols: list) -> Dict[str, float]:
    """Fetch multiple prices in parallel or sequence."""
    out = {}
    for s in symbols:
        p = fetch_ticker_price(s)
        if p:
            out[s] = p
    return out
