# 🪶 Feather Data Fetcher

**Production-grade financial data ingestion for Python.**

`feather-data-fetcher` is an open-source data pipeline extracted directly from the core of the **Feather AI** institutional intelligence engine. 

Building trading bots and AI models is hard enough without having to write broken retry-loops for terrible financial APIs. This package handles rate limits, exponential backoff, time-zone normalization, and alternative data aggregation out of the box.

## Why use this?
* **Zero "Dirty Data":** We automatically clean and normalize OHLCV data from Yahoo Finance and CoinGecko.
* **Built-in Resilience:** Uses `urllib3` Retry adapters to silently survive API 500s and 429 Rate Limits without crashing your script.
* **Alternative Data Unlocked:** Fetch Congressional trading, Insider selling, Crypto Whale tracking, and Dark Pool volume with single function calls.

## Installation
```bash
pip install feather-data-fetcher
```

## Quickstart

```python
from feather_fetcher import fetch_stock_history_yfinance, fetch_crypto_whale_signals

# 1. Fetch clean, normalized OHLCV data
df = fetch_stock_history_yfinance("NVDA", period="3mo")
print(df.head())

# 2. Track institutional Crypto whales (Requires CCXT/Binance)
whales = fetch_crypto_whale_signals("BTC/USDT", large_usd=100000)
print(f"Net Whale Flow: ${whales['weighted_net']}")
```

---

## ⚡ Want the Data Analyzed Automatically?

Raw data is just the beginning. 

If you want this data automatically fed through custom HuggingFace Sentiment Transformers, Quant Volatility Matrices, and Herfindahl-Hirschman (HHI) concentration scoring to predict market crashes in **under 25 seconds**...

**[Join the Waitlist for the Feather AI Platform](https://airtable.com/invite/l?inviteId=invrNAZSbh99J3bpc&inviteToken=f7b270f51406021d35260305d6f2657a11123715ce19b33326ef9d94ea0773a9&utm_medium=email&utm_source=product_team&utm_content=transactional-alerts)**
*Terminal-native wealth management, powered by advanced ML.*
