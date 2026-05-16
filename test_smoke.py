"""Quick smoke test for feather-data-fetcher package."""
import sys
sys.path.insert(0, ".")

from feather_fetcher import (
    fetch_stock_history_yfinance,
    fetch_high_quality_crypto_data,
    fetch_crypto_whale_signals,
    fetch_news_articles,
    fetch_fear_greed_index,
)

print("=" * 60)
print("  FEATHER DATA FETCHER — SMOKE TEST")
print("=" * 60)

# 1. Stock Data
print("\n[1/5] Fetching NVDA stock data...")
df = fetch_stock_history_yfinance("NVDA", period="1mo")
if df is not None and not df.empty:
    print(f"  [SUCCESS] {len(df)} rows fetched")
    print(f"  Latest close: ${df['close'].iloc[-1]:.2f}")
else:
    print("  [FAILED] No data returned")

# 2. Crypto Data
print("\n[2/5] Fetching BTC crypto data...")
crypto = fetch_high_quality_crypto_data("bitcoin", days=30)
if crypto is not None and not crypto.empty:
    print(f"  [SUCCESS] {len(crypto)} rows fetched")
    print(f"  Latest close: ${crypto['close'].iloc[-1]:,.2f}")
else:
    print("  [FAILED] No data returned")

# 3. Whale Signals
print("\n[3/5] Fetching BTC whale signals...")
whales = fetch_crypto_whale_signals("BTC/USDT", large_usd=50000)
if whales and not whales.get("error"):
    print(f"  [SUCCESS] {whales.get('sample_size', 0)} trades scanned")
    print(f"  Whale Buys: {whales.get('buys', 0)} | Sells: {whales.get('sells', 0)}")
    print(f"  Net Flow: ${whales.get('net_value', 0):,.0f}")
    print(f"  Trend: {whales.get('trend', 'N/A')}")
else:
    print(f"  [WARNING]  {whales.get('error', whales.get('note', 'Unknown'))}")

# 4. News
print("\n[4/5] Fetching Tesla news...")
news = fetch_news_articles("Tesla", days=3, max_articles=3)
if news:
    print(f"  [SUCCESS] {len(news)} articles fetched")
    for a in news[:2]:
        print(f"  [NEWS] {a.get('title', 'No title')[:60]}...")
else:
    print("  [FAILED] No articles returned")

# 5. Fear & Greed
print("\n[5/5] Fetching Fear & Greed Index...")
fg = fetch_fear_greed_index()
if fg and fg.get("score"):
    print(f"  [SUCCESS] Score: {fg['score']} ({fg.get('label', 'N/A')})")
else:
    print(f"  [WARNING]  {fg}")

print("\n" + "=" * 60)
print("  SMOKE TEST COMPLETE")
print("=" * 60)
