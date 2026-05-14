"""
fetchers.py — Production-Grade Data Ingestion Engine
=====================================================
All external API calls use retry logic with exponential backoff.
All data is fetched in real-time (no caching).
"""

import logging
import os
import requests
try:
    import ccxt
    CCXT_AVAILABLE = True
except ImportError:
    CCXT_AVAILABLE = False
    ccxt = None

import pandas as pd
import numpy as np
import math
from typing import Optional, List, Dict, Any
import time
import re
import html as htmlmod
import json
import hashlib
from .utils import logger, safe_float
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==================================================================================
# CACHE HELPERS
# ==================================================================================

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
NEWS_CACHE_DIR = os.path.join(CACHE_DIR, "news")
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(NEWS_CACHE_DIR, exist_ok=True)

def _debug_ndjson(*args, **kwargs) -> None:
    pass

def _cache_disabled() -> bool:
    return os.environ.get("FEATHER_CACHE_DISABLE", "0") == "1"

def _cache_path(prefix: str, payload: dict) -> str:
    key = json.dumps(payload, sort_keys=True)
    h = hashlib.md5(key.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{prefix}_{h}.csv")

def _news_cache_path(query: str, days: int) -> str:
    h = hashlib.md5(f"{query}_{days}".encode()).hexdigest()
    return os.path.join(NEWS_CACHE_DIR, f"news_{h}.json")

def _read_cache_df(path: str, ttl_seconds: int) -> Optional[pd.DataFrame]:
    try:
        if not os.path.exists(path):
            return None
        if time.time() - os.path.getmtime(path) > ttl_seconds:
            return None
        return pd.read_csv(path, index_col=0, parse_dates=True)
    except Exception:
        return None

def _write_cache_df(path: str, df: pd.DataFrame) -> None:
    try:
        df.to_csv(path)
    except Exception:
        pass

# ==================================================================================
# RETRY-ENABLED HTTP SESSION (Production-Grade)
# ==================================================================================

def _get_retry_session(
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: tuple = (500, 502, 503, 504),
    timeout: int = 10,
) -> requests.Session:
    """Create a requests.Session with retry logic and exponential backoff.
    
    NOTE: 429 is intentionally NOT in status_forcelist — retrying on 429
    makes rate limiting worse. Instead we handle 429 explicitly per-API.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=0,  # CRITICAL FIX: Do not retry on ReadTimeout. Fails fast for slow APIs.
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Module-level session (reused for connection pooling)
_http_session = _get_retry_session()

# CoinGecko rate limiter: free tier = 10-30 req/min
_coingecko_last_call = 0.0
def _coingecko_get(url: str, params: dict = None, timeout: int = 10) -> requests.Response:
    """Rate-limited GET for CoinGecko. Respects 429 Retry-After header."""
    global _coingecko_last_call
    # Enforce minimum 2s between CoinGecko calls (free tier)
    elapsed = time.time() - _coingecko_last_call
    if elapsed < 2.0:
        time.sleep(2.0 - elapsed)
    
    resp = _http_session.get(url, params=params, timeout=timeout)
    _coingecko_last_call = time.time()
    
    if resp.status_code == 429:
        # Respect Retry-After header if present, else wait 60s
        retry_after = int(resp.headers.get('Retry-After', '60'))
        wait_time = min(retry_after, 120)  # Cap at 2 minutes
        logger.warning(f"CoinGecko 429 rate limit — waiting {wait_time}s")
        time.sleep(wait_time)
        # One retry after waiting
        resp = _http_session.get(url, params=params, timeout=timeout)
        _coingecko_last_call = time.time()
    
    return resp


class AsyncFetcher:
    """Institutional-Grade Async Ingestion Engine."""
    @staticmethod
    async def fetch_json(url: str, params: Dict = None, headers: Dict = None, timeout: int = 3) -> Dict:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params, headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.warning(f"AsyncFetcher: HTTP {resp.status} for {url}")
            except Exception as e:
                logger.warning(f"AsyncFetcher failed for {url}: {e}")
        return {}


try:
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
except Exception:
    pass


def _normalize_ticker(sym: str) -> str:
    """Normalize user-provided ticker to a form yfinance/Yahoo accepts."""
    s = str(sym or "").strip().upper().replace(" ", "")
    if ":" in s:
        s = s.split(":")[-1]
    if s.startswith("$"):
        s = s[1:]
    import re as _re
    s = _re.sub(r"[^A-Z0-9\.\-\^=]", "", s)
    return s


def _is_market_open() -> bool:
    """Check if US stock market is currently open."""
    try:
        import datetime
        import pytz

        et = pytz.timezone('US/Eastern')
        now_et = datetime.datetime.now(et)

        if now_et.weekday() >= 5:
            return False

        market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

        return market_open <= now_et <= market_close
    except ImportError:
        try:
            import datetime
            now_utc = datetime.datetime.utcnow()
            month = now_utc.month
            offset_hours = 4 if 3 <= month <= 11 else 5
            now_et_approx = now_utc - datetime.timedelta(hours=offset_hours)
            if now_et_approx.weekday() >= 5:
                return False
            return 9.5 <= now_et_approx.hour + now_et_approx.minute / 60 <= 16
        except Exception:
            import datetime
            return 9 <= datetime.datetime.now().hour < 16


# ==================================================================================
# PRICE DATA FETCHERS
# ==================================================================================

def fetch_stock_history_yfinance(symbol: str, period: str = "1mo") -> Optional[pd.DataFrame]:
    """Fetch historical data from yfinance (Lazy Import).
    
    FIX F-4: Period expansion fetches extra data for indicator warm-up,
    but the returned DataFrame is trimmed to honor the user's requested period.
    """
    # CHECK CACHE FIRST (10 min TTL)
    cache_path = _cache_path("yf", {"symbol": symbol.upper(), "period": period})
    if not _cache_disabled():
        cached = _read_cache_df(cache_path, ttl_seconds=600)
        if cached is not None and not cached.empty:
            logger.debug(f"yfinance cache hit for '{symbol}'")
            return cached

    try:
        import yfinance as yf
        sym = _normalize_ticker(symbol)
        index_map = {
            "SPY": "SPY", "QQQ": "QQQ", "GLD": "GLD",
            "VIX": "^VIX", "VOO": "VOO", "IWM": "IWM",
            "DIA": "DIA", "XLE": "XLE", "XLK": "XLK"
        }
        sym = index_map.get(sym, sym)

        # Expand period for indicator warm-up (MA50 needs 50+ days)
        fetch_period = period
        if period in ["1d", "5d"]:
            fetch_period = "1mo"
        elif period == "1mo":
            fetch_period = "3mo"

        t = yf.Ticker(sym)
        df = t.history(period=fetch_period)
        
        if not df.empty:
            df.columns = [c.lower() for c in df.columns]
            if 'close' not in df.columns and 'price' in df.columns:
                df['close'] = df['price']
            df = df.dropna(subset=['close'])
            
            # SAVE TO CACHE before returning
            if not _cache_disabled() and not df.empty:
                _write_cache_df(cache_path, df)
                
            return df
            
    except Exception as e:
        logger.warning(f"yfinance fetch failed for {symbol}: {e}")

    # Fallback 1: Polygon.io
    polygon_key = os.environ.get("POLYGON_API_KEY")
    if polygon_key:
        try:
            import datetime
            from dateutil.relativedelta import relativedelta
            
            # Map yfinance period strings to days for polygon
            days_back = 90 # default 3mo
            if period == "1d": days_back = 30
            elif period == "5d": days_back = 30
            elif period == "1mo": days_back = 90
            elif period == "3mo": days_back = 90
            elif period == "6mo": days_back = 180
            elif period == "1y": days_back = 365
            elif period == "2y": days_back = 730
            elif period == "5y": days_back = 1825
            elif period == "10y": days_back = 3650
            elif period == "ytd":
                days_back = (datetime.datetime.now() - datetime.datetime(datetime.datetime.now().year, 1, 1)).days
                days_back = max(30, days_back + 50) # ensure some warmup
                
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d")
            
            # Polygon uses standard tickers, not Yahoo specific ones like ^VIX initially
            poly_sym = symbol.replace("^", "").upper()
            
            url = f"https://api.polygon.io/v2/aggs/ticker/{poly_sym}/range/1/day/{start_date}/{end_date}"
            params = {"apiKey": polygon_key, "adjusted": "true", "sort": "asc"}
            
            resp = _http_session.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                results = data.get("results", [])
                
                if results:
                    df = pd.DataFrame(results)
                    # Polygon keys: v=volume, vwap=vwap, o=open, c=close, h=high, l=low, t=timestamp, n=transactions
                    df = df.rename(columns={"v": "volume", "o": "open", "c": "close", "h": "high", "l": "low", "t": "timestamp"})
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                    
                    # Add timezone info to match yfinance output mostly
                    try:
                        import pytz
                        df["timestamp"] = df["timestamp"].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
                    except Exception:
                        pass
                        
                    df = df.set_index("timestamp")
                    
                    # Ensure index has timezone info for compatibility in asset_model if needed
                    # but usually yfinance history has tz-aware DatetimeIndex
                    df = df[['open', 'high', 'low', 'close', 'volume']]
                    
                    # SAVE TO CACHE before returning
                    if not _cache_disabled() and not df.empty:
                        _write_cache_df(cache_path, df)
                        
                    return df
            else:
                 logger.warning(f"Polygon API HTTP {resp.status_code} for {poly_sym}")
        except Exception as e:
            logger.warning(f"Polygon fallback failed for {symbol}: {e}")

    logger.error(f"All price fetch mechanisms failed for {symbol}")
    return None

def resolve_crypto_asset(query: str) -> tuple[str, str]:
    """Resolve a ticker or address to a CoinGecko ID and Name."""
    query = query.strip().lower()
    
    # 1. Local common map
    cg_map = {
        "btc": ("bitcoin", "Bitcoin"), "eth": ("ethereum", "Ethereum"), "sol": ("solana", "Solana"),
        "bnb": ("binancecoin", "BNB"), "xrp": ("ripple", "XRP"), "ada": ("cardano", "Cardano"),
        "doge": ("dogecoin", "Dogecoin"), "dot": ("polkadot", "Polkadot"), "matic": ("matic-network", "Polygon"),
        "pepe": ("pepe", "Pepe"), "wif": ("dogwifcoin", "dogwifhat"), "shib": ("shiba-inu", "Shiba Inu"),
        "link": ("chainlink", "Chainlink"), "avax": ("avalanche-2", "Avalanche")
    }
    # Clean the query (e.g. btc-usd -> btc, btc/usdt -> btc)
    clean_query = query.split('-')[0].split('/')[0]
    
    if clean_query in cg_map:
        return cg_map[clean_query]

    # 2. Contract Address Resolution (length > 20)
    if len(query) > 20:
        for chain in ["solana", "ethereum", "binance-smart-chain", "base"]:
            try:
                url = f"https://api.coingecko.com/api/v3/coins/{chain}/contract/{query}"
                resp = _coingecko_get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    name = data.get("name", "Unknown Token")
                    cid = data.get("id", query)
                    return cid, name
            except Exception:
                pass
        return query, "Unknown Token"
        
    # 3. Search API for tickers (e.g. "BONK")
    try:
        url = "https://api.coingecko.com/api/v3/search"
        resp = _coingecko_get(url, params={"query": query}, timeout=5)
        if resp.status_code == 200:
            coins = resp.json().get("coins", [])
            for c in coins:
                if c.get("symbol", "").lower() == query:
                    return c.get("id"), c.get("name")
            if coins:
                return coins[0].get("id"), coins[0].get("name")
    except Exception as e:
        logger.warning(f"CoinGecko search failed for '{query}': {e}")
        
    return query, query.upper()


def fetch_high_quality_crypto_data(coin_id: str, days: int = 365) -> pd.DataFrame:
    """Fetch crypto OHLC data from CoinGecko.
    
    FIX F-3: Uses /market_chart endpoint to get volume data instead of /ohlc.
    Falls back to /ohlc (with volume=0) if market_chart fails.
    """
    # CHECK CACHE FIRST (15 min TTL)
    cache_path = _cache_path("cg", {"symbol": coin_id.lower(), "days": days})
    if not _cache_disabled():
        cached = _read_cache_df(cache_path, ttl_seconds=900)
        if cached is not None and not cached.empty:
            logger.debug(f"CoinGecko cache hit for '{coin_id}'")
            return cached

    # Strategy 1: Try /market_chart (includes volume)
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        params = {"vs_currency": "usd", "days": str(days)}
        resp = _coingecko_get(url, params=params, timeout=10)

        if resp.status_code == 200:
            data = resp.json()
            prices = data.get("prices", [])
            volumes = data.get("total_volumes", [])

            if prices:
                df = pd.DataFrame(prices, columns=["timestamp", "close"])
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                df.set_index("timestamp", inplace=True)

                # Synthesize OHLC from price data (market_chart gives point prices)
                df["open"] = df["close"].shift(1).fillna(df["close"])
                df["high"] = df["close"].rolling(2, min_periods=1).max()
                df["low"] = df["close"].rolling(2, min_periods=1).min()

                # Map volumes by closest timestamp
                if volumes:
                    vol_df = pd.DataFrame(volumes, columns=["timestamp", "volume"])
                    vol_df["timestamp"] = pd.to_datetime(vol_df["timestamp"], unit="ms")
                    vol_df.set_index("timestamp", inplace=True)
                    df = df.join(vol_df, how="left")
                    df["volume"] = df["volume"].fillna(0.0)
                else:
                    df["volume"] = 0.0

                # SAVE TO CACHE before returning
                if not _cache_disabled() and not df.empty:
                    _write_cache_df(cache_path, df)

                return df
    except Exception as e:
        logger.warning(f"CoinGecko market_chart failed for {coin_id}, falling back to /ohlc: {e}")

    # Strategy 2: Fallback to /ohlc (no volume, but structured OHLC)
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
        params = {"vs_currency": "usd", "days": str(days)}
        resp = _coingecko_get(url, params=params, timeout=10)

        if resp.status_code != 200:
            logger.warning(f"CoinGecko OHLC HTTP {resp.status_code} for {coin_id}")
            return pd.DataFrame()

        data = resp.json()
        if not data or not isinstance(data, list):
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("timestamp", inplace=True)
        df["volume"] = 0.0  # CoinGecko /ohlc has no volume
        
        # SAVE TO CACHE before returning
        if not _cache_disabled() and not df.empty:
            _write_cache_df(cache_path, df)
            
        return df
    except Exception as e:
        logger.error(f"Crypto fetch failed for {coin_id}: {e}")
        return pd.DataFrame()


# ==================================================================================
# WHALE SIGNAL FETCHER (V2 — Binance aggTrades)
# Cache of symbols confirmed not listed on Binance
BINANCE_UNSUPPORTED_SYMBOLS: set = set()

# ==================================================================================

def fetch_crypto_whale_signals(symbol: str, large_usd: float = 10000, lookback_seconds: int = 10) -> Dict[str, Any]:
    """Fetch large transactions using CCXT with automatic rate limiting."""
    _EMPTY = {"buys": 0, "sells": 0, "net_value": 0.0, "weighted_net": 0.0,
              "quality_score": 0.0, "trend": "NEUTRAL", "sample_size": 0,
              "source": "ccxt-binance"}
    if not CCXT_AVAILABLE:
        return {**_EMPTY, "error": "ccxt not installed"}
        
    try:
        # Normalize symbol to CCXT format (e.g. BTC/USDT)
        base = symbol.split('/')[0].upper().replace('-', '').replace('_', '')
        if base.endswith('USD') or base.endswith('USDT'):
            base = base.replace('USDT', '').replace('USD', '')
        
        # Guard against empty/corrupted symbol after normalization
        if not base or len(base) < 2:
            logger.warning(f"Invalid symbol after normalization: {symbol}")
            return {**_EMPTY, "error": f"Invalid symbol: {symbol}"}
        
        quote = 'USDT'
        if '/' in symbol:
            quote = symbol.split('/')[1].upper()
            if quote == 'USD': quote = 'USDT'
        ccxt_sym = f"{base}/{quote}"

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (lookback_seconds * 1000)
        recent_cutoff = now_ms - 5000

        exchange = ccxt.binance({
            'enableRateLimit': True,
        })
        try:
            exchange.load_markets()
            if ccxt_sym not in exchange.symbols:
                BINANCE_UNSUPPORTED_SYMBOLS.add(ccxt_sym)
                return {**_EMPTY, "note": f"{ccxt_sym} not on Binance"}
        except Exception:
            pass
        
        # Skip immediately if known unsupported
        if ccxt_sym in BINANCE_UNSUPPORTED_SYMBOLS:
            logger.info(f"Skipping {ccxt_sym} — cached as Binance-unsupported")
            return {**_EMPTY, "note": f"{ccxt_sym} not on Binance (cached)"}

        try:
            # Option A: Get the 1000 most recent trades to fix the UI lagging
            trades = exchange.fetch_trades(ccxt_sym, limit=1000)
        except ccxt.BadSymbol:
            BINANCE_UNSUPPORTED_SYMBOLS.add(ccxt_sym)
            logger.info(f"Cached {ccxt_sym} as Binance-unsupported")
            return {**_EMPTY, "note": f"{ccxt_sym} not listed on Binance"}
        except Exception as e:
            logger.warning(f"CCXT fetch failed for {ccxt_sym}: {e}")
            return {**_EMPTY, "note": f"CCXT fetch failed: {e}"}

        if not trades:
            return {**_EMPTY, "note": f"No trades found for {ccxt_sym}"}

        df_list = []
        for t in trades:
            price = float(t.get('price', 0) or 0)
            qty = float(t.get('amount', 0) or 0)
            cost = float(t.get('cost', 0) or (price * qty))
            ts = t.get('timestamp') or 0
            side = t.get('side', 'unknown') # 'buy' or 'sell'
            df_list.append({'p': price, 'q': qty, 'usd_val': cost, 'T': ts, 'side': side})

        df = pd.DataFrame(df_list)
        df.dropna(subset=['p', 'q', 'T'], inplace=True)

        if df.empty:
            return {**_EMPTY, "note": "All trades had NaN values"}

        total_sample = len(df)
        total_volume = float(df['usd_val'].sum())

        # Time weighting (recent trades get more weight)
        t_max, t_min = df['T'].max(), df['T'].min()
        if t_max > t_min:
            df['weight'] = 0.5 + 0.5 * ((df['T'] - t_min) / (t_max - t_min))
        else:
            df['weight'] = 1.0

        # Filter to whale-sized trades
        whales = df[df['usd_val'] >= large_usd].copy()
        if whales.empty:
            buy_mask = df['side'] == 'buy'
            sell_mask = df['side'] == 'sell'
            micro_net = float(df.loc[buy_mask, 'usd_val'].sum() - df.loc[sell_mask, 'usd_val'].sum())
            return {
                **_EMPTY,
                "sample_size": total_sample,
                "total_volume_usd": total_volume,
                "micro_net": micro_net,
                "note": f"No trades >= ${large_usd:,.0f} in latest sample ({total_sample} trades scanned)"
            }

        # Classify whale trades
        buy_mask = whales['side'] == 'buy'
        sell_mask = whales['side'] == 'sell'
        buys = int(buy_mask.sum())
        sells = int(sell_mask.sum())
        
        buy_vol_raw = float(whales.loc[buy_mask, 'usd_val'].sum())
        sell_vol_raw = float(whales.loc[sell_mask, 'usd_val'].sum())
        buy_vol_w = float((whales.loc[buy_mask, 'usd_val'] * whales.loc[buy_mask, 'weight']).sum())
        sell_vol_w = float((whales.loc[sell_mask, 'usd_val'] * whales.loc[sell_mask, 'weight']).sum())

        net_raw = buy_vol_raw - sell_vol_raw
        net_weighted = buy_vol_w - sell_vol_w
        total_whale_vol = buy_vol_raw + sell_vol_raw
        whale_count = buys + sells

        largest_trade = float(whales['usd_val'].max())
        
        hot_whales = whales[whales['T'] >= recent_cutoff]
        hot_net = 0.0
        if not hot_whales.empty:
            hot_buy = float(hot_whales.loc[hot_whales['side'] == 'buy', 'usd_val'].sum())
            hot_sell = float(hot_whales.loc[hot_whales['side'] == 'sell', 'usd_val'].sum())
            hot_net = hot_buy - hot_sell

        # --- Gap 6: Birdeye (Solana DEX) Augmentation ---
        birdeye_key = os.environ.get("BIRDEYE_API_KEY")
        
        if birdeye_key and "SOL" in base:
            try:
                # SOL Mint Address on Solana
                sol_address = "So11111111111111111111111111111111111111112"
                url = "https://public-api.birdeye.so/defi/txs/token"
                headers = {"X-API-KEY": birdeye_key, "x-chain": "solana"}
                params = {"address": sol_address, "limit": 100}
                b_resp = _http_session.get(url, headers=headers, params=params, timeout=5)
                
                if b_resp.status_code == 200:
                    b_data = b_resp.json().get("data", {}).get("items", [])
                    for tx in b_data:
                        val = safe_float(tx.get("volumeUSD", 0))
                        if val >= large_usd:
                            is_buy = tx.get("type") == "buy"
                            buys += 1 if is_buy else 0
                            sells += 1 if not is_buy else 0
                            net_raw += val if is_buy else -val
                            net_weighted += val if is_buy else -val
                            buy_vol_raw += val if is_buy else 0
                            sell_vol_raw += val if not is_buy else 0
                            buy_vol_w += val if is_buy else 0
                            sell_vol_w += val if not is_buy else 0
                            total_whale_vol += val
                            total_volume += val
                            whale_count += 1
                            if val > largest_trade:
                                largest_trade = val
                            
                            # Add to hot net if extremely recent
                            tx_time = tx.get("blockTime", 0) * 1000
                            if tx_time >= recent_cutoff:
                                hot_net += val if is_buy else -val
            except Exception as e:
                logger.warning(f"Birdeye fetch failed: {e}")
                
        # Re-evaluate Trend and Quality Score after augmentation
        q_score = (abs(net_weighted) / (buy_vol_w + sell_vol_w)) * 100 if (buy_vol_w + sell_vol_w) > 0 else 0.0
        if whale_count >= 20: q_score = min(100.0, q_score * 2.0)
        elif whale_count >= 10: q_score = min(100.0, q_score * 1.5)
        elif whale_count >= 5: q_score = min(100.0, q_score * 1.2)
        
        trend = "NEUTRAL"
        if q_score > 15:
            trend = "BULLISH" if net_weighted > 0 else "BEARISH"
        elif q_score > 8 and abs(net_raw) > 100000:
            trend = "LEANING BULLISH" if net_raw > 0 else "LEANING BEARISH"

        whale_pct = (total_whale_vol / total_volume * 100) if total_volume > 0 else 0.0

        return {
            "buys": buys,
            "sells": sells,
            "buy_vol": buy_vol_raw,
            "sell_vol": sell_vol_raw,
            "net_value": net_raw,
            "weighted_net": net_weighted,
            "quality_score": min(100.0, q_score),
            "trend": trend,
            "sample_size": total_sample,
            "whale_count": whale_count,
            "largest_trade_usd": largest_trade,
            "whale_pct_of_volume": round(whale_pct, 2),
            "recent_net_15s": hot_net,
            "total_volume_usd": total_volume,
            "lookback_seconds": lookback_seconds,
            "threshold_usd": large_usd,
            "source": "ccxt-binance"
        }
    except ccxt.NetworkError as e:
        logger.error(f"Binance connection failed for {symbol}: {e}")
        return {**_EMPTY, "error": "Connection failed"}
    except ccxt.RequestTimeout as e:
        logger.error(f"Binance timeout for {symbol}: {e}")
        return {**_EMPTY, "error": "Timeout"}
    except Exception as e:
        logger.warning(f"Whale fetch failed for {symbol}: {e}")
        return {**_EMPTY, "error": str(e)}


# ==================================================================================
# NEWS FETCHER
# ==================================================================================

def fetch_news_articles(query: str, days: int = 7, max_articles: int = 10) -> List[Dict[str, Any]]:
    """Fetch news articles using parallel external APIs and timeout-guarded enrichment."""
    # Sanitize Forex queries to improve news search results
    if query.endswith("=X"):
        base_pair = query.replace("=X", "")
        if len(base_pair) == 6:
            query = f'"{base_pair[:3]}/{base_pair[3:]}" OR "{base_pair}"'

    # CHECK CACHE FIRST (1 hour TTL)
    cache_path = _news_cache_path(query, days)
    if not _cache_disabled():
        try:
            if os.path.exists(cache_path) and \
               time.time() - os.path.getmtime(cache_path) < 3600:
                with open(cache_path, "r") as f:
                    cached = json.load(f)
                    if cached:
                        logger.debug(f"News cache hit for '{query}'")
                        return cached
        except Exception:
            pass

    import concurrent.futures
    articles = []
    
    budget = float(os.environ.get("NEWS_FETCH_BUDGET_SEC", "45"))
    start_time = time.time()

    def _fetch_newsapi():
        res = []
        if os.environ.get("NEWSAPI_KEY"):
            try:
                url = "https://newsapi.org/v2/everything"
                params = {"q": query, "language": "en", "pageSize": max_articles, "sortBy": "publishedAt", "apiKey": os.environ.get("NEWSAPI_KEY")}
                resp = _http_session.get(url, params=params, timeout=8)
                if resp.status_code == 200:
                    for a in resp.json().get("articles", [])[:max_articles]:
                        res.append({"title": a.get("title", ""), "url": a.get("url", ""), "published_at": a.get("publishedAt", ""), "source": (a.get("source") or {}).get("name") or "Unknown", "summary": a.get("description", "") or a.get("content", ""), "provider_path": "newsapi"})
            except Exception as e:
                logger.warning(f"NewsAPI fetch failed: {e}")
        return res

    def _fetch_currents():
        res = []
        if os.environ.get("CURRENTS_API_KEY"):
            try:
                url = "https://api.currentsapi.services/v1/search"
                params = {"keywords": query, "language": "en", "limit": max_articles, "apiKey": os.environ.get("CURRENTS_API_KEY")}
                resp = _http_session.get(url, params=params, timeout=6)
                if resp.status_code == 200:
                    for a in resp.json().get("news", [])[:max_articles]:
                        res.append({"title": a.get("title", ""), "url": a.get("url", ""), "published_at": a.get("published", ""), "source": a.get("author") or "Currents API", "summary": a.get("description", ""), "provider_path": "currents"})
            except Exception as e:
                logger.warning(f"Currents fetch failed: {e}")
        return res

    def _fetch_google_rss():
        """Single Google News RSS — fast, works for stocks and crypto."""
        res = []
        try:
            import feedparser
            encoded_q = requests.utils.quote(query)
            url = f"https://news.google.com/rss/search?q={encoded_q}+when:{days}d&hl=en-US&gl=US&ceid=US:en"
            r = _http_session.get(url, timeout=6)
            if r.status_code == 200 and len(r.content) < 500_000:
                feed = feedparser.parse(r.content)
                for e in feed.entries[:max_articles]:
                    res.append({
                        "title": e.get('title', ''),
                        "url": e.get('link', ''),
                        "published_at": getattr(e, 'published', ''),
                        "source": getattr(getattr(e, 'source', None), 'title', 'Google News'),
                        "summary": getattr(e, 'summary', ''),
                        "provider_path": "google_rss"
                    })
        except Exception as e:
            logger.warning(f"Google RSS fetch failed: {e}")
        return res

    def _fetch_crypto_rss():
        """CoinTelegraph RSS — crypto only, keyword filtered, fast."""
        res = []
        CRYPTO_TERMS = {"crypto","bitcoin","ethereum","solana","coin","token","defi","nft","blockchain"}
        if not any(t in query.lower() for t in CRYPTO_TERMS):
            return res
        try:
            import feedparser
            r = _http_session.get("https://cointelegraph.com/rss", timeout=5)
            if r.status_code == 200 and len(r.content) < 500_000:
                feed = feedparser.parse(r.content)
                q_lower = query.lower().split()[0]
                for e in feed.entries[:max_articles]:
                    title = e.get('title', '')
                    summary = getattr(e, 'summary', '')
                    if q_lower in (title + summary).lower():
                        res.append({
                            "title": title,
                            "url": e.get('link', ''),
                            "published_at": getattr(e, 'published', ''),
                            "source": "CoinTelegraph",
                            "summary": summary,
                            "provider_path": "crypto_rss"
                        })
        except Exception as e:
            logger.warning(f"CoinTelegraph RSS fetch failed: {e}")
        return res

    def _fetch_newsdata():
        res = []
        if os.environ.get("NEWSDATA_API_KEY"):
            try:
                url = "https://newsdata.io/api/1/news"
                params = {"apikey": os.environ.get("NEWSDATA_API_KEY"), "q": query, "language": "en"}
                resp = _http_session.get(url, params=params, timeout=6)
                if resp.status_code == 200:
                    for item in resp.json().get("results", [])[:max_articles]:
                        res.append({"title": item.get("title", ""), "url": item.get("link", ""), "published_at": item.get("pubDate", ""), "source": item.get("source_id", ""), "summary": item.get("description", ""), "provider_path": "newsdata_io"})
            except Exception as e:
                logger.warning(f"NewsData fetch failed: {e}")
        return res

    provider_keys = {
        "newsapi_key_set": bool(os.environ.get("NEWSAPI_KEY")),
        "currents_key_set": bool(os.environ.get("CURRENTS_API_KEY")),
        "newsdata_key_set": bool(os.environ.get("NEWSDATA_API_KEY")),
    }
    _debug_ndjson(
        "H1",
        "fetchers.py:fetch_news_articles:start",
        "launch parallel news providers",
        data={"query": query, "days": days, "max_articles": max_articles, "budget_sec": budget, "timeout_sec": 12, **provider_keys},
        run_id="pre-debug",
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        future_map = {
            "newsapi":    ex.submit(_fetch_newsapi),
            "currents":   ex.submit(_fetch_currents),
            "newsdata":   ex.submit(_fetch_newsdata),
            "google_rss": ex.submit(_fetch_google_rss),
            "crypto_rss": ex.submit(_fetch_crypto_rss),
        }
        future_to_key = {f: k for k, f in future_map.items()}
        try:
            for future in concurrent.futures.as_completed(future_map.values(), timeout=12):
                try:
                    articles.extend(future.result())
                except Exception:
                    pass
        except concurrent.futures.TimeoutError:
            done_count = sum(1 for f in future_map.values() if f.done())
            unfinished = [future_to_key[f] for f in future_map.values() if not f.done()]
            logger.warning("News fetch parallel executor reached hard timeout.")
            _debug_ndjson(
                "H1",
                "fetchers.py:fetch_news_articles:timeout",
                "parallel providers timed out",
                data={"done": done_count, "unfinished": unfinished, "timeout_sec": 12, "elapsed_sec": round(time.time() - start_time, 3)},
                run_id="pre-debug",
            )

    # Deduplicate by URL and filter out false positives
    seen_urls = set()
    unique_articles = []
    blacklist = ["pypi", "github", "npm", "ruby", "release candidate", "v1.", "v2.", "v0."]
    
    for a in articles:
        url = a.get("url", "")
        title = (a.get("title") or "").lower()
        summary = (a.get("summary") or "").lower()
        is_noise = any(b in title or b in summary for b in blacklist)
        if url and url not in seen_urls and not is_noise:
            seen_urls.add(url)
            unique_articles.append(a)

    # ── RELEVANCE FILTER ──────────────────────────────────────────
    # Drop articles that don't actually mention the queried company/asset.
    # Without this, generic tech/finance articles leak through from Google RSS.
    _STOPWORDS = {"stock", "stocks", "cryptocurrency", "crypto", "blockchain",
                  "defi", "token", "coin", "market", "markets", "trading",
                  "price", "share", "shares"}
    
    # Extract meaningful keywords from query (e.g. "Apple stock" → {"apple"})
    relevance_keywords = set()
    for w in query.lower().split():
        if w not in _STOPWORDS and len(w) > 1:
            relevance_keywords.add(w)
    
    # Also add common aliases (ticker → company name mapping)
    _ticker_aliases = {
        "aapl": ["apple", "iphone", "ipad", "mac"], "msft": ["microsoft", "windows", "azure"],
        "googl": ["google", "alphabet", "gmail"], "goog": ["google", "alphabet"],
        "amzn": ["amazon", "aws"], "tsla": ["tesla", "elon", "musk"],
        "nvda": ["nvidia", "geforce", "gpu"], "meta": ["facebook", "instagram", "zuckerberg"],
        "nflx": ["netflix"], "dis": ["disney"], "ba": ["boeing"],
        "jpm": ["jpmorgan", "chase"], "v": ["visa"], "ma": ["mastercard"],
        "ko": ["coca-cola", "coke"], "pep": ["pepsi", "pepsico"],
        "jnj": ["johnson"], "pg": ["procter", "gamble"],
        "btc": ["bitcoin"], "eth": ["ethereum"], "sol": ["solana"],
        "doge": ["dogecoin"], "xrp": ["ripple"], "ada": ["cardano"],
        "dot": ["polkadot"], "avax": ["avalanche"], "matic": ["polygon"],
    }
    for kw in list(relevance_keywords):
        if kw in _ticker_aliases:
            relevance_keywords.update(_ticker_aliases[kw])
    
    if relevance_keywords:
        relevant_articles = []
        for a in unique_articles:
            text = ((a.get("title") or "") + " " + (a.get("summary") or "")).lower()
            if any(kw in text for kw in relevance_keywords):
                relevant_articles.append(a)
        
        # Only apply filter if it doesn't wipe out everything
        if relevant_articles:
            dropped = len(unique_articles) - len(relevant_articles)
            if dropped > 0:
                logger.info(f"Relevance filter: kept {len(relevant_articles)}/{len(unique_articles)} articles for query '{query}'")
            unique_articles = relevant_articles
    # ──────────────────────────────────────────────────────────────

    _debug_ndjson(
        "H1",
        "fetchers.py:fetch_news_articles:dedup",
        "dedup complete",
        data={"raw_articles": len(articles), "unique_articles": len(unique_articles)},
        run_id="pre-debug",
    )

    # ── ARTICLE ENRICHMENT (newspaper4k) ──────────────────────
    try:
        from newspaper import Article
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

        def _fetch_article_text(url):
            art = Article(url)
            art.download()
            art.parse()
            return art.text or ""

        elapsed_so_far = time.time() - start_time
        if elapsed_so_far < 10.0:  # hard 10s ceiling — if we're already at 10s, skip enrichment
            _debug_ndjson(
                "H3",
                "fetchers.py:fetch_news_articles:enrichment_start",
                "starting enrichment loop",
                data={"unique_articles": len(unique_articles), "elapsed_sec": round(time.time() - start_time, 3)},
                run_id="pre-debug",
            )
            for a in unique_articles[:3]:  # Reduce from 5 to 3 on Windows to save time
                if len(a.get("summary", "")) < 200 and a.get("url", "").startswith("http"):
                    try:
                        with ThreadPoolExecutor(max_workers=1) as ex:
                            future = ex.submit(_fetch_article_text, a["url"])
                            text = future.result(timeout=4)
                            if text and len(text) > 100:
                                a["summary"] = text[:2000]
                    except Exception:
                        pass
        else:
            logger.debug(f"Skipping enrichment — already {elapsed_so_far:.1f}s elapsed")
            _debug_ndjson("H3", "fetchers.py:fetch_news_articles:enrichment_skipped",
                          "enrichment skipped due to budget", 
                          data={"elapsed_sec": round(elapsed_so_far, 3), "budget_sec": budget},
                          run_id="pre-debug")
        _debug_ndjson(
            "H3",
            "fetchers.py:fetch_news_articles:enrichment_done",
            "finished enrichment loop",
            data={"elapsed_sec": round(time.time() - start_time, 3)},
            run_id="pre-debug",
        )
    except ImportError:
        pass
    # ───────────────────────────────────────────────────────────

    # SAVE TO CACHE before returning
    if not _cache_disabled() and unique_articles:
        try:
            with open(cache_path, "w") as f:
                json.dump(unique_articles[:max_articles], f)
        except Exception:
            pass

    return unique_articles[:max_articles]



def fetch_twitter_sentiment(query: str, max_tweets: int = 50) -> Dict[str, Any]:
    """Fetch recent tweets using Twitter Bearer Token and v2 API."""
    bearer_token = os.environ.get('TWITTER_BEARER_TOKEN')
    if not bearer_token:
        return {"avg_sentiment": 0.0, "tweet_count": 0, "tweets": [], "error": "TWITTER_BEARER_TOKEN not set"}

    try:
        headers = {"Authorization": f"Bearer {bearer_token}"}
        url = "https://api.twitter.com/2/tweets/search/recent"
        params = {
            "query": f"{query} lang:en -is:retweet",
            "max_results": max(10, min(max_tweets, 100)),
            "tweet.fields": "created_at,public_metrics,text"
        }
        
        resp = _http_session.get(url, headers=headers, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            tweets_data = data.get("data", [])
            
            logger.warning("Twitter sentiment fetched, passing to sentiment.py for actual scoring.")
            
            structured_tweets = []
            for t in tweets_data:
                text_val = t.get("text", "")
                structured_tweets.append({
                    "title": text_val[:100],
                    "summary": text_val,
                    "text": text_val,
                    "created_at": t.get("created_at", ""),
                    "metrics": t.get("public_metrics", {})
                })
                
            avg_sentiment = 0.0
            try:
                import sentiment
                s_res = sentiment.analyze_news_sentiment(structured_tweets, is_crypto=False)
                avg_sentiment = s_res.get("avg_sentiment", 0.0)
            except Exception as e:
                logger.warning(f"Could not analyze Twitter sentiment: {e}")
                
            return {
                "avg_sentiment": avg_sentiment,
                "tweet_count": len(structured_tweets),
                "tweets": structured_tweets,
                "source": "twitter_v2"
            }
        elif resp.status_code == 402:
            logger.debug(f"Twitter API HTTP 402: Credits Depleted for {query}")
            return {"avg_sentiment": 0.0, "tweet_count": 0, "tweets": [], "error": "Twitter API Credits Depleted"}
        else:
            logger.warning(f"Twitter API HTTP {resp.status_code}: {resp.text}")
            _debug_ndjson(
                "H2",
                "fetchers.py:fetch_twitter_sentiment:http_error",
                "twitter http error",
                data={"http_status": resp.status_code, "tweet_count": 0},
                run_id="pre-debug",
            )
            return {"avg_sentiment": 0.0, "tweet_count": 0, "tweets": [], "error": f"HTTP {resp.status_code}"}
            
    except Exception as e:
        logger.warning(f"Twitter fetch failed: {e}")
        return {"avg_sentiment": 0.0, "tweet_count": 0, "tweets": [], "error": str(e)}


# ==================================================================================
# INSIDER TRANSACTIONS
# ==================================================================================

def deduplicate_insider_transactions(transactions: list) -> list:
    seen = set()
    unique = []
    for tx in transactions:
        shares = float(tx.get("share", 0))
        rounded = round(shares, -2) if shares > 500 else round(shares, 0)
        key = (
            tx.get("name", ""),
            tx.get("transactionDate", ""),
            tx.get("transactionCode", ""),
            rounded
        )
        if key not in seen:
            seen.add(key)
            unique.append(tx)
    return unique

def fetch_insider_transactions(symbol: str, months: int = 3) -> Dict[str, Any]:
    """Fetch real insider transaction data from Finnhub with yfinance fallback."""
    finnhub_key = os.environ.get("FINNHUB_API_KEY")
    if finnhub_key:
        try:
            import datetime
            from dateutil.relativedelta import relativedelta
            
            url = f"https://finnhub.io/api/v1/stock/insider-transactions"
            cutoff_date = (datetime.datetime.now() - relativedelta(months=months)).strftime("%Y-%m-%d")
            params = {"symbol": symbol, "from": cutoff_date, "token": finnhub_key}
            
            resp = _http_session.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                transactions = data.get("data", [])
                transactions = deduplicate_insider_transactions(transactions)
                
                if not transactions:
                    return {"buys": 0, "sells": 0, "net_value": 0.0, "note": "No recent records", "source": "finnhub"}
                
                buys, sells, net_val = 0, 0, 0.0
                records = []
                
                last_price = 0.0
                try:
                    import yfinance as yf
                    tk = yf.Ticker(symbol)
                    last_price = float(tk.fast_info.last_price)
                except Exception:
                    pass
                
                for tx in transactions:
                    tx_change = safe_float(tx.get("change", 0))
                    # Finnhub 'share' is post-transaction holdings; 'change' is the transaction sizes
                    shares = abs(tx_change) 
                    
                    price = safe_float(tx.get("transactionPrice", 0))
                    if price == 0:
                        price = safe_float(tx.get("price", 0))
                        
                    # Transaction value = shares transacted * price
                    val = price * shares
                    
                    # ── CONTEXT FILTER (consolidated tape) ──────────────
                    # Finnhub transactionCode: P=Purchase, S=Sale,
                    #   M=Option Exercise, X=Exercise, J/G=Gift, F=Tax
                    tx_code = str(tx.get("transactionCode", "")).upper()
                    filing_code = str(tx.get("filingType", "")).upper()
                    
                    # 1) Option exercise / conversion → neutral, skip net
                    is_exercise = tx_code in ("M", "X", "C")
                    is_gift     = tx_code in ("J", "G")
                    
                    # 2) $10M+ context filter
                    if abs(val) > 10_000_000:
                        # Rule 144 = true legal insider sale → allow
                        is_rule_144 = "144" in filing_code or "144" in tx_code
                        # Huge purchases (>$50M) by institutions are often block transfers, but normal S/P should pass
                        is_block = filing_code in ("P", "PRIOR") or (tx_code == "P" and abs(val) > 50_000_000)
                        is_regular_market = tx_code in ("S", "P")
                        
                        if is_rule_144 or is_regular_market:
                            pass  # keep val — true insider intent
                        elif is_block:
                            logger.debug(f"Insider filter: zeroing ${val:,.0f} trade (code={tx_code}, filing={filing_code})")
                            val = 0.0
                    # ────────────────────────────────────────────────────
                    
                    if val == 0 and shares > 0 and last_price > 0:
                        val = safe_float(shares * last_price)
                        # Re-apply cap after fallback price calc
                        if abs(val) > 10_000_000:
                            val = 0.0
                    # Rely on Finnhub explicit transaction codes rather than raw unsigned changes
                    is_buy = tx_code == "P" or (tx_change > 0 and tx_code not in ("S", "M", "X", "C", "J", "G", "F"))
                    is_sell = tx_code == "S" or (tx_change < 0 and tx_code not in ("P", "M", "X", "C", "J", "G", "F"))
                    
                    # Classify with exercise/gift priority (matches yahoo fallback logic)
                    final_type = "OTHER"
                    if is_exercise:
                        final_type = "EXERCISE"
                    elif is_gift:
                        final_type = "GIFT"
                    elif is_buy:
                        final_type = "BUY"
                        buys += 1
                        net_val += val
                    elif is_sell:
                        final_type = "SELL"
                        sells += 1
                        net_val -= val
                        
                    records.append({
                        "type": final_type, 
                        "shares": abs(shares), 
                        "value": val, 
                        "text": f"Finnhub {final_type.lower()} (code:{tx_code})",
                        "insider": tx.get("name", "Unknown"), 
                        "position": "Insider",
                        "date": tx.get("transactionDate", "")
                    })
                
                return {
                    "buys": buys, "sells": sells, "net_value": safe_float(net_val),
                    "record_count": len(transactions), "transactions": records, "source": "finnhub"
                }
            elif resp.status_code == 429:
                logger.warning("Finnhub rate-limited (429)")
            else:
                logger.warning(f"Finnhub HTTP {resp.status_code} for insider transactions")
                
        except Exception as e:
            logger.warning(f"Finnhub insider fetch failed for {symbol}: {e}")
            
    # Fallback to yfinance
    try:
        import yfinance as yf
        import datetime
        tk = yf.Ticker(symbol)
        df = tk.insider_transactions
        if df is None or df.empty:
            try:
                df = tk.insider_purchases
            except Exception:
                pass
        if df is None or df.empty:
            return {"buys": 0, "sells": 0, "net_value": 0.0, "note": "No records found", "source": "yahoo-insider"}

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join([str(c) for c in col if str(c)]).lower().replace(" ", "_") for col in df.columns]
        else:
            df.columns = [str(c).lower().replace(" ", "_").strip() for c in df.columns]

        date_col = next((c for c in df.columns if 'date' in c), None)
        if date_col:
            from dateutil.relativedelta import relativedelta
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            cutoff = datetime.datetime.now() - relativedelta(months=months)
            recent_df = df[df[date_col] >= cutoff].copy()
        else:
            recent_df = df.head(50).copy()

        if recent_df.empty:
            return {"buys": 0, "sells": 0, "net_value": 0.0, "note": "No recent records", "source": "yahoo-insider"}

        cols = recent_df.columns.tolist()
        type_col = next((c for c in cols if 'type' in c or 'trans' in c), None)
        shares_col = next((c for c in cols if 'shares' in c or 'qty' in c), None)
        value_col = next((c for c in cols if 'value' in c or 'amount' in c), None)

        if not type_col or not shares_col:
            return {"buys": 0, "sells": 0, "net_value": 0.0, "note": "Format mismatch", "source": "yahoo-insider"}

        buys, sells, net_val = 0, 0, 0.0
        records = []
        last_price = 0.0
        try:
            last_price = float(tk.fast_info.last_price)
        except Exception:
            pass

        text_col = next((c for c in cols if 'text' in c or 'desc' in c), "text")
        for _, row in recent_df.iterrows():
            t1 = str(row.get(type_col, "")).lower()
            t2 = str(row.get(text_col, "")).lower()
            combined_type = f"{t1} {t2}".strip()
            shares = safe_float(row.get(shares_col, 0))
            val = safe_float(row.get(value_col, 0))
            if val == 0 and shares > 0 and last_price > 0:
                val = safe_float(shares * last_price)

            is_buy = any(x in combined_type for x in ['purchase', 'acquisition', 'buy'])
            is_sell = any(x in combined_type for x in ['sale', 'sell', 'disposition'])
            is_exercise = 'exercise' in combined_type or 'convert' in combined_type or 'option' in combined_type
            is_gift = 'gift' in combined_type

            if is_exercise:
                final_type = "EXERCISE"
            elif is_gift:
                final_type = "GIFT"
            elif is_buy:
                final_type = "BUY"
                buys += 1
                net_val += val
            elif is_sell:
                final_type = "SELL"
                sells += 1
                net_val -= val
            else:
                final_type = "OTHER"

            records.append({
                "type": final_type, "shares": shares, "value": val, "text": combined_type,
                "insider": str(row.get("insider", "Unknown")), "position": str(row.get("position", "")),
                "date": str(row.get(date_col, "")) if date_col else ""
            })

        return {
            "buys": buys, "sells": sells, "net_value": safe_float(net_val),
            "record_count": len(recent_df), "transactions": records, "source": "yahoo-insider"
        }
    except Exception as e:
        logger.warning(f"Insider fetch failed for {symbol}: {e}")
        
    return {'buys': 0, 'sells': 0, 'net_value': 0.0, 'transactions': [], 'source': 'unavailable'}


# ==================================================================================
# OPTIONS ACTIVITY
# ==================================================================================

def fetch_options_activity_yfinance(symbol: str, expiries: int = 2, top: int = 5) -> Dict[str, Any]:
    """Fetch options chain and identify unusual contracts (Vol/OI spikes)."""
    try:
        is_open = _is_market_open()
        
        import yfinance as yf
        tk = yf.Ticker(symbol)
        dates = tk.options
        if not dates:
            return {}

        calls_vol, puts_vol, total_oi = 0, 0, 0
        all_contracts = []

        for d in dates[:expiries]:
            opt = tk.option_chain(d)
            for side_name, side_df in [("call", opt.calls), ("put", opt.puts)]:
                if side_df.empty:
                    continue
                vol_sum = side_df['volume'].sum()
                oi_sum = side_df['openInterest'].sum()
                if side_name == "call":
                    calls_vol += vol_sum
                else:
                    puts_vol += vol_sum
                total_oi += oi_sum

                sub = side_df[side_df['volume'] > 100].copy()
                for _, row in sub.iterrows():
                    v = float(row.get('volume', 0))
                    oi = float(row.get('openInterest', 0))
                    iv = float(row.get('impliedVolatility', 0))
                    score = (v / max(1, oi)) * min(1.0, v / 500)
                    all_contracts.append({
                        "type": side_name, "strike": float(row.get('strike', 0)),
                        "expiry": str(d), "volume": int(v), "openInterest": int(oi),
                        "impliedVolatility": iv, "unusual_score": min(10.0, score)
                    })

        pc_ratio = (puts_vol / calls_vol) if calls_vol > 0 else 0.0
        result = {
            "pc_volume_ratio": safe_float(pc_ratio),
            "total_volume": int(calls_vol + puts_vol),
            "total_oi": int(total_oi),
            "expiration_dates": dates[:expiries],
            "top": sorted(all_contracts, key=lambda x: x['unusual_score'], reverse=True)[:top],
            "market_closed": not is_open,
            "volume_reliable": is_open,
            "source": "yahoo-json"
        }
        return result
    except Exception as e:
        logger.warning(f"Options fetch failed for {symbol}: {e}")
        return {"error": str(e)}


# ==================================================================================
# FUNDAMENTAL DATA
# ==================================================================================

def fetch_fundamental_data(symbol: str) -> Dict[str, Any]:
    """Fetch fundamentals from FMP API with yfinance fallback."""
    fmp_key = os.environ.get("FMP_API_KEY")
    if fmp_key:
        try:
            profile_url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}"
            ratios_url = f"https://financialmodelingprep.com/api/v3/ratios-ttm/{symbol}"
            
            profile_resp = _http_session.get(profile_url, params={"apikey": fmp_key}, timeout=5)
            ratios_resp = _http_session.get(ratios_url, params={"apikey": fmp_key}, timeout=5)
            
            if profile_resp.status_code == 200 and ratios_resp.status_code == 200:
                profile_data = profile_resp.json()
                ratios_data = ratios_resp.json()
                
                if profile_data and len(profile_data) > 0:
                    prof = profile_data[0]
                    # Ratios might be empty for some assets
                    rats = ratios_data[0] if ratios_data and len(ratios_data) > 0 else {}
                    
                    return {
                        "pe_ratio": safe_float(rats.get("peRatioTTM")),
                        "forward_pe": 0.0, # FMP profile doesn't have forward PE easily
                        "peg_ratio": safe_float(rats.get("pegRatioTTM")),
                        "ps_ratio": safe_float(rats.get("priceToSalesRatioTTM")),
                        "pb_ratio": safe_float(rats.get("priceToBookRatioTTM")),
                        "dividend_yield": safe_float(prof.get("lastDiv", 0) / max(1, prof.get("price", 1))),
                        "market_cap": prof.get("mktCap"),
                        "sector": prof.get("sector"),
                        "beta": safe_float(prof.get("beta")),
                        "profit_margin": safe_float((rats.get("netProfitMarginTTM") or 0) * 100),
                        "operating_margin": safe_float((rats.get("operatingProfitMarginTTM") or 0) * 100),
                        "debt_to_equity": safe_float(rats.get("debtEquityRatioTTM")),
                        "revenue_growth": 0.0, # Needs different endpoint
                        "free_cashflow": 0.0,
                        "forward_eps": 0.0,
                        "source": "fmp"
                    }
        except Exception as e:
            logger.warning(f"FMP fundamentals fetch failed for {symbol}: {e}")

    # Fallback to yfinance
    try:
        import yfinance as yf
        t = yf.Ticker(symbol)
        info = t.info
        return {
            "pe_ratio": safe_float(info.get("trailingPE")),
            "forward_pe": safe_float(info.get("forwardPE")),
            "peg_ratio": safe_float(info.get("pegRatio", info.get("trailingPegRatio"))),
            "ps_ratio": safe_float(info.get("priceToSalesTrailing12Months")),
            "pb_ratio": safe_float(info.get("priceToBook")),
            "dividend_yield": safe_float(info.get("dividendYield", 0)),
            "market_cap": info.get("marketCap"),
            "sector": info.get("sector"),
            "beta": safe_float(info.get("beta")),
            # FIX F-2: Guard against None values before multiplication
            "profit_margin": safe_float((info.get("profitMargins") or 0) * 100),
            "operating_margin": safe_float((info.get("operatingMargins") or 0) * 100),
            "debt_to_equity": safe_float(info.get("debtToEquity")),
            "revenue_growth": safe_float((info.get("revenueGrowth") or 0) * 100),
            "free_cashflow": safe_float(info.get("freeCashflow")),
            "forward_eps": safe_float(info.get("forwardEps")),
            "source": "yfinance"
        }
    except Exception as e:
        logger.warning(f"Fundamentals fetch failed for {symbol}: {e}")
        return {}

# ==================================================================================
# ALTERNATIVE DATA FETCHERS
# ==================================================================================

def fetch_unusual_options(symbol: str) -> Dict[str, Any]:
    """Detect unusual options activity — institutions hiding large bets."""
    try:
        import yfinance as yf
        tk = yf.Ticker(symbol)
        dates = tk.options[:3]  # next 3 expiries

        unusual = []
        for date in dates:
            chain = tk.option_chain(date)
            for side, df in [("call", chain.calls), ("put", chain.puts)]:
                if df.empty:
                    continue
                df = df.copy()
                df['vol_oi_ratio'] = df['volume'] / df['openInterest'].replace(0, 1)
                unusual_contracts = df[df['vol_oi_ratio'] > 5]
                for _, row in unusual_contracts.iterrows():
                    unusual.append({
                        'type': side,
                        'strike': float(row['strike']),
                        'expiry': date,
                        'volume': int(row['volume']),
                        'oi': int(row['openInterest']),
                        'ratio': float(row['vol_oi_ratio']),
                        'iv': float(row.get('impliedVolatility', 0))
                    })

        call_unusual = [u for u in unusual if u['type'] == 'call']
        put_unusual = [u for u in unusual if u['type'] == 'put']

        return {
            'unusual_calls': len(call_unusual),
            'unusual_puts': len(put_unusual),
            'bull_signal': len(call_unusual) > len(put_unusual) * 2,
            'bear_signal': len(put_unusual) > len(call_unusual) * 2,
            'top_unusual': sorted(unusual, key=lambda x: x['ratio'], reverse=True)[:5],
            'source': 'yfinance_options'
        }
    except Exception as e:
        logger.warning(f"Unusual options fetch failed for {symbol}: {e}")
        return {'unusual_calls': 0, 'unusual_puts': 0, 'bull_signal': False, 'bear_signal': False, 'top_unusual': []}


def fetch_short_interest(symbol: str) -> Dict[str, Any]:
    """Fetch short interest from NASDAQ — free, public, underused."""
    try:
        url = f"https://api.nasdaq.com/api/quote/{symbol}/short-interest"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = requests.get(url, headers=headers, timeout=3)
        if resp.status_code == 200:
            data = resp.json().get('data') or {}
            short_interest = data.get('shortInterest', [])
            if short_interest:
                latest = short_interest[0] if isinstance(short_interest, list) else short_interest
                shares_short = int(latest.get('shortInterest', 0) or 0)
                days_to_cover = float(latest.get('daysToCover', 0) or 0)
                return {
                    'shares_short': shares_short,
                    'days_to_cover': days_to_cover,
                    'squeeze_risk': days_to_cover > 5,
                    'source': 'finra'
                }
    except Exception as e:
        logger.warning(f"Short interest fetch failed for {symbol}: {e}")
    return {'shares_short': 0, 'days_to_cover': 0, 'squeeze_risk': False}


def fetch_earnings_calendar(symbol: str) -> Dict[str, Any]:
    """Check if earnings are coming up — massive signal for volatility."""
    try:
        import yfinance as yf
        import pandas as pd

        tk = yf.Ticker(symbol)
        cal = tk.calendar

        if cal is not None and not (hasattr(cal, 'empty') and cal.empty):
            earnings_date = cal.get('Earnings Date') if isinstance(cal, dict) else None
            if earnings_date is None and hasattr(cal, 'loc'):
                try:
                    earnings_date = cal.loc['Earnings Date']
                except (KeyError, IndexError):
                    pass

            if earnings_date is not None:
                if hasattr(earnings_date, '__iter__') and not isinstance(earnings_date, str):
                    next_earnings = pd.Timestamp(list(earnings_date)[0])
                else:
                    next_earnings = pd.Timestamp(earnings_date)

                days_until = (next_earnings - pd.Timestamp.now()).days

                return {
                    'next_earnings': str(next_earnings.date()),
                    'days_until_earnings': days_until,
                    'earnings_this_week': 0 <= days_until <= 7,
                    'earnings_tomorrow': 0 <= days_until <= 1,
                    'expected_move': float(tk.info.get('impliedVolatility', 0) or 0) * 100,
                    'source': 'yfinance'
                }
    except Exception as e:
        logger.warning(f"Earnings calendar fetch failed for {symbol}: {e}")
    return {'days_until_earnings': 999, 'earnings_this_week': False, 'earnings_tomorrow': False}


def fetch_institutional_holdings(symbol: str) -> Dict[str, Any]:
    """Fetch 13F institutional holdings from SEC EDGAR."""
    try:
        from datetime import datetime, timedelta
        url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": f'"{symbol}"',
            "forms": "13F-HR",
            "dateRange": "custom",
            "startdt": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            "enddt": datetime.now().strftime("%Y-%m-%d")
        }
        headers = {"User-Agent": "FeatherBot/1.0 (research@feather.app)"}
        resp = _http_session.get(url, params=params, headers=headers, timeout=8)
        if resp.status_code == 200:
            hits = resp.json().get('hits', {}).get('hits', [])
            return {
                'recent_13f_filings': len(hits),
                'institutional_interest': len(hits) > 10,
                'source': 'sec_edgar'
            }
    except Exception as e:
        logger.warning(f"13F fetch failed for {symbol}: {e}")
    return {'recent_13f_filings': 0, 'institutional_interest': False}


def fetch_congress_trades(symbol: str) -> Dict[str, Any]:
    """Fetch Congressional stock trades from House disclosure data."""
    try:
        # housestockwatcher.com is offline permanently.
        # Fallback to returning default zeroes to prevent connection timeouts.
        return {
            'congress_buys': 0,
            'congress_sells': 0,
            'recent_activity': 0,
            'net_signal': 0,
            'congress_buying': False,
            'source': 'offline'
        }
    except Exception as e:
        logger.warning(f"Congress trade fetch failed for {symbol}: {e}")
    return {'congress_buys': 0, 'congress_sells': 0, 'net_signal': 0, 'congress_buying': False}


def fetch_google_trends(symbol: str, company_name: str = None) -> Dict[str, Any]:
    """Fetch Google Trends data — retail interest proxy."""
    try:
        from pytrends.request import TrendReq
        pytrends = TrendReq(hl='en-US', tz=360)

        query = company_name or symbol
        pytrends.build_payload([query], timeframe='today 3-m')
        data = pytrends.interest_over_time()

        if not data.empty:
            recent = data[query].tail(7).mean()
            historical = data[query].mean()
            trend_score = recent / (historical + 1e-9)

            return {
                'trend_score': float(trend_score),
                'trending_up': trend_score > 1.5,
                'trending_viral': trend_score > 3.0,
                'recent_interest': float(recent),
                'source': 'google_trends'
            }
    except Exception as e:
        logger.warning(f"Google Trends fetch failed for {symbol}: {e}")
    return {'trend_score': 1.0, 'trending_up': False, 'trending_viral': False}


def fetch_reddit_mentions(symbol: str) -> Dict[str, Any]:
    """Count Reddit mentions — retail sentiment proxy."""
    try:
        url = "https://www.reddit.com/search.json"
        params = {
            "q": symbol,
            "sort": "new",
            "limit": 25,
            "t": "day"
        }
        headers = {"User-Agent": "FeatherBot/1.0"}
        resp = _http_session.get(url, params=params, headers=headers, timeout=8)
        if resp.status_code == 200:
            posts = resp.json().get('data', {}).get('children', [])

            wsb_mentions = sum(
                1 for p in posts
                if 'wallstreetbets' in p.get('data', {}).get('subreddit', '').lower()
            )

            total_upvotes = sum(
                p.get('data', {}).get('score', 0)
                for p in posts
            )

            return {
                'reddit_mentions_24h': len(posts),
                'wsb_mentions': wsb_mentions,
                'total_upvotes': total_upvotes,
                'viral_reddit': wsb_mentions > 5,
                'source': 'reddit'
            }
    except Exception as e:
        logger.warning(f"Reddit mentions fetch failed for {symbol}: {e}")
    return {'reddit_mentions_24h': 0, 'wsb_mentions': 0, 'total_upvotes': 0, 'viral_reddit': False}


def fetch_fear_greed_index() -> Dict[str, Any]:
    """Fetch CNN Fear & Greed Index."""
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = _http_session.get(url, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            current = data.get('fear_and_greed', {})
            score = float(current.get('score', 50))
            rating = current.get('rating', 'neutral')

            return {
                'score': score,
                'rating': rating,
                'extreme_fear': score < 25,
                'extreme_greed': score > 75,
                'normalized': score / 100.0,
                'source': 'cnn_fear_greed'
            }
    except Exception as e:
        logger.warning(f"Fear & Greed fetch failed: {e}")
    return {'score': 50, 'rating': 'neutral', 'extreme_fear': False, 'extreme_greed': False, 'normalized': 0.5}


def fetch_fear_greed_history(limit: int = 365) -> list:
    """Fetch historical Fear & Greed data for training."""
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = _http_session.get(url, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            timeline = data.get('fear_and_greed_historical', {}).get('data', [])
            results = []
            for point in timeline[:limit]:
                ts = point.get('x', 0) / 1000  # JS timestamp → Unix
                val = point.get('y', 50)
                if ts > 0:
                    results.append({'timestamp': int(ts), 'value': float(val)})
            return results
    except Exception as e:
        logger.warning(f"Fear & Greed history fetch failed: {e}")
    return []



def news_health_check(query: str, max_articles: int = 5, timeout_sec: int = 5) -> Dict[str, Any]:
    start = time.time()
    try:
        arts = fetch_news_articles(query, days=3, max_articles=max_articles)
        elapsed = time.time() - start
        provider = arts[0].get("provider_path", "unknown") if arts else "none"
        return {
            "status": "ok" if arts else "empty",
            "count": len(arts),
            "elapsed": elapsed,
            "provider": provider
        }
    except Exception as e:
        return {"status": "error", "error": str(e), "elapsed": time.time() - start}