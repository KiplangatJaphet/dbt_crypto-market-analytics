import time
from datetime import datetime, timezone
import requests
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

RATE_LIMIT_SLEEP = 0.3  # seconds between API calls


# ─────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("dbname"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port")
    )


# ─────────────────────────────────────────
# SYMBOLS — Top 30 USDT pairs by volume
# ─────────────────────────────────────────
def get_all_symbols():
    response = requests.get("https://api.binance.com/api/v3/ticker/24hr")
    response.raise_for_status()
    data = response.json()
    usdt_pairs = [s for s in data if s["symbol"].endswith("USDT")]
    sorted_pairs = sorted(usdt_pairs, key=lambda x: float(x["quoteVolume"]), reverse=True)
    return [s["symbol"] for s in sorted_pairs[:30]]


# ─────────────────────────────────────────
# 1. EXTRACT & LOAD TRADES
# ─────────────────────────────────────────
def extract_trades(symbols):
    """Fetch last 500 trades for each symbol from Binance."""
    all_trades = []
    ingested_at = datetime.now(timezone.utc)

    for symbol in symbols:
        url = "https://api.binance.com/api/v3/trades"
        params = {"symbol": symbol, "limit": 500}
        response = requests.get(url, params=params)
        response.raise_for_status()
        trades = response.json()

        for trade in trades:
            all_trades.append((
                trade["id"],                        # trade_id
                symbol,                             # symbol
                float(trade["price"]),              # price  — cast to float
                float(trade["qty"]),                # qty    — cast to float
                float(trade["quoteQty"]),           # quote_qty — cast to float
                trade["time"],                      # trade_time (ms epoch)
                trade["isBuyerMaker"],              # is_buyer_maker
                trade["isBestMatch"],               # is_best_match
                ingested_at                         # ingested_at
            ))

        time.sleep(RATE_LIMIT_SLEEP)               # respect Binance rate limits
        print(f"  [trades] Extracted {len(trades)} trades for {symbol}")

    return all_trades


def load_trades(cur, trades):
    """Insert trades into crypto.raw_binance_trades."""
    sql = """
        INSERT INTO crypto.raw_binance_trades
            (trade_id, symbol, price, qty, quote_qty,
             trade_time, is_buyer_maker, is_best_match, ingested_at)
        VALUES (%s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), %s, %s, %s)
        ON CONFLICT DO NOTHING
    """
    cur.executemany(sql, trades)
    print(f"[trades] Loaded {len(trades)} rows into crypto.raw_binance_trades")


# ─────────────────────────────────────────
# 2. EXTRACT & LOAD KLINES (CANDLES)
# ─────────────────────────────────────────
def extract_klines(symbols, interval="5m", limit=500):
    """Fetch OHLCV klines for each symbol from Binance."""
    all_klines = []
    ingested_at = datetime.now(timezone.utc)

    for symbol in symbols:
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        response = requests.get(url, params=params)
        response.raise_for_status()
        klines = response.json()

        for k in klines:
            all_klines.append((
                symbol,
                k[0],           # open_time (ms epoch)
                float(k[1]),    # open_price
                float(k[2]),    # high_price
                float(k[3]),    # low_price
                float(k[4]),    # close_price
                float(k[5]),    # volume
                k[6],           # close_time (ms epoch)
                float(k[7]),    # quote_asset_volume
                int(k[8]),      # number_of_trades
                float(k[9]),    # taker_buy_base_asset_volume
                float(k[10]),   # taker_buy_quote_asset_volume
                ingested_at     # ingested_at
            ))

        time.sleep(RATE_LIMIT_SLEEP)
        print(f"  [klines] Extracted {len(klines)} candles for {symbol}")

    return all_klines


def load_klines(cur, klines):
    """Insert klines into crypto.raw_binance_klines."""
    sql = """
        INSERT INTO crypto.raw_binance_klines
            (symbol, open_time, open_price, high_price, low_price, close_price,
             volume, close_time, quote_asset_volume, number_of_trades,
             taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ingested_at)
        VALUES (
            %s,
            to_timestamp(%s / 1000.0),
            %s, %s, %s, %s, %s,
            to_timestamp(%s / 1000.0),
            %s, %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING
    """
    cur.executemany(sql, klines)
    print(f"[klines] Loaded {len(klines)} rows into crypto.raw_binance_klines")


# ─────────────────────────────────────────
# 3. EXTRACT & LOAD TICKERS
# ─────────────────────────────────────────
def extract_tickers(symbols):
    """Fetch 24hr ticker data for each symbol from Binance."""
    all_tickers = []
    ingested_at = datetime.now(timezone.utc)

    for symbol in symbols:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        params = {"symbol": symbol}
        response = requests.get(url, params=params)
        response.raise_for_status()
        t = response.json()

        all_tickers.append((
            t["symbol"],
            float(t["priceChange"]),
            float(t["priceChangePercent"]),
            float(t["weightedAvgPrice"]),
            float(t["prevClosePrice"]),
            float(t["lastPrice"]),
            float(t["lastQty"]),
            float(t["bidPrice"]),
            float(t["bidQty"]),
            float(t["askPrice"]),
            float(t["askQty"]),
            float(t["openPrice"]),
            float(t["highPrice"]),
            float(t["lowPrice"]),
            float(t["volume"]),
            float(t["quoteVolume"]),
            t["openTime"],
            t["closeTime"],
            t["firstId"],
            t["lastId"],
            t["count"],
            ingested_at             # ingested_at
        ))

        time.sleep(RATE_LIMIT_SLEEP)
        print(f"  [tickers] Extracted ticker for {symbol}")

    return all_tickers


def load_tickers(cur, tickers):
    """Insert tickers into crypto.raw_binance_tickers."""
    sql = """
        INSERT INTO crypto.raw_binance_tickers
            (symbol, price_change, price_change_percent, weighted_avg_price,
             prev_close_price, last_price, last_qty, bid_price, bid_qty,
             ask_price, ask_qty, open_price, high_price, low_price, volume,
             quote_volume, open_time, close_time, first_id, last_id, count, ingested_at)
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            to_timestamp(%s / 1000.0),
            to_timestamp(%s / 1000.0),
            %s, %s, %s, %s
        )
    """
    cur.executemany(sql, tickers)
    print(f"[tickers] Loaded {len(tickers)} rows into crypto.raw_binance_tickers")


# ─────────────────────────────────────────
# MAIN ETL PIPELINE
# ─────────────────────────────────────────
def run_etl():
    print(f"\n{'='*50}")
    print(f"Binance ETL started at {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*50}\n")

    # Step 1 — Get symbols
    print("Fetching top 30 USDT symbols by volume...")
    symbols = get_all_symbols()
    print(f"Symbols: {symbols}\n")

    # Step 2 — Extract all data
    print("--- EXTRACTION ---")
    trades  = extract_trades(symbols)
    klines  = extract_klines(symbols)
    tickers = extract_tickers(symbols)

    # Step 3 — Load all data in one transaction
    print("\n--- LOADING ---")
    conn = get_connection()
    try:
        with conn:
            cur = conn.cursor()
            load_trades(cur, trades)
            load_klines(cur, klines)
            load_tickers(cur, tickers)
            cur.close()
        print("\nAll data committed successfully.")
    except Exception as e:
        print(f"Error during load — transaction rolled back: {e}")
        raise
    finally:
        conn.close()

    print(f"\nETL completed at {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    run_etl()