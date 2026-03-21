import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
print("CONN =", os.getenv("CONN"))
engine = create_engine(os.getenv("CONN"))

def get_all_symbols():
    response = requests.get("https://api.binance.com/api/v3/ticker/24hr")
    data = response.json()
    usdt_pairs = [item for item in data if item["symbol"].endswith("USDT")]
    sorted_pairs = sorted(usdt_pairs, key=lambda x: float(x["quoteVolume"]), reverse=True)
    return [s["symbol"] for s in sorted_pairs[:60]]

SYMBOLS = get_all_symbols()
print(SYMBOLS)

def extract_klines():
    all_klines = []

    for symbol in SYMBOLS:
        response = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol, "interval": "5m", "limit": 60}
        )

        if response.status_code == 200:
            klines = response.json()
            for k in klines:
                all_klines.append({
                    "symbol":                        symbol,
                    "openTime":                      k[0],
                    "open":                          k[1],
                    "high":                          k[2],
                    "low":                           k[3],
                    "close":                         k[4],
                    "volume":                        k[5],
                    "closeTime":                     k[6],
                    "quoteAssetVolume":              k[7],
                    "numberOfTrades":                k[8],
                    "takerBuyBaseAssetVolume":       k[9],
                    "takerBuyQuoteAssetVolume":      k[10]
                })
        else:
            print(f"Failed to fetch klines for {symbol}: {response.status_code}")

        time.sleep(0.3)

    df = pd.DataFrame(all_klines)
    print(f"Extracted {len(df)} klines")
    return df

def load_klines(df):
    df.to_sql("raw_binance_klines", engine, schema="crypto", if_exists="append", index=False)
    print(f"Loaded {len(df)} rows into crypto.raw_binance_klines")

df = extract_klines()
load_klines(df)
