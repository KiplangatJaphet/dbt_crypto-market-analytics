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

def extract_tickers():
    all_tickers = []

    for symbol in SYMBOLS:
        response = requests.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": symbol}
        )

        if response.status_code == 200:
            ticker = response.json()
            all_tickers.append({
                "symbol":             ticker["symbol"],
                "priceChange":        ticker["priceChange"],
                "priceChangePct":     ticker["priceChangePercent"],
                "weightedAvgPrice":   ticker["weightedAvgPrice"],
                "prevClosePrice":     ticker["prevClosePrice"],
                "lastPrice":          ticker["lastPrice"],
                "lastQty":            ticker["lastQty"],
                "bidPrice":           ticker["bidPrice"],
                "bidQty":             ticker["bidQty"],
                "askPrice":           ticker["askPrice"],
                "askQty":             ticker["askQty"],
                "openPrice":          ticker["openPrice"],
                "highPrice":          ticker["highPrice"],
                "lowPrice":           ticker["lowPrice"],
                "volume":             ticker["volume"],
                "quoteVolume":        ticker["quoteVolume"],
                "openTime":           ticker["openTime"],
                "closeTime":          ticker["closeTime"],
                "firstId":            ticker["firstId"],
                "lastId":             ticker["lastId"],
                "count":              ticker["count"]
            })
        else:
            print(f"Failed to fetch ticker for {symbol}: {response.status_code}")

        time.sleep(0.3)

    df = pd.DataFrame(all_tickers)
    print(f"Extracted {len(df)} tickers")
    return df

def load_tickers(df):
    df.to_sql("raw_binance_tickers", engine, schema="crypto", if_exists="append", index=False)
    print(f"Loaded {len(df)} rows into crypto.raw_binance_tickers")

df = extract_tickers()
load_tickers(df)

