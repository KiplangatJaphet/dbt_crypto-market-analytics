import requests
import psycopg2
import pandas as pd
import time
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from sqlalchemy import  create_engine   


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

def extract_trades():
    all_trades = []

    for symbol in SYMBOLS:
        response = requests.get(
            "https://api.binance.com/api/v3/trades",
            params={"symbol": symbol, "limit": 60}
        )

        if response.status_code == 200:
            trades = response.json()
            for trade in trades:
                trade["symbol"] = symbol
            all_trades.extend(trades)
        else:
            print(f"Failed to fetch trades for {symbol}: {response.status_code}")

        time.sleep(0.3)

    df = pd.DataFrame(all_trades)
    print(f"Extracted {len(df)} trades")
    return df

def load_trades(df):
    df.to_sql("raw_binance_trades", engine, schema="crypto", if_exists="append", index=False)
    print(f"Loaded {len(df)} rows into crypto.raw_binance_trades")

df = extract_trades()
load_trades(df)

