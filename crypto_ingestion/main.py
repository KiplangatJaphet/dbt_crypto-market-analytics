import time
import subprocess

while True:
    print("Starting ingestion...")

    subprocess.run(["python3", "trades.py"])
    subprocess.run(["python3", "klines.py"])
    subprocess.run(["python3", "tickers.py"])

    print("Running dbt ingestionsp...")
    subprocess.run(["dbt", "run"], cwd="/root/crypto_market")

    print("Pipeline run complete. Waiting 10 minutes for next run...")
    time.sleep(600)