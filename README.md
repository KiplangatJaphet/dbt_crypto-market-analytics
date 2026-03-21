# Crypto Market Analytics Pipeline

A production-style ELT data pipeline that collects cryptocurrency market data from the Binance API, stores it in PostgreSQL, transforms it using dbt, and visualizes it in Grafana.

---

## Architecture

```
Binance API
↓
Python Data Ingestion (trades.py, klines.py, tickers.py)
↓
Raw Data Tables (PostgreSQL)
↓
dbt Staging Models
↓
dbt Intermediate Models
↓
dbt Mart Tables
↓
Grafana Dashboard
```

---

## Technology Stack

| Tool | Purpose |
|---|---|
| Python | API data extraction and loading |
| PostgreSQL | Data warehouse |
| dbt Core | Data transformation and modeling |
| Grafana | Data visualization and dashboards |
| Git & GitHub | Version control |

---

## Data Sources

Data is extracted from the **Binance Public REST API**:

| Endpoint | Description | Table |
|---|---|---|
| `/api/v3/trades` | Trade transactions | `raw_binance_trades` |
| `/api/v3/klines` | Candlestick (OHLCV) data | `raw_binance_klines` |
| `/api/v3/ticker/24hr` | 24hr market statistics | `raw_binance_tickers` |

Top 60 USDT trading pairs by volume are extracted including BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT and XRPUSDT.

---

## Project Structure

```
crypto_market/
├── crypto_ingestion/
│   ├── trades.py           # Extract & load trades
│   ├── klines.py           # Extract & load klines
│   ├── tickers.py          # Extract & load tickers
│   ├── main.py             # Runs full pipeline
│   └── schema.sql          # CREATE TABLE statements
├── models/
│   ├── staging/
│   │   ├── stg_raw_binance_trades.sql
│   │   ├── stg_raw_binance_klines.sql
│   │   └── stg_raw_binance_tickers.sql
│   ├── intermediate/
│   │   ├── int_price_metrics.sql
│   │   ├── int_symbol_volume.sql
│   │   └── int_price_volume_joined.sql
│   └── marts/
│       ├── fct_crypto_trades.sql
│       ├── fct_crypto_candles.sql
│       ├── dim_symbols.sql
│       ├── mart_daily_market_summary.sql
│       └── mart_rolling_metrics.sql
├── dbt_project.yml
└── README.md
```

---

## Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/KiplangatJaphet/dbt_crypto-market-analytics.git
cd dbt_crypto-market-analytics
```

### 2. Create virtual environment
```bash
python3 -m venv env
source env/bin/activate
```

### 3. Install dependencies
```bash
pip install requests pandas sqlalchemy psycopg2-binary python-dotenv
```

### 4. Configure environment variables
Create a `.env` file in `crypto_ingestion/`:
```
CONN=postgresql+psycopg2://user:password@host:port/dbname
```

### 5. Create raw tables in PostgreSQL
```bash
psql -U postgres -p 5433 -d analytics -f crypto_ingestion/schema.sql
```

### 6. Install dbt
```bash
pip install dbt-postgres
```

### 7. Configure dbt profile
Edit `~/.dbt/profiles.yml`:
```yaml
crypto_market:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433
      user: postgres
      password: your_password
      dbname: analytics
      schema: crypto
```

---

## Running the Pipeline

### Run ingestion once:
```bash
cd crypto_ingestion
python3 main.py
```

### Run continuously (every 10 minutes):
```bash
python3 main.py  # runs in infinite loop
```

### Run dbt transformations:
```bash
cd /root/crypto_market
dbt run
dbt test
```

---

## dbt Data Models

### Staging Layer
Cleans and standardizes raw data — renames columns, casts types, handles nulls and removes duplicates.

| Model | Source | Purpose |
|---|---|---|
| `stg_raw_binance_trades` | `raw_binance_trades` | Clean trade data |
| `stg_raw_binance_klines` | `raw_binance_klines` | Clean OHLCV candle data |
| `stg_raw_binance_tickers` | `raw_binance_tickers` | Clean ticker statistics |

### Intermediate Layer
Creates enriched and joined datasets for mart consumption.

| Model | Purpose |
|---|---|
| `int_price_metrics` | Price change, volatility and price range per candle |
| `int_symbol_volume` | Aggregated trading volumes per symbol per day |
| `int_price_volume_joined` | Joins price and volume metrics together |

### Mart Layer
Analytics-ready tables for reporting and dashboards.

| Model | Purpose |
|---|---|
| `fct_crypto_trades` | Trade facts with trade value |
| `fct_crypto_candles` | Candle facts with price metrics |
| `dim_symbols` | Symbol dimension with latest price attributes |
| `mart_daily_market_summary` | Daily price and volume summary per symbol |
| `mart_rolling_metrics` | 7-day rolling average price trend |

---

## Grafana Dashboard

The **Crypto Market Analytics** dashboard answers 5 key questions:

| Panel | Question | Model |
|---|---|---|
| Daily Average Price | What is the daily average price of each cryptocurrency? | `mart_daily_market_summary` |
| Highest Trading Volume | Which cryptocurrency has the highest trading volume? | `mart_daily_market_summary` |
| Hourly Price Volatility | What is the hourly price volatility for each coin? | `fct_crypto_candles` |
| Largest Price Movement | Which trading pair has the largest price movement? | `fct_crypto_candles` |
| 7-Day Rolling Average | What is the 7-day rolling average price trend? | `mart_rolling_metrics` |

---

## Key Analytics Questions Answered

1. **Daily average price** — tracked using `avg_price` in `mart_daily_market_summary`
2. **Highest trading volume** — ranked using `total_quote_volume` in USDT terms
3. **Hourly price volatility** — measured using `price_range` (high minus low) per candle
4. **Largest price movement** — identified using `price_change_percent` per candle
5. **7-day rolling average** — calculated using window function in `mart_rolling_metrics`

---

## Author

**Japhet Kiplagat**
GitHub: [KiplangatJaphet](https://github.com/KiplangatJaphet)
