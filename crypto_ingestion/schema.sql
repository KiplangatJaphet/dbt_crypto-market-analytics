-- Raw Binance Trades
CREATE TABLE crypto.raw_binance_trades (
    id              BIGINT,
    symbol          VARCHAR(20),
    price           NUMERIC,
    qty             NUMERIC,
    "quoteQty"      NUMERIC,
    time            BIGINT,
    "isBuyerMaker"  BOOLEAN,
    "isBestMatch"   BOOLEAN
);

-- Raw Binance Klines
CREATE TABLE crypto.raw_binance_klines (
    symbol                      VARCHAR(20),
    "openTime"                  BIGINT,
    "open"                      NUMERIC,
    "high"                      NUMERIC,
    "low"                       NUMERIC,
    "close"                     NUMERIC,
    "volume"                    NUMERIC,
    "closeTime"                 BIGINT,
    "quoteAssetVolume"          NUMERIC,
    "numberOfTrades"            INTEGER,
    "takerBuyBaseAssetVolume"   NUMERIC,
    "takerBuyQuoteAssetVolume"  NUMERIC
);

-- Raw Binance Tickers
CREATE TABLE crypto.raw_binance_tickers (
    symbol              VARCHAR(20),
    "priceChange"       NUMERIC,
    "priceChangePct"    NUMERIC,
    "weightedAvgPrice"  NUMERIC,
    "prevClosePrice"    NUMERIC,
    "lastPrice"         NUMERIC,
    "lastQty"           NUMERIC,
    "bidPrice"          NUMERIC,
    "bidQty"            NUMERIC,
    "askPrice"          NUMERIC,
    "askQty"            NUMERIC,
    "openPrice"         NUMERIC,
    "highPrice"         NUMERIC,
    "lowPrice"          NUMERIC,
    "volume"            NUMERIC,
    "quoteVolume"       NUMERIC,
    "openTime"          BIGINT,
    "closeTime"         BIGINT,
    "firstId"           BIGINT,
    "lastId"            BIGINT,
    "count"             INTEGER
);
