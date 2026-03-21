WITH cleaned_tickers AS (

SELECT
    symbol,

    "priceChange"::numeric AS price_change,                 -- renamed from camelCase + cast
    "priceChangePct"::numeric AS price_change_pct,          -- renamed from camelCase + cast
    "weightedAvgPrice"::numeric AS weighted_avg_price,      -- renamed from camelCase + cast
    "prevClosePrice"::numeric AS prev_close_price,          -- renamed from camelCase + cast
    "lastPrice"::numeric AS last_price,                     -- renamed from camelCase + cast
    "lastQty"::numeric AS last_qty,                         -- renamed from camelCase + cast
    "bidPrice"::numeric AS bid_price,                       -- renamed from camelCase + cast
    "bidQty"::numeric AS bid_qty,                           -- renamed from camelCase + cast
    "askPrice"::numeric AS ask_price,                       -- renamed from camelCase + cast
    "askQty"::numeric AS ask_qty,                           -- renamed from camelCase + cast
    "openPrice"::numeric AS open_price,                     -- renamed from camelCase + cast
    "highPrice"::numeric AS high_price,                     -- renamed from camelCase + cast
    "lowPrice"::numeric AS low_price,                       -- renamed from camelCase + cast
    "volume"::numeric AS volume,                            -- cast datatype
    "quoteVolume"::numeric AS quote_volume,                 -- renamed from camelCase + cast
    "count" AS trade_count,                                 -- renamed for clarity

    to_timestamp("openTime" / 1000.0) AS open_time,        -- cast bigint ms to timestamp
    to_timestamp("closeTime" / 1000.0) AS close_time       -- cast bigint ms to timestamp

FROM crypto.raw_binance_tickers

WHERE symbol IS NOT NULL                                    -- handle null values

),

deduplicated AS (

SELECT *,

ROW_NUMBER() OVER (
    PARTITION BY symbol, open_time                          -- identify duplicates
    ORDER BY close_time DESC
) AS row_num

FROM cleaned_tickers

)

SELECT
    symbol,
    price_change,
    price_change_pct,
    weighted_avg_price,
    prev_close_price,
    last_price,
    last_qty,
    bid_price,
    bid_qty,
    ask_price,
    ask_qty,
    open_price,
    high_price,
    low_price,
    volume,
    quote_volume,
    trade_count,
    open_time,
    close_time
FROM deduplicated

WHERE row_num = 1                                          -- remove duplicates