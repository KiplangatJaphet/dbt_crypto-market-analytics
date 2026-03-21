WITH cleaned_klines AS (

SELECT
    symbol,

    to_timestamp("openTime" / 1000.0) AS open_time,        -- cast bigint ms to timestamp
    to_timestamp("closeTime" / 1000.0) AS close_time,      -- cast bigint ms to timestamp

    "open"::numeric AS open_price,                         -- renamed from camelCase + cast
    "high"::numeric AS high_price,                         -- renamed from camelCase + cast
    "low"::numeric AS low_price,                           -- renamed from camelCase + cast
    "close"::numeric AS close_price,                       -- renamed from camelCase + cast
    "volume"::numeric AS volume,                           -- cast datatype
    "quoteAssetVolume"::numeric AS quote_asset_volume,     -- renamed from camelCase + cast
    "numberOfTrades"::integer AS number_of_trades,         -- renamed from camelCase + cast
    "takerBuyBaseAssetVolume"::numeric AS taker_buy_base_volume,   -- renamed from camelCase
    "takerBuyQuoteAssetVolume"::numeric AS taker_buy_quote_volume  -- renamed from camelCase

FROM crypto.raw_binance_klines

WHERE symbol IS NOT NULL                                    -- handle null values

),

deduplicated AS (

SELECT *,

ROW_NUMBER() OVER (
    PARTITION BY symbol, open_time                         -- identify duplicates
    ORDER BY close_time DESC
) AS row_num                                               -- remove duplicates

FROM cleaned_klines

)

SELECT
    symbol,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    quote_asset_volume,
    number_of_trades,
    taker_buy_base_volume,
    taker_buy_quote_volume
FROM deduplicated

WHERE row_num = 1                                          -- keep only one record (deduplicated)