WITH cleaned_trades AS (

SELECT
    id AS trade_id,                                         -- rename column
    symbol,
    price::numeric AS price,                                -- cast datatype
    qty::numeric AS quantity,
    "quoteQty"::numeric AS quote_quantity,                  -- renamed from quote_qty
    to_timestamp(time / 1000.0) AS trade_time,             -- cast bigint ms to timestamp
    "isBuyerMaker" AS is_buyer_maker,                      -- renamed from camelCase
    "isBestMatch" AS is_best_match                         -- renamed from camelCase
FROM crypto.raw_binance_trades
WHERE symbol IS NOT NULL                                    -- handle null values

),

deduplicated AS (

SELECT *,
ROW_NUMBER() OVER (
    PARTITION BY trade_id
    ORDER BY trade_time DESC
) AS row_num
FROM cleaned_trades

)

SELECT
    trade_id,
    symbol,
    price,
    quantity,
    quote_quantity,
    trade_time,
    is_buyer_maker,
    is_best_match
FROM deduplicated
WHERE row_num = 1