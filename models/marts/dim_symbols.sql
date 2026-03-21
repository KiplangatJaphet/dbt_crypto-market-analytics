WITH symbols AS (

SELECT DISTINCT symbol FROM {{ ref('int_price_metrics') }}

UNION

SELECT DISTINCT symbol FROM {{ ref('int_symbol_volume') }}

),

enriched AS (

SELECT
    s.symbol,
    t.last_price,                               -- current price
    t.high_price AS day_high,                   -- day high
    t.low_price AS day_low,                     -- day low
    t.price_change,                             -- price change
    t.price_change_pct,                         -- price change percent
    t.volume,                                   -- trading volume
    t.open_time,                                -- window open
    t.close_time                                -- window close

FROM symbols s
LEFT JOIN {{ ref('stg_raw_binance_tickers') }} t
    ON s.symbol = t.symbol

)

SELECT *
FROM enriched