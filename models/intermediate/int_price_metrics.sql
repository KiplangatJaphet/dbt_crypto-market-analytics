WITH klines AS (

SELECT
    symbol,
    open_time,
    close_time,
    open_price,
    close_price,
    high_price,
    low_price,
    volume,
    quote_asset_volume,
    number_of_trades

FROM {{ ref('stg_raw_binance_klines') }}

),

price_metrics AS (

SELECT
    symbol,
    open_time,
    close_time,

    open_price,
    close_price,
    high_price,
    low_price,
    volume,
    quote_asset_volume,
    number_of_trades,

    -- Calculate price movement
    close_price - open_price AS price_change,

    -- Calculate percentage price change
    ((close_price - open_price) / open_price) * 100 AS price_change_percent,

    -- Volatility (price spread)
    high_price - low_price AS price_range

FROM klines

)

SELECT *
FROM price_metrics