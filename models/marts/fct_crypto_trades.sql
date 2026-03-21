WITH trades AS (

SELECT
    symbol,
    trade_time,
    price,
    quantity,
    quote_quantity

FROM {{ ref('stg_raw_binance_trades') }}

),

fact_trades AS (

SELECT
    symbol,
    trade_time,
    price,
    quantity,
    quote_quantity,

    -- Correct trade value
    quantity * price AS trade_value

FROM trades

)

SELECT *
FROM fact_trades