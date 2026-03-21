WITH volume AS (

SELECT *
FROM {{ ref('int_symbol_volume') }}

),

price AS (

SELECT *
FROM {{ ref('int_price_metrics') }}          

),

joined AS (

SELECT
    p.symbol,
    p.open_time,
    p.close_time,

    -- Price metrics
    p.open_price,
    p.close_price,
    p.high_price,
    p.low_price,
    p.price_change,
    p.price_change_percent,
    p.price_range,

    -- Volume metrics
    v.trade_date,
    v.total_quantity_traded,
    v.total_quote_volume,
    v.total_trade_value,
    v.number_of_trades

FROM price p

INNER JOIN volume v
    ON p.symbol = v.symbol
    AND DATE(p.open_time) = v.trade_date

)

SELECT *
FROM joined


