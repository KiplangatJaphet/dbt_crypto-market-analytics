SELECT
    symbol,
    open_time,
    close_time,
    open_price,
    close_price,
    high_price,
    low_price,
    volume,
    price_change,
    price_change_percent,
    price_range

FROM {{ ref('int_price_metrics') }}