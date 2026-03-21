WITH base AS (

SELECT *
FROM {{ ref('mart_daily_market_summary') }}

)

SELECT
    symbol,
    market_date,
    avg_price,

    -- 7-day rolling average
    AVG(avg_price) OVER (
        PARTITION BY symbol
        ORDER BY market_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_price

FROM base