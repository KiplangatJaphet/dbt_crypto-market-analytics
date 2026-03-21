
WITH joined AS (

SELECT *
FROM {{ ref('int_price_volume_joined') }}

),

daily AS (

SELECT
    symbol,
    DATE(open_time) AS market_date,

    -- Price metrics
    AVG(close_price) AS avg_price,
    MAX(high_price) AS highest_price,
    MIN(low_price) AS lowest_price,

    AVG(price_change) AS avg_price_change,
    AVG(price_change_percent) AS avg_price_change_pct,
    AVG(price_range) AS avg_price_range,

    -- Volume metrics
    SUM(total_quantity_traded) AS total_volume,
    SUM(total_quote_volume) AS total_quote_volume,
    SUM(total_trade_value) AS total_trade_value,
    SUM(number_of_trades) AS total_trades

FROM joined

GROUP BY symbol, DATE(open_time)

),

with_rolling AS (

SELECT
    *,

    -- 7-day rolling average price
    AVG(avg_price) OVER (
        PARTITION BY symbol
        ORDER BY market_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg_price                 -- answers 7-day rolling average question

FROM daily

)

SELECT *
FROM with_rolling
