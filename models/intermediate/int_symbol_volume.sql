SELECT
    symbol,
    DATE(trade_time) AS trade_date,
    SUM(quantity) AS total_quantity_traded,
    SUM(quote_quantity) AS total_quote_volume,
    SUM(quantity * price) AS total_trade_value,
    COUNT(*) AS number_of_trades,
    AVG(quantity * price) AS avg_trade_value

FROM {{ ref('stg_raw_binance_trades') }}

GROUP BY symbol, DATE(trade_time)