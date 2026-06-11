SELECT
    symbol,
    DATE_TRUNC('day', minute_bucket)    AS trade_date,
    SUM(vwap * total_volume)
        / NULLIF(SUM(total_volume), 0)  AS daily_vwap,
    SUM(total_volume)                   AS daily_volume,
    MAX(high_price)                     AS daily_high,
    MIN(low_price)                      AS daily_low,
    MAX(high_price) - MIN(low_price)    AS daily_range,
    COUNT(*)                            AS total_minutes_traded
FROM {{ ref('silver_trades') }}
GROUP BY symbol, DATE_TRUNC('day', minute_bucket)
ORDER BY trade_date, symbol