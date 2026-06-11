SELECT
    symbol,
    DATE_TRUNC('minute', event_time)    AS minute_bucket,
    SUM(price * volume) / SUM(volume)   AS vwap,
    SUM(volume)                         AS total_volume,
    AVG(price)                          AS avg_price,
    MAX(price)                          AS high_price,
    MIN(price)                          AS low_price,
    MAX(price) - MIN(price)             AS price_range,
    STDDEV(price)                       AS price_stddev,
    COUNT(*)                            AS tick_count
FROM {{ ref('bronze_trades') }}
GROUP BY symbol, DATE_TRUNC('minute', event_time)