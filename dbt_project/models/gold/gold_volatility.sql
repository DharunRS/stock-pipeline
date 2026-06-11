WITH base AS (
    SELECT
        symbol,
        minute_bucket,
        vwap,
        total_volume,
        AVG(vwap) OVER (
            PARTITION BY symbol
            ORDER BY minute_bucket
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30min_avg,
        STDDEV(vwap) OVER (
            PARTITION BY symbol
            ORDER BY minute_bucket
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30min_stddev
    FROM {{ ref('silver_trades') }}
)

SELECT
    symbol,
    minute_bucket,
    vwap,
    rolling_30min_avg,
    rolling_30min_stddev,
    CASE
        WHEN rolling_30min_stddev > 0
        THEN (vwap - rolling_30min_avg) / rolling_30min_stddev
        ELSE 0
    END AS z_score,
    CASE
        WHEN rolling_30min_stddev / NULLIF(rolling_30min_avg, 0) > 0.02
        THEN TRUE ELSE FALSE
    END AS is_high_volatility
FROM base