SELECT
    symbol,
    price::DOUBLE       AS price,
    volume::BIGINT      AS volume,
    bid::DOUBLE         AS bid,
    ask::DOUBLE         AS ask,
    (ask - bid)::DOUBLE AS spread,
    timestamp::TIMESTAMP AS event_time
FROM {{ source('delta', 'bronze_trades') }}
WHERE price > 0
  AND volume > 0
  AND symbol IS NOT NULL