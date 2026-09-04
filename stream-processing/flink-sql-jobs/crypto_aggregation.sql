-- ============================================================
-- Realtime Streaming Pipeline: Crypto Trades Aggregation
-- ============================================================

-- 1. Create Source Table from Kafka (Binance Trades)
CREATE TABLE crypto_trades (
    symbol            STRING,
    price             DOUBLE,
    quantity          DOUBLE,
    trade_time        BIGINT,
    is_buyer_maker    BOOLEAN,
    -- Convert epoch timestamp in milliseconds to TIMESTAMP(3)
    event_time AS TO_TIMESTAMP_LTZ(trade_time, 3),
    -- Define Watermark to handle out-of-order events (allow 5 seconds lateness)
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'crypto-trades',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-crypto-aggregator',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'latest-offset'
);


-- 2. Create Sink Table to ClickHouse via JDBC (Aggregated by Minute)
CREATE TABLE crypto_revenue_by_minute (
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    symbol          STRING,
    total_volume    DOUBLE,
    trade_count     BIGINT,
    avg_price       DOUBLE,
    PRIMARY KEY (symbol, window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/cryptodb',
    'table-name' = 'crypto_by_minute',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '5s'
);


-- 3. Tumbling Window Aggregation (5 Seconds) -> Insert into ClickHouse
INSERT INTO crypto_revenue_by_minute
SELECT
    window_start,
    window_end,
    symbol,
    SUM(price * quantity)                  AS total_volume,
    COUNT(*)                               AS trade_count,
    ROUND(SUM(price * quantity) / NULLIF(SUM(quantity), 0), 2)  AS avg_price
FROM TABLE(
    TUMBLE(TABLE crypto_trades, DESCRIPTOR(event_time), INTERVAL '5' SECOND)
)
GROUP BY window_start, window_end, symbol;
