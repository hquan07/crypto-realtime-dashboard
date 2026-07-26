-- ============================================================
-- Realtime Streaming Pipeline: Crypto Technical Indicators
-- ============================================================

-- 1. Create Source Table from Kafka (Binance Trades)
CREATE TABLE crypto_trades_ind (
    symbol            STRING,
    price             DOUBLE,
    quantity          DOUBLE,
    trade_time        BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(trade_time, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'crypto-trades',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-crypto-indicators',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'latest-offset'
);

-- 2. Create Sink Table to Kafka (Indicators)
CREATE TABLE crypto_indicators (
    window_end    TIMESTAMP(3),
    symbol        STRING,
    sma_1m        DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'crypto-indicators',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- 3. Calculate 1-Minute SMA using HOP Window (Slide every 5 seconds)
INSERT INTO crypto_indicators
SELECT 
    window_end,
    symbol,
    AVG(price) AS sma_1m
FROM TABLE(
    HOP(TABLE crypto_trades_ind, DESCRIPTOR(event_time), INTERVAL '5' SECOND, INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, symbol;
