-- ============================================================
-- Realtime Streaming Pipeline: Crypto Alerts
-- ============================================================

-- 1. Create Source Table from Kafka (Binance Trades)
CREATE TABLE crypto_trades_alerts (
    symbol            STRING,
    price             DOUBLE,
    quantity          DOUBLE,
    trade_time        BIGINT,
    is_buyer_maker    BOOLEAN,
    event_time AS TO_TIMESTAMP_LTZ(trade_time, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'crypto-trades',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-crypto-alerts',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'latest-offset'
);

-- 2. Create Sink Table to Kafka (Alerts)
CREATE TABLE crypto_alerts (
    alert_time    TIMESTAMP(3),
    symbol        STRING,
    alert_type    STRING,
    message       STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'crypto-alerts',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- 3. Emit Alerts using STATEMENT SET
BEGIN STATEMENT SET;

-- Whale Alert (> $50,000 in a single trade to make it trigger more often for demo)
INSERT INTO crypto_alerts
SELECT 
    event_time AS alert_time,
    symbol,
    'WHALE_ALERT' AS alert_type,
    'Whale spotted! ' || CAST(quantity AS STRING) || ' ' || symbol || ' traded at $' || CAST(price AS STRING) AS message
FROM crypto_trades_alerts
WHERE (price * quantity) > 50000;

-- CEP Alert: 3 Consecutive Price Drops
INSERT INTO crypto_alerts
SELECT 
    alert_time,
    symbol,
    'DOWNTREND' AS alert_type,
    'Price dropped 3 times consecutively starting from $' || CAST(start_price AS STRING) AS message
FROM crypto_trades_alerts
MATCH_RECOGNIZE (
    PARTITION BY symbol
    ORDER BY event_time
    MEASURES
        A.price AS start_price,
        C.price AS end_price,
        C.event_time AS alert_time
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B C)
    WITHIN INTERVAL '30' SECOND
    DEFINE
        B AS B.price < A.price,
        C AS C.price < B.price
);

END;
