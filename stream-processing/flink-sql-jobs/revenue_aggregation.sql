-- ============================================================
-- Realtime Streaming Pipeline — Flink SQL: Revenue Aggregation
-- ============================================================
-- Reads purchase events from Kafka, applies tumbling window
-- aggregation, and writes results to Elasticsearch.
-- ============================================================

SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '3';
SET 'restart-strategy.fixed-delay.delay' = '10s';

-- --------------------------------------------------------
-- Source: Kafka purchase-events topic
-- Timestamp format: yyyy-MM-ddTHH:mm:ss.SSS (no timezone)
-- --------------------------------------------------------
CREATE TABLE purchase_events (
    event_id       STRING,
    event_type     STRING,
    user_id        STRING,
    product_id     STRING,
    product_name   STRING,
    category       STRING,
    quantity        INT,
    unit_price     DOUBLE,
    total_amount   DOUBLE,
    currency       STRING,
    `timestamp`    STRING,
    session_id     STRING,
    event_time AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                        = 'kafka',
    'topic'                            = 'purchase-events',
    'properties.bootstrap.servers'     = 'kafka:9092',
    'properties.group.id'              = 'flink-es-aggregation',
    'format'                           = 'json',
    'json.fail-on-missing-field'       = 'false',
    'json.ignore-parse-errors'         = 'true',
    'scan.startup.mode'                = 'latest-offset'
);

-- --------------------------------------------------------
-- Sink: Elasticsearch — revenue by minute
-- --------------------------------------------------------
CREATE TABLE revenue_per_minute (
    window_start   TIMESTAMP(3),
    window_end     TIMESTAMP(3),
    category       STRING,
    total_revenue  DOUBLE,
    event_count    BIGINT,
    avg_order_value DOUBLE,
    PRIMARY KEY (category, window_start) NOT ENFORCED
) WITH (
    'connector'   = 'elasticsearch-7',
    'hosts'       = 'http://elasticsearch:9200',
    'index'       = 'revenue-by-minute'
);

-- --------------------------------------------------------
-- Sink: Elasticsearch — revenue by hour
-- --------------------------------------------------------
CREATE TABLE revenue_per_hour (
    window_start   TIMESTAMP(3),
    window_end     TIMESTAMP(3),
    category       STRING,
    total_revenue  DOUBLE,
    event_count    BIGINT,
    avg_order_value DOUBLE,
    PRIMARY KEY (category, window_start) NOT ENFORCED
) WITH (
    'connector'   = 'elasticsearch-7',
    'hosts'       = 'http://elasticsearch:9200',
    'index'       = 'revenue-by-hour'
);

-- ============================================================
-- Query 1: Tumbling Window 1 minute — Revenue per category
-- ============================================================
INSERT INTO revenue_per_minute
SELECT
    window_start,
    window_end,
    category,
    SUM(total_amount)                          AS total_revenue,
    COUNT(*)                                   AS event_count,
    ROUND(SUM(total_amount) / COUNT(*), 2)     AS avg_order_value
FROM TABLE(
    TUMBLE(TABLE purchase_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, category;
