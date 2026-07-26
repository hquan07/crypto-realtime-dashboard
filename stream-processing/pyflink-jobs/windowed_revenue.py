"""
Realtime Streaming Pipeline — PyFlink: Windowed Revenue Aggregation
===================================================================
Python equivalent of the Flink SQL job, demonstrating the Table API
and DataStream API for windowed aggregation.

Usage:
    Submit to Flink cluster:
        flink run -py windowed_revenue.py

    Or run locally (requires PyFlink installed):
        python windowed_revenue.py
"""

import os
import json
import logging

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# ============================================================
# Configuration
# ============================================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PURCHASE_TOPIC = os.getenv("PURCHASE_TOPIC", "purchase-events")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("windowed_revenue")


# ============================================================
# Aggregate Function — Revenue Counter
# ============================================================
class RevenueAggregator(AggregateFunction):
    """Accumulates total revenue and event count within a window."""

    def create_accumulator(self):
        # (total_revenue, event_count)
        return (0.0, 0)

    def add(self, value, accumulator):
        total_amount = value.get("total_amount", 0.0)
        return (accumulator[0] + total_amount, accumulator[1] + 1)

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        return (a[0] + b[0], a[1] + b[1])


# ============================================================
# Main Job
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("🚀 PyFlink Windowed Revenue Aggregation Job")
    logger.info("=" * 60)
    logger.info(f"   Kafka Broker    : {KAFKA_BROKER}")
    logger.info(f"   Purchase Topic  : {PURCHASE_TOPIC}")
    logger.info(f"   ES Host         : {ELASTICSEARCH_HOST}")
    logger.info("=" * 60)

    # --------------------------------------------------------
    # 1. Environment Setup
    # --------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)

    # Enable checkpointing for exactly-once semantics
    env.enable_checkpointing(60000)  # Every 60 seconds

    t_env = StreamTableEnvironment.create(env)

    # --------------------------------------------------------
    # 2. Flink SQL approach (recommended for production)
    # --------------------------------------------------------
    # Using Flink SQL via Table API for cleaner code

    # Source table — Kafka
    t_env.execute_sql(f"""
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
            event_time AS TO_TIMESTAMP(`timestamp`),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'                        = 'kafka',
            'topic'                            = '{PURCHASE_TOPIC}',
            'properties.bootstrap.servers'     = '{KAFKA_BROKER}',
            'properties.group.id'              = 'pyflink-revenue-job',
            'format'                           = 'json',
            'json.ignore-parse-errors'         = 'true',
            'scan.startup.mode'                = 'latest-offset'
        )
    """)

    # Sink table — Elasticsearch (revenue by minute)
    t_env.execute_sql(f"""
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
            'hosts'       = '{ELASTICSEARCH_HOST}',
            'index'       = 'revenue-by-minute'
        )
    """)

    # Sink table — Print to console (for debugging)
    t_env.execute_sql("""
        CREATE TABLE revenue_console (
            window_start   TIMESTAMP(3),
            window_end     TIMESTAMP(3),
            category       STRING,
            total_revenue  DOUBLE,
            event_count    BIGINT,
            avg_order_value DOUBLE
        ) WITH (
            'connector'   = 'print'
        )
    """)

    # --------------------------------------------------------
    # 3. Tumbling Window Query — Revenue per minute by category
    # --------------------------------------------------------
    logger.info("📊 Starting tumbling window aggregation (1 minute)...")

    t_env.execute_sql("""
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
        GROUP BY window_start, window_end, category
    """)

    # Also print to console for debugging
    t_env.execute_sql("""
        INSERT INTO revenue_console
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
        GROUP BY window_start, window_end, category
    """).wait()

    logger.info("✅ Job completed.")


if __name__ == "__main__":
    main()
