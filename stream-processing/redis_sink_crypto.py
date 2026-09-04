#!/usr/bin/env python3
"""
Crypto Redis Sink Service
=========================
Continuously polls ClickHouse for the latest aggregated crypto data
and updates Redis for real-time dashboards.

This is what populates the `crypto:avg_price`, `crypto:volume`, and
`crypto:trades` hashes that the FastAPI backend reads for `/api/data`.
Flink writes 5-second aggregations into `crypto_by_minute` in ClickHouse;
this service takes the latest window per symbol and copies it into Redis
so the dashboard has O(1) reads.
"""

import os
import time
import logging
import clickhouse_connect
import redis

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("crypto-redis-sink")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

LATEST_QUERY = """
SELECT symbol, total_volume, trade_count, avg_price
FROM cryptodb.crypto_by_minute
ORDER BY window_start DESC
LIMIT 1 BY symbol
"""

def main():
    logger.info("Starting Crypto Redis Sink Service...")

    ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database="cryptodb",
    )
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    while True:
        try:
            result = ch.query(LATEST_QUERY)

            if not result.result_rows:
                time.sleep(5)
                continue

            pipe = r.pipeline()
            for row in result.result_rows:
                symbol, volume, trades, avg_price = row
                pipe.hset("crypto:volume", symbol, volume)
                pipe.hset("crypto:trades", symbol, trades)
                pipe.hset("crypto:avg_price", symbol, avg_price)

            pipe.execute()

            logger.info(f"✅ Synced {len(result.result_rows)} symbols to Redis")

        except Exception as e:
            logger.error(f"Error syncing to Redis: {e}")

        time.sleep(10)

if __name__ == "__main__":
    main()