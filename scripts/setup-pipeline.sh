#!/bin/bash
# ============================================================
# Crypto Streaming Pipeline — Full Setup Script
# ============================================================
# Downloads Flink connectors, submits the 3 crypto Flink SQL
# jobs, and creates the ClickHouse table used by /api/history.
#
# Prerequisite: `docker compose up -d` must already be running
# and healthy.
# ============================================================
set -e

CLICKHOUSE_HOST="http://localhost:8123"
FLINK_HOST="http://localhost:8081"
JOB_MANAGER="flink-jobmanager"
TASK_MANAGER="flink-taskmanager"

echo "============================================================"
echo "🔧 Step 1: Install Flink Connectors (Kafka + JDBC)"
echo "============================================================"

echo "============================================================"
echo "🔧 Step 1: Skipping Connector Install (Baked in Dockerfile)"
echo "============================================================"

echo ""
echo "============================================================"
echo "🔧 Step 2: Restart Flink to pick up connectors"
echo "============================================================"
docker restart "$JOB_MANAGER" "$TASK_MANAGER" >/dev/null
echo "⏳ Waiting for Flink..."
for i in $(seq 1 30); do
    if curl -sf "$FLINK_HOST/overview" > /dev/null 2>&1; then
        echo "✅ Flink is ready"
        break
    fi
    sleep 2
done

echo ""
echo "============================================================"
echo "🔧 Step 3: Create ClickHouse table"
echo "============================================================"
curl -sf "$CLICKHOUSE_HOST" -d "
CREATE TABLE IF NOT EXISTS cryptodb.crypto_by_minute (
    window_start DateTime64(3),
    window_end   DateTime64(3),
    symbol       String,
    total_volume Float64,
    trade_count  UInt64,
    avg_price    Float64
) ENGINE = MergeTree()
ORDER BY (symbol, window_start)
TTL toDateTime(window_start) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192
" > /dev/null 2>&1 && echo " ✅ crypto_by_minute table created (or already exists)" \
                    || echo " ⚠️  Could not create table — check ClickHouse logs"

echo ""
echo "============================================================"
echo "🔧 Step 4: Submit PyFlink Jobs"
echo "============================================================"
echo "  → submitting crypto_patterns.py"
docker exec "$JOB_MANAGER" flink run --python /opt/flink/usrlib/pyflink-jobs/crypto_patterns.py

echo ""
echo "============================================================"
echo "🎉 Pipeline is LIVE!"
echo "============================================================"
echo "  Dashboard:       http://localhost:8000"
echo "  Flink UI:        http://localhost:8081"
echo "  ClickHouse HTTP: http://localhost:8123"
echo "============================================================"