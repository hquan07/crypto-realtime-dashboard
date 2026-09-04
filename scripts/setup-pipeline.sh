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

install_jar() {
    local container=$1
    local url=$2
    local jar
    jar=$(basename "$url")
    docker exec "$container" bash -c "
        cd /opt/flink/lib
        if [ -f '$jar' ]; then
            echo '  ✓ $jar already present in $container'
        else
            echo '  ↓ downloading $jar into $container'
            curl -sfLO '$url'
        fi
    "
}

KAFKA_JAR="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar"
JDBC_JAR="https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar"
CH_JDBC_JAR="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0-patch5/clickhouse-jdbc-0.6.0-patch5-all.jar"

for c in "$JOB_MANAGER" "$TASK_MANAGER"; do
    install_jar "$c" "$KAFKA_JAR"
    install_jar "$c" "$JDBC_JAR"
    install_jar "$c" "$CH_JDBC_JAR"
done

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
echo "🔧 Step 4: Submit Flink SQL jobs"
echo "============================================================"
for job in crypto_aggregation crypto_indicators crypto_alerts; do
    echo "  → submitting ${job}.sql"
    docker exec "$JOB_MANAGER" /opt/flink/bin/sql-client.sh \
        -f "/opt/flink/usrlib/flink-sql-jobs/${job}.sql" 2>&1 | tail -3
done

echo ""
echo "============================================================"
echo "🎉 Pipeline is LIVE!"
echo "============================================================"
echo "  Dashboard:       http://localhost:8000"
echo "  Flink UI:        http://localhost:8081"
echo "  ClickHouse HTTP: http://localhost:8123"
echo "============================================================"