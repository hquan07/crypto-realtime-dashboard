#!/bin/bash
# ============================================================
# Crypto Streaming Pipeline — Full Setup Script
# ============================================================
# Downloads Flink connectors, submits the 3 crypto Flink SQL
# jobs, and creates the Elasticsearch index used by /api/history.
#
# Prerequisite: `docker compose up -d` must already be running
# and healthy.
# ============================================================
set -e

ES_HOST="http://localhost:9200"
FLINK_HOST="http://localhost:8081"
JOB_MANAGER="flink-jobmanager"
TASK_MANAGER="flink-taskmanager"

echo "============================================================"
echo "🔧 Step 1: Install Flink Connectors (Kafka + Elasticsearch)"
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
ES_JAR="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.1.0-1.19/flink-sql-connector-elasticsearch7-3.1.0-1.19.jar"

for c in "$JOB_MANAGER" "$TASK_MANAGER"; do
    install_jar "$c" "$KAFKA_JAR"
    install_jar "$c" "$ES_JAR"
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
echo "🔧 Step 3: Create Elasticsearch index"
echo "============================================================"
curl -sf -X PUT "$ES_HOST/crypto-by-minute" -H "Content-Type: application/json" -d '{
  "mappings": {
    "properties": {
      "window_start":  {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"},
      "window_end":    {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"},
      "symbol":        {"type": "keyword"},
      "total_volume":  {"type": "double"},
      "trade_count":   {"type": "long"},
      "avg_price":     {"type": "double"}
    }
  }
}' >/dev/null 2>&1 && echo " ✅ crypto-by-minute index created" \
                  || echo " ℹ️  crypto-by-minute already exists"

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
echo "  Dashboard:    http://localhost:8000"
echo "  Flink UI:     http://localhost:8081"
echo "  Kibana:       http://localhost:5601"
echo "  ES Health:    http://localhost:9200/_cluster/health"
echo "============================================================"