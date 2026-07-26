#!/bin/bash
# ============================================================
# Realtime Streaming Pipeline — Full Setup Script
# ============================================================
# Installs Flink connectors, creates ES indices, creates
# Kibana data views, submits Flink jobs, and starts producer.
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ES_HOST="http://localhost:9200"
KIBANA_HOST="http://localhost:5601"
FLINK_HOST="http://localhost:8081"

echo "============================================================"
echo "🔧 Step 1: Install Flink Connectors"
echo "============================================================"

# Kafka connector
docker exec flink-jobmanager bash -c "
  cd /opt/flink/lib
  [ -f flink-sql-connector-kafka-3.2.0-1.19.jar ] && echo 'Kafka connector exists' || curl -sfLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
"
docker exec flink-taskmanager bash -c "
  cd /opt/flink/lib
  [ -f flink-sql-connector-kafka-3.2.0-1.19.jar ] && echo 'Kafka connector exists' || curl -sfLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
"

# Elasticsearch connector
docker exec flink-jobmanager bash -c "
  cd /opt/flink/lib
  [ -f flink-sql-connector-elasticsearch7-3.1.0-1.19.jar ] && echo 'ES connector exists' || curl -sfLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.1.0-1.19/flink-sql-connector-elasticsearch7-3.1.0-1.19.jar
"
docker exec flink-taskmanager bash -c "
  cd /opt/flink/lib
  [ -f flink-sql-connector-elasticsearch7-3.1.0-1.19.jar ] && echo 'ES connector exists' || curl -sfLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.1.0-1.19/flink-sql-connector-elasticsearch7-3.1.0-1.19.jar
"

echo "✅ Connectors installed"

echo ""
echo "============================================================"
echo "🔧 Step 2: Restart Flink + Fix checkpoint dirs"
echo "============================================================"

docker restart flink-jobmanager flink-taskmanager
sleep 15
docker exec flink-jobmanager bash -c "mkdir -p /opt/flink/checkpoints && chmod 777 /opt/flink/checkpoints"
docker exec flink-taskmanager bash -c "mkdir -p /opt/flink/checkpoints && chmod 777 /opt/flink/checkpoints"

# Wait for Flink to be ready
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
echo "🔧 Step 3: Create Elasticsearch Indices"
echo "============================================================"

curl -sf -X PUT "$ES_HOST/revenue-by-minute" -H "Content-Type: application/json" -d '{
  "mappings": {
    "properties": {
      "window_start": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"},
      "window_end":   {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"},
      "category":     {"type": "keyword"},
      "total_revenue": {"type": "double"},
      "event_count":  {"type": "long"},
      "avg_order_value": {"type": "double"}
    }
  }
}' 2>/dev/null && echo " ✅ revenue-by-minute index created" || echo " ℹ️  revenue-by-minute already exists"

curl -sf -X PUT "$ES_HOST/revenue-by-hour" -H "Content-Type: application/json" -d '{
  "mappings": {
    "properties": {
      "window_start": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"},
      "window_end":   {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"},
      "category":     {"type": "keyword"},
      "total_revenue": {"type": "double"},
      "event_count":  {"type": "long"},
      "avg_order_value": {"type": "double"}
    }
  }
}' 2>/dev/null && echo " ✅ revenue-by-hour index created" || echo " ℹ️  revenue-by-hour already exists"

echo ""
echo "============================================================"
echo "🔧 Step 4: Create Kibana Data Views"
echo "============================================================"

# Wait for Kibana
echo "⏳ Waiting for Kibana..."
for i in $(seq 1 60); do
  STATUS=$(curl -sf "$KIBANA_HOST/api/status" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['status']['overall']['level'])" 2>/dev/null || echo "unavailable")
  if [ "$STATUS" = "available" ]; then
    echo "✅ Kibana is ready"
    break
  fi
  sleep 3
done

# Create data view: revenue-by-minute
curl -sf -X POST "$KIBANA_HOST/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "title": "revenue-by-minute",
      "name": "Revenue by Minute",
      "timeFieldName": "window_start"
    }
  }' 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f' ✅ Data view created: {d[\"data_view\"][\"name\"]} (id: {d[\"data_view\"][\"id\"]})')" 2>/dev/null || echo " ℹ️  Data view may already exist"

# Create data view: revenue-by-hour
curl -sf -X POST "$KIBANA_HOST/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "title": "revenue-by-hour",
      "name": "Revenue by Hour",
      "timeFieldName": "window_start"
    }
  }' 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f' ✅ Data view created: {d[\"data_view\"][\"name\"]} (id: {d[\"data_view\"][\"id\"]})')" 2>/dev/null || echo " ℹ️  Data view may already exist"

echo ""
echo "============================================================"
echo "🔧 Step 5: Submit Flink SQL Job → Elasticsearch"
echo "============================================================"

docker exec flink-jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink/usrlib/flink-sql-jobs/revenue_aggregation.sql 2>&1 | tail -5

echo ""
echo "============================================================"
echo "🔧 Step 6: Start Producer (20 events/sec)"
echo "============================================================"

cd "$SCRIPT_DIR/../producer"
KAFKA_BROKER=localhost:29092 EVENTS_PER_SECOND=20 python3 -u producer.py &
PRODUCER_PID=$!
echo "✅ Producer started (PID: $PRODUCER_PID)"

echo ""
echo "============================================================"
echo "🎉 Pipeline is LIVE!"
echo "============================================================"
echo "  Flink UI:     http://localhost:8081"
echo "  Kibana:       http://localhost:5601"
echo "  ES Health:    http://localhost:9200/_cluster/health"
echo "  Producer PID: $PRODUCER_PID"
echo "============================================================"

wait $PRODUCER_PID
