#!/bin/bash
# ============================================================
# Create Kafka topics for Realtime Streaming Pipeline
# ============================================================
# Usage: ./create-topics.sh [KAFKA_BROKER]
# Default broker: kafka:9092
# ============================================================

set -euo pipefail

BROKER="${1:-kafka:9092}"
MAX_RETRIES=30
RETRY_INTERVAL=3

echo "=========================================="
echo "🔧 Kafka Topic Setup"
echo "   Broker: ${BROKER}"
echo "=========================================="

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka broker to be ready..."
for i in $(seq 1 $MAX_RETRIES); do
    if kafka-broker-api-versions --bootstrap-server "$BROKER" >/dev/null 2>&1; then
        echo "✅ Kafka is ready!"
        break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "❌ Kafka not available after ${MAX_RETRIES} attempts. Exiting."
        exit 1
    fi
    echo "   Attempt ${i}/${MAX_RETRIES} — retrying in ${RETRY_INTERVAL}s..."
    sleep "$RETRY_INTERVAL"
done

# Create purchase-events topic
echo ""
echo "📦 Creating topic: purchase-events"
kafka-topics --create --if-not-exists \
    --topic purchase-events \
    --bootstrap-server "$BROKER" \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete

# Create click-events topic
echo "🖱️  Creating topic: click-events"
kafka-topics --create --if-not-exists \
    --topic click-events \
    --bootstrap-server "$BROKER" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete

# List all topics
echo ""
echo "=========================================="
echo "📋 All topics:"
kafka-topics --list --bootstrap-server "$BROKER"
echo ""

# Describe topics
echo "📝 Topic details:"
kafka-topics --describe --bootstrap-server "$BROKER" --topic purchase-events
echo ""
kafka-topics --describe --bootstrap-server "$BROKER" --topic click-events
echo "=========================================="
echo "✅ Topic setup complete!"
