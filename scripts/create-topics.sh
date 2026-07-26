#!/bin/bash
# ============================================================
# Create Kafka topics for the Crypto Streaming Pipeline
# ============================================================
# Usage: ./create-topics.sh [KAFKA_BROKER]
# Default broker: kafka:9092
# ============================================================

set -euo pipefail

BROKER="${1:-kafka:9092}"
MAX_RETRIES=30
RETRY_INTERVAL=3

echo "=========================================="
echo "🔧 Kafka Topic Setup (Crypto Pipeline)"
echo "   Broker: ${BROKER}"
echo "=========================================="

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka broker..."
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

create_topic() {
    local name=$1
    local partitions=$2
    echo "📦 Creating topic: ${name} (partitions=${partitions})"
    kafka-topics --create --if-not-exists \
        --topic "$name" \
        --bootstrap-server "$BROKER" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config retention.ms=604800000 \
        --config cleanup.policy=delete
}

# High-throughput raw trades from Binance
create_topic crypto-trades 6
# Downstream indicator stream (1-min SMA)
create_topic crypto-indicators 3
# Alert stream (whale trades, downtrends)
create_topic crypto-alerts 3

echo ""
echo "=========================================="
echo "📋 All topics:"
kafka-topics --list --bootstrap-server "$BROKER"
echo "=========================================="
echo "✅ Topic setup complete!"