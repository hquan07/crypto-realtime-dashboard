"""
Realtime Streaming Pipeline — Load Test Script
================================================
Simulates traffic spikes to test pipeline under high load.

Usage:
    python load-test.py [--broker BROKER] [--duration SECONDS] [--peak-rate RATE]

Phases:
    1. Warmup    — ramp from 10 → baseline events/sec over 30s
    2. Baseline  — hold at baseline events/sec for 60s
    3. Spike     — ramp to peak events/sec over 15s, hold for 60s
    4. Recovery  — drop back to baseline for 30s
    5. Cooldown  — ramp down to 0 over 15s
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# ============================================================
# Configuration
# ============================================================
PRODUCTS = [
    {"id": "P5567", "name": "Wireless Mouse", "category": "Electronics", "price": 15.99},
    {"id": "P1122", "name": "Coffee Mug", "category": "Home", "price": 8.50},
    {"id": "P3344", "name": "Running Shoes", "category": "Sports", "price": 49.90},
    {"id": "P4455", "name": "USB-C Hub", "category": "Electronics", "price": 29.99},
    {"id": "P6677", "name": "Yoga Mat", "category": "Sports", "price": 24.50},
]

running = True


def signal_handler(sig, frame):
    global running
    print("\n⏹  Stopping load test...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def generate_event():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "purchase",
        "user_id": f"U{random.randint(1000, 9999)}",
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(product["price"] * quantity, 2),
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": str(uuid.uuid4()),
    }


_delivered = 0
_failed = 0


def delivery_report(err, msg):
    global _delivered, _failed
    if err:
        _failed += 1
    else:
        _delivered += 1


def send_events_at_rate(producer, topic, target_rate, duration, phase_name):
    """Send events at a target rate for a given duration."""
    global _delivered, _failed, running

    print(f"\n{'='*50}")
    print(f"📡 Phase: {phase_name}")
    print(f"   Target rate : {target_rate} events/sec")
    print(f"   Duration    : {duration}s")
    print(f"{'='*50}")

    if target_rate <= 0:
        return

    interval = 1.0 / target_rate
    start = time.time()
    count = 0

    while running and (time.time() - start) < duration:
        event = generate_event()
        try:
            producer.produce(
                topic=topic,
                key=event["user_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            count += 1
        except BufferError:
            producer.flush(timeout=2)

        producer.poll(0)
        time.sleep(interval)

        # Log every 5 seconds
        elapsed = time.time() - start
        if count % (target_rate * 5) < 1:
            actual_rate = count / max(elapsed, 0.001)
            print(
                f"   ⏱  {elapsed:>5.0f}s | "
                f"Sent: {count:>6,} | "
                f"Rate: {actual_rate:>7.1f}/s | "
                f"Delivered: {_delivered:>6,} | "
                f"Failed: {_failed}"
            )

    actual_rate = count / max(time.time() - start, 0.001)
    print(f"   ✅ {phase_name} complete: {count:,} events ({actual_rate:.1f}/s actual)")


def ramp(producer, topic, from_rate, to_rate, duration, phase_name):
    """Gradually change rate from from_rate to to_rate over duration seconds."""
    global running

    print(f"\n{'='*50}")
    print(f"📈 Phase: {phase_name}")
    print(f"   From : {from_rate} → {to_rate} events/sec")
    print(f"   Over : {duration}s")
    print(f"{'='*50}")

    start = time.time()
    count = 0
    last_log = start

    while running and (time.time() - start) < duration:
        progress = (time.time() - start) / duration
        current_rate = from_rate + (to_rate - from_rate) * progress
        if current_rate <= 0:
            time.sleep(0.1)
            continue

        interval = 1.0 / current_rate
        event = generate_event()

        try:
            producer.produce(
                topic=topic,
                key=event["user_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            count += 1
        except BufferError:
            producer.flush(timeout=2)

        producer.poll(0)
        time.sleep(interval)

        if time.time() - last_log >= 5:
            print(f"   ⏱  {time.time()-start:>5.0f}s | Rate: {current_rate:>7.1f}/s | Sent: {count:>6,}")
            last_log = time.time()

    print(f"   ✅ {phase_name} complete: {count:,} events")


def main():
    parser = argparse.ArgumentParser(description="Load test for Realtime Streaming Pipeline")
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:29092"),
                        help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="purchase-events", help="Kafka topic")
    parser.add_argument("--baseline-rate", type=int, default=100, help="Baseline events/sec")
    parser.add_argument("--peak-rate", type=int, default=1000, help="Peak events/sec during spike")
    parser.add_argument("--warmup", type=int, default=30, help="Warmup duration (seconds)")
    parser.add_argument("--baseline-duration", type=int, default=60, help="Baseline duration (seconds)")
    parser.add_argument("--spike-duration", type=int, default=60, help="Spike duration (seconds)")
    parser.add_argument("--recovery-duration", type=int, default=30, help="Recovery duration (seconds)")
    args = parser.parse_args()

    print("=" * 60)
    print("🔥 Realtime Streaming Pipeline — Load Test")
    print("=" * 60)
    print(f"   Broker        : {args.broker}")
    print(f"   Topic         : {args.topic}")
    print(f"   Baseline Rate : {args.baseline_rate} events/sec")
    print(f"   Peak Rate     : {args.peak_rate} events/sec")
    print("=" * 60)

    conf = {
        "bootstrap.servers": args.broker,
        "client.id": "load-tester",
        "acks": "all",
        "linger.ms": 5,
        "batch.num.messages": 2000,
        "queue.buffering.max.messages": 500000,
        "compression.type": "lz4",
    }

    producer = Producer(conf)

    # Phase 1: Warmup
    ramp(producer, args.topic, 10, args.baseline_rate, args.warmup, "🌡️  Warmup")

    # Phase 2: Baseline
    if running:
        send_events_at_rate(producer, args.topic, args.baseline_rate, args.baseline_duration, "📊 Baseline")

    # Phase 3: Spike ramp-up
    if running:
        ramp(producer, args.topic, args.baseline_rate, args.peak_rate, 15, "🚀 Spike Ramp-up")

    # Phase 4: Spike hold
    if running:
        send_events_at_rate(producer, args.topic, args.peak_rate, args.spike_duration, "🔥 Spike Hold")

    # Phase 5: Recovery
    if running:
        ramp(producer, args.topic, args.peak_rate, args.baseline_rate, 10, "📉 Spike Drop")
        send_events_at_rate(producer, args.topic, args.baseline_rate, args.recovery_duration, "🔄 Recovery")

    # Phase 6: Cooldown
    if running:
        ramp(producer, args.topic, args.baseline_rate, 0, 15, "🧊 Cooldown")

    # Flush
    print(f"\n🔄 Flushing remaining messages...")
    remaining = producer.flush(timeout=30)

    print(f"\n{'='*60}")
    print(f"📈 Load Test Results")
    print(f"{'='*60}")
    print(f"   Total Delivered : {_delivered:,}")
    print(f"   Total Failed    : {_failed:,}")
    if remaining > 0:
        print(f"   ⚠️  Undelivered  : {remaining:,}")
    print(f"   Success Rate    : {(_delivered / max(_delivered + _failed, 1)) * 100:.1f}%")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
