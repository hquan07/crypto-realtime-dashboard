"""
Realtime Streaming Pipeline — Event Producer
=============================================
Generates simulated purchase and click events and pushes them to Kafka topics.

Usage:
    python producer.py

Environment variables:
    KAFKA_BROKER        — Kafka bootstrap servers  (default: localhost:29092)
    PURCHASE_TOPIC      — Topic for purchase events (default: purchase-events)
    CLICK_TOPIC         — Topic for click events    (default: click-events)
    EVENTS_PER_SECOND   — Target event throughput   (default: 100)
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

# ============================================================
# Configuration
# ============================================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
PURCHASE_TOPIC = os.getenv("PURCHASE_TOPIC", "purchase-events")
CLICK_TOPIC = os.getenv("CLICK_TOPIC", "click-events")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "100"))

fake = Faker()

# ============================================================
# Product Catalog
# ============================================================
PRODUCTS = [
    {"id": "P5567", "name": "Wireless Mouse", "category": "Electronics", "price": 15.99},
    {"id": "P1122", "name": "Coffee Mug", "category": "Home", "price": 8.50},
    {"id": "P3344", "name": "Running Shoes", "category": "Sports", "price": 49.90},
    {"id": "P4455", "name": "USB-C Hub", "category": "Electronics", "price": 29.99},
    {"id": "P6677", "name": "Yoga Mat", "category": "Sports", "price": 24.50},
    {"id": "P7788", "name": "Desk Lamp", "category": "Home", "price": 19.99},
    {"id": "P8899", "name": "Bluetooth Speaker", "category": "Electronics", "price": 39.99},
    {"id": "P9900", "name": "Water Bottle", "category": "Sports", "price": 12.99},
    {"id": "P1010", "name": "Notebook Set", "category": "Office", "price": 6.99},
    {"id": "P1111", "name": "Mechanical Keyboard", "category": "Electronics", "price": 79.99},
]

PAGES = [
    "/products/wireless-mouse",
    "/products/coffee-mug",
    "/products/running-shoes",
    "/category/electronics",
    "/category/sports",
    "/category/home",
    "/cart",
    "/checkout",
    "/",
    "/deals",
]

ELEMENTS = [
    "add_to_cart_btn",
    "buy_now_btn",
    "product_image",
    "product_title",
    "category_link",
    "search_bar",
    "nav_menu",
    "wishlist_btn",
    "review_section",
    "size_selector",
]

DEVICES = ["mobile", "desktop", "tablet"]
CURRENCIES = ["USD"]

# ============================================================
# Graceful shutdown
# ============================================================
running = True


def signal_handler(sig, frame):
    global running
    print("\n⏹  Shutting down producer gracefully...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================
# Event generators
# ============================================================


def generate_purchase_event(session_id: str, user_id: str) -> dict:
    """Generate a single purchase event."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "purchase",
        "user_id": user_id,
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(product["price"] * quantity, 2),
        "currency": random.choice(CURRENCIES),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        "session_id": session_id,
    }


def generate_click_event(session_id: str, user_id: str) -> dict:
    """Generate a single click event."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "click",
        "user_id": user_id,
        "page_url": random.choice(PAGES),
        "element_id": random.choice(ELEMENTS),
        "device": random.choice(DEVICES),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        "session_id": session_id,
    }


# ============================================================
# Kafka delivery callback
# ============================================================

_delivered = 0
_failed = 0


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    global _delivered, _failed
    if err is not None:
        _failed += 1
        print(f"❌ Delivery failed for {msg.topic()}[{msg.partition()}]: {err}")
    else:
        _delivered += 1


# ============================================================
# Main loop
# ============================================================


def main():
    global _delivered, _failed

    print("=" * 60)
    print("🚀 Realtime Streaming Pipeline — Producer")
    print("=" * 60)
    print(f"   Kafka Broker     : {KAFKA_BROKER}")
    print(f"   Purchase Topic   : {PURCHASE_TOPIC}")
    print(f"   Click Topic      : {CLICK_TOPIC}")
    print(f"   Target Rate      : {EVENTS_PER_SECOND} events/sec")
    print("=" * 60)

    # Kafka producer configuration
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "streaming-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 200,
        "linger.ms": 5,
        "batch.num.messages": 1000,
        "queue.buffering.max.messages": 100000,
        "compression.type": "lz4",
    }

    producer = Producer(conf)

    # Wait for Kafka to be ready
    print("\n⏳ Waiting for Kafka broker to be ready...")
    for attempt in range(30):
        try:
            metadata = producer.list_topics(timeout=5)
            if metadata.topics:
                print(f"✅ Connected to Kafka! Brokers: {len(metadata.brokers)}")
                break
        except Exception:
            if attempt < 29:
                print(f"   Attempt {attempt + 1}/30 — retrying in 2s...")
                time.sleep(2)
            else:
                print("❌ Could not connect to Kafka after 30 attempts. Exiting.")
                sys.exit(1)

    # Simulate active user sessions
    active_users = [f"U{random.randint(1000, 9999)}" for _ in range(200)]
    active_sessions = {uid: str(uuid.uuid4()) for uid in active_users}

    sleep_interval = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0.01
    batch_start = time.time()
    batch_count = 0
    total_sent = 0

    print(f"\n📡 Producing events... (Ctrl+C to stop)\n")

    while running:
        try:
            user_id = random.choice(active_users)
            session_id = active_sessions.get(user_id, str(uuid.uuid4()))

            # 70% purchase events, 30% click events
            if random.random() < 0.7:
                event = generate_purchase_event(session_id, user_id)
                topic = PURCHASE_TOPIC
            else:
                event = generate_click_event(session_id, user_id)
                topic = CLICK_TOPIC

            producer.produce(
                topic=topic,
                key=user_id,
                value=json.dumps(event),
                callback=delivery_report,
            )

            batch_count += 1
            total_sent += 1

            # Poll to trigger delivery callbacks (non-blocking)
            producer.poll(0)

            # Print stats every 5 seconds
            elapsed = time.time() - batch_start
            if elapsed >= 5.0:
                rate = batch_count / elapsed
                print(
                    f"📊 Rate: {rate:>7.1f} events/sec | "
                    f"Total: {total_sent:>8,} | "
                    f"Delivered: {_delivered:>8,} | "
                    f"Failed: {_failed}"
                )
                batch_start = time.time()
                batch_count = 0

                # Rotate some users to simulate realistic behavior
                rotate_count = random.randint(5, 20)
                for _ in range(rotate_count):
                    idx = random.randint(0, len(active_users) - 1)
                    old_user = active_users[idx]
                    new_user = f"U{random.randint(1000, 9999)}"
                    active_users[idx] = new_user
                    active_sessions.pop(old_user, None)
                    active_sessions[new_user] = str(uuid.uuid4())

            # Throttle to target rate
            time.sleep(sleep_interval)

        except BufferError:
            # Internal queue full — wait for deliveries
            print("⚠️  Producer queue full, flushing...")
            producer.flush(timeout=5)
        except KeyboardInterrupt:
            break

    # Flush remaining messages
    print(f"\n🔄 Flushing remaining messages...")
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        print(f"⚠️  {remaining} messages were not delivered")
    else:
        print(f"✅ All messages delivered!")

    print(f"\n📈 Final Stats:")
    print(f"   Total Sent    : {total_sent:,}")
    print(f"   Delivered     : {_delivered:,}")
    print(f"   Failed        : {_failed:,}")
    print(f"   Success Rate  : {(_delivered / max(total_sent, 1)) * 100:.1f}%")


if __name__ == "__main__":
    main()
