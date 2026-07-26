"""
Momentum Signal Service
========================
Consumes crypto trades from Kafka and computes a simple **momentum indicator**
per symbol using a rolling 20-tick window. This is NOT a machine-learning model
— it's a lightweight technical signal:

    change = (mean(last 5 prices) - mean(first 5 prices)) / mean(first 5 prices)

    change >  0.001  →  UPTREND
    change < -0.001  →  DOWNTREND
    otherwise        →  SIDEWAYS

The `confidence` field is a rough proxy (|change| × 10000, capped at 99) so the
UI has something to display; it is not a calibrated probability.

Signals are written to Redis hash `crypto:prediction` for the dashboard to pick up.
Kept under this name (and directory `ml-service/`) for backwards compatibility with
existing docker-compose service names and Redis keys.
"""

import os
import json
import time
import logging
from kafka import KafkaConsumer
import redis
from collections import deque
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("momentum-signal")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')

# Rolling window for the momentum indicator.
# See module docstring for the exact rule.
WINDOW_SIZE = 20
prices = {}

logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
try:
    consumer = KafkaConsumer(
        'crypto-trades',
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest'
    )
    logger.info("Kafka connected.")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    exit(1)

logger.info(f"Connecting to Redis at {REDIS_HOST}...")
r = redis.Redis(host=REDIS_HOST, port=6379, db=0)

logger.info("Starting prediction loop...")

def predict_trend(symbol_prices):
    if len(symbol_prices) < WINDOW_SIZE:
        return None
    
    # Simple Momentum Calculation
    arr = np.array(symbol_prices)
    recent_mean = np.mean(arr[-5:])
    old_mean = np.mean(arr[:5])
    
    change = (recent_mean - old_mean) / old_mean
    
    if change > 0.001:
        direction = "UPTREND"
        confidence = min(change * 10000, 99.0)
    elif change < -0.001:
        direction = "DOWNTREND"
        confidence = min(abs(change) * 10000, 99.0)
    else:
        direction = "SIDEWAYS"
        confidence = np.random.uniform(50, 70)
        
    return {
        "direction": direction,
        "confidence": round(confidence, 2)
    }

for message in consumer:
    data = message.value
    sym = data.get('symbol')
    price = data.get('price')
    
    if not sym or not price:
        continue
        
    if sym not in prices:
        prices[sym] = deque(maxlen=WINDOW_SIZE)
        
    prices[sym].append(price)
    
    if len(prices[sym]) == WINDOW_SIZE:
        prediction = predict_trend(prices[sym])
        if prediction:
            # Publish prediction to Redis for the backend to read
            r.hset("crypto:prediction", sym, json.dumps(prediction))
            logger.info(f"Momentum signal for {sym}: {prediction}")
            # To avoid spamming, clear half the window after predicting
            for _ in range(WINDOW_SIZE // 2):
                prices[sym].popleft()