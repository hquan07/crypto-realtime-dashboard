#!/usr/bin/env python3
import json
import logging
import redis
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("indicator_consumer")

import os
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC = 'crypto-indicators'
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = 6379

def main():
    logger.info("Starting Indicator Consumer...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='fastapi-indicators-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Listening to Kafka topic: {TOPIC}")
    
    for message in consumer:
        indicator = message.value
        symbol = indicator.get('symbol')
        sma_1m = indicator.get('sma_1m')
        if symbol and sma_1m is not None:
            # Lưu SMA 1 phút vào Hash trong Redis
            r.hset("crypto:sma_1m", symbol, sma_1m)

if __name__ == '__main__':
    main()
