#!/usr/bin/env python3
import json
import logging
import redis
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("alert_consumer")

import os
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC = 'crypto-alerts'
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = 6379

def main():
    logger.info("Starting Alert Consumer...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='fastapi-alerts-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Listening to Kafka topic: {TOPIC}")
    
    for message in consumer:
        alert = message.value
        logger.info(f"Received Alert: {alert}")
        
        # Publish to Redis Pub/Sub channel 'crypto_alerts_channel'
        r.publish('crypto_alerts_channel', json.dumps(alert))

if __name__ == '__main__':
    main()
