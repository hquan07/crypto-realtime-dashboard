#!/usr/bin/env python3
"""
Redis Sink Service
==================
Since Apache Flink 1.19 does not have an official SQL Redis sink connector out of the box,
this lightweight microservice acts as the Redis Sink. It continuously polls Elasticsearch
for the latest aggregated revenue data (which Flink just wrote) and updates Redis.
This powers fast dashboards and real-time alerts.
"""

import time
import json
import logging
from elasticsearch import Elasticsearch
import redis

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("redis-sink")

ES_HOST = "http://localhost:9200"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

def main():
    logger.info("Starting Redis Sink Service...")
    
    # Initialize connections
    es = Elasticsearch(ES_HOST)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    while True:
        try:
            # Query the latest window data from Elasticsearch
            query = {
                "size": 20,
                "sort": [{"window_start": "desc"}],
                "query": {"match_all": {}}
            }
            
            response = es.search(index="revenue-by-minute", body=query)
            hits = response.get('hits', {}).get('hits', [])
            
            if not hits:
                time.sleep(5)
                continue
            
            # The top hit gives us the latest window start time
            latest_window = hits[0]['_source']['window_start']
            
            # Group all hits belonging to the latest window
            latest_data = [hit['_source'] for hit in hits if hit['_source']['window_start'] == latest_window]
            
            # Update Redis
            pipe = r.pipeline()
            for data in latest_data:
                category = data['category']
                revenue = data['total_revenue']
                events = data['event_count']
                
                # We store the latest revenue per category in a Redis Hash
                pipe.hset("realtime:revenue:category", category, revenue)
                pipe.hset("realtime:events:category", category, events)
                
            # Also store the overall latest window timestamp
            pipe.set("realtime:latest_window", latest_window)
            
            pipe.execute()
            
            logger.info(f"✅ Synced {len(latest_data)} categories to Redis for window {latest_window}")
            
        except Exception as e:
            logger.error(f"Error syncing to Redis: {e}")
            
        # Poll every 10 seconds
        time.sleep(10)

if __name__ == "__main__":
    main()
