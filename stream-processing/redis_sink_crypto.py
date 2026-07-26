#!/usr/bin/env python3
"""
Crypto Redis Sink Service
=========================
Continuously polls Elasticsearch for the latest aggregated crypto data
and updates Redis for real-time dashboards.
"""

import time
import logging
from elasticsearch import Elasticsearch
import redis

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("crypto-redis-sink")

ES_HOST = "http://localhost:9200"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

def main():
    logger.info("Starting Crypto Redis Sink Service...")
    
    es = Elasticsearch(ES_HOST)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    while True:
        try:
            query = {
                "size": 200,
                "sort": [{"window_start": "desc"}],
                "query": {"match_all": {}}
            }
            
            response = es.search(index="crypto-by-minute", body=query)
            hits = response.get('hits', {}).get('hits', [])
            
            if not hits:
                time.sleep(5)
                continue
            
            seen_symbols = set()
            latest_data = []
            for hit in hits:
                symbol = hit['_source']['symbol']
                if symbol not in seen_symbols:
                    seen_symbols.add(symbol)
                    latest_data.append(hit['_source'])
            
            pipe = r.pipeline()
            for data in latest_data:
                symbol = data['symbol']
                volume = data['total_volume']
                trades = data['trade_count']
                avg_price = data['avg_price']
                
                pipe.hset("crypto:volume", symbol, volume)
                pipe.hset("crypto:trades", symbol, trades)
                pipe.hset("crypto:avg_price", symbol, avg_price)
                
            pipe.execute()
            
            logger.info(f"✅ Synced {len(latest_data)} symbols to Redis")
            
        except Exception as e:
            logger.error(f"Error syncing to Redis: {e}")
            
        time.sleep(10)

if __name__ == "__main__":
    main()
