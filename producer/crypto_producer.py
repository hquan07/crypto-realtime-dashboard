#!/usr/bin/env python3
"""
Binance Crypto WebSocket Producer
=================================
Connects to Binance WebSocket streams for real-time cryptocurrency trades
and forwards them to a Kafka topic.

Dynamically fetches all active USDT trading pairs from Binance.
"""

import os
import json
import logging
import websocket
import requests
import time
from confluent_kafka import Producer
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("crypto-producer")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = "crypto-trades"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"

# Initialize Kafka Producer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'binance-crypto-producer'
}
producer = Producer(conf)

def get_all_usdt_symbols():
    """Fetches all active USDT trading pairs from Binance."""
    try:
        logger.info("Fetching exchange info from Binance...")
        response = requests.get("https://api.binance.com/api/v3/exchangeInfo")
        response.raise_for_status()
        data = response.json()
        
        symbols = []
        for s in data.get('symbols', []):
            if s['symbol'].endswith('USDT') and s['status'] == 'TRADING':
                # websocket streams require lowercase
                symbols.append(s['symbol'].lower())
                
        logger.info(f"Found {len(symbols)} active USDT pairs.")
        # Binance allows max 1024 streams per connection. 
        # If > 1000, we'd need multiple connections, but for USDT pairs it's usually ~400-500.
        return symbols
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        # Fallback to a few major coins if API fails
        return ["btcusdt", "ethusdt", "solusdt", "adausdt"]

def delivery_report(err, msg):
    """Callback for Kafka delivery reports."""
    if err is not None:
        logger.error(f"❌ Message delivery failed: {err}")

def on_message(ws, message):
    """Callback when a message is received from Binance."""
    try:
        data = json.loads(message)
        
        if "e" not in data or data["e"] != "trade":
            # Ignore non-trade messages like subscription responses
            if "result" in data:
                logger.info(f"Subscription successful. ID: {data.get('id')}")
            return

        # Transform to a cleaner schema for Flink
        trade_event = {
            "symbol": data.get("s"),
            "price": float(data.get("p", 0.0)),
            "quantity": float(data.get("q", 0.0)),
            "trade_time": data.get("T"),  # Epoch milliseconds
            "is_buyer_maker": data.get("m", False)
        }
        
        # Produce to Kafka
        producer.produce(
            TOPIC,
            key=trade_event["symbol"],
            value=json.dumps(trade_event),
            callback=delivery_report
        )
        producer.poll(0)
        
        # logger.info(f"Streamed: {trade_event['symbol']} - Price: ${trade_event['price']} - Qty: {trade_event['quantity']}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket Closed")
    producer.flush()

def on_open(ws):
    logger.info("✅ Connected to Binance WebSocket. Sending subscription...")
    symbols = get_all_usdt_symbols()
    
    # We create stream names by appending @trade
    streams = [f"{s}@trade" for s in symbols]
    
    # Max streams per request is usually ~1024, but let's chunk them if needed.
    # For ~450 pairs, one request is fine.
    subscribe_msg = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }
    ws.send(json.dumps(subscribe_msg))

def main():
    logger.info(f"Starting Crypto Producer. Target Kafka: {KAFKA_BROKER}, Topic: {TOPIC}")
    # Connect to WebSocket
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(ping_interval=60, ping_timeout=10)

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            logger.error(f"Producer crashed: {e}. Restarting in 5s...")
            time.sleep(5)
