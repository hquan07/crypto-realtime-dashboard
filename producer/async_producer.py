#!/usr/bin/env python3
"""
Async Crypto WebSocket Producer
=================================
Connects to cryptocurrency exchanges (Binance, etc.) via async WebSockets.
Subscribes to Trade (L1) and Depth (L2) streams for active USDT pairs.
Normalizes data and pushes to Kafka asynchronously using confluent_kafka.
"""

import asyncio
import json
import logging
import os
import time
from typing import List, Optional

import aiohttp
from aiohttp import web
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("async-crypto-producer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_TRADES = os.getenv("KAFKA_TOPIC", "crypto-trades")
TOPIC_DEPTH = os.getenv("KAFKA_TOPIC_DEPTH", "crypto-depth")
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_INFO = "https://api.binance.com/api/v3/exchangeInfo"

# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------
# confluent_kafka's produce() is non-blocking and thread-safe.
kafka_producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "async-crypto-producer",
    "linger.ms": 5,
    "compression.type": "lz4",
})

# ---------------------------------------------------------------------------
# Health State
# ---------------------------------------------------------------------------
class HealthState:
    started_at = time.time()
    last_message_at = None
    messages_sent = 0
    active_connections = 0

state = HealthState()

def touch_message():
    state.last_message_at = time.time()
    state.messages_sent += 1

def delivery_report(err, msg):
    if err is not None:
        logger.error("❌ Kafka delivery failed: %s", err)

async def kafka_poll_task():
    """Background task to poll Kafka producer events to trigger delivery callbacks."""
    while True:
        kafka_producer.poll(0)
        await asyncio.sleep(1)

# ---------------------------------------------------------------------------
# Health HTTP server (aiohttp)
# ---------------------------------------------------------------------------
async def health_handler(request):
    now = time.time()
    age = (now - state.last_message_at) if state.last_message_at else None
    
    # Healthy if at least one WS is connected AND received message in last 60s
    # Or within 30s grace period.
    healthy = state.active_connections > 0 and (
        (age is not None and age < 60)
        or (now - state.started_at) < 30
    )
    
    data = {
        "status": "ok" if healthy else "unhealthy",
        "active_connections": state.active_connections,
        "messages_sent": state.messages_sent,
        "seconds_since_last_message": round(age, 2) if age is not None else None,
        "uptime_seconds": round(now - state.started_at, 2),
    }
    return web.json_response(data, status=200 if healthy else 503)

async def start_health_server():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HEALTH_PORT)
    await site.start()
    logger.info("Health server listening on :%d", HEALTH_PORT)

# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------
def normalize_binance_trade(msg: dict) -> Optional[dict]:
    if msg.get("e") != "trade":
        return None
    return {
        "exchange": "binance",
        "symbol": msg.get("s"),
        "price": float(msg.get("p", 0.0)),
        "quantity": float(msg.get("q", 0.0)),
        "trade_time": msg.get("T"),
        "is_buyer_maker": bool(msg.get("m", False)),
    }

def normalize_binance_depth(msg: dict) -> Optional[dict]:
    # Binance depth update payload (depthUpdate)
    if msg.get("e") == "depthUpdate":
        return {
            "exchange": "binance",
            "symbol": msg.get("s"),
            "bids": msg.get("b", []),
            "asks": msg.get("a", []),
            "update_time": msg.get("E")
        }
    return None

# ---------------------------------------------------------------------------
# WebSocket Client
# ---------------------------------------------------------------------------
async def fetch_usdt_symbols(session: aiohttp.ClientSession) -> List[str]:
    """Fetch active USDT trading pairs from Binance."""
    try:
        logger.info("Fetching exchange info from Binance...")
        async with session.get(BINANCE_INFO) as response:
            data = await response.json()
            symbols = [
                s["symbol"].lower()
                for s in data.get("symbols", [])
                if s["symbol"].endswith("USDT") and s["status"] == "TRADING"
            ]
            logger.info("Found %d active USDT pairs.", len(symbols))
            return symbols
    except Exception as e:
        logger.error("Failed to fetch symbols: %s. Falling back to majors.", e)
        return ["btcusdt", "ethusdt", "solusdt", "adausdt"]

async def binance_ws_worker(session: aiohttp.ClientSession, symbols: List[str], worker_id: int):
    """Connects to Binance WS for a subset of symbols."""
    # Binance allows up to 1024 streams per connection
    # We will subscribe to trade and depthUpdate
    streams = [f"{s}@trade" for s in symbols] + [f"{s}@depth@100ms" for s in symbols]
    
    # Combined stream URL
    stream_param = "/".join(streams)
    ws_url = f"wss://stream.binance.com:9443/stream?streams={stream_param}"
    
    backoff = 1.0
    while True:
        try:
            logger.info("[worker %d] Connecting to %d streams...", worker_id, len(streams))
            async with session.ws_connect(ws_url, heartbeat=30.0) as ws:
                state.active_connections += 1
                backoff = 1.0  # reset on successful connect
                logger.info("[worker %d] Connected.", worker_id)
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        stream_name = data.get("stream", "")
                        payload = data.get("data", {})
                        
                        if "@trade" in stream_name:
                            normalized = normalize_binance_trade(payload)
                            if normalized:
                                kafka_producer.produce(TOPIC_TRADES, key=normalized["symbol"], value=json.dumps(normalized), callback=delivery_report)
                                touch_message()
                        elif "@depth" in stream_name:
                            normalized = normalize_binance_depth(payload)
                            if normalized:
                                kafka_producer.produce(TOPIC_DEPTH, key=normalized["symbol"], value=json.dumps(normalized), callback=delivery_report)
                                touch_message()
                                
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        logger.warning("[worker %d] WS Closed", worker_id)
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("[worker %d] WS Error", worker_id)
                        break
                        
        except Exception as e:
            logger.error("[worker %d] Connection error: %s", worker_id, e)
        finally:
            state.active_connections = max(0, state.active_connections - 1)
            
        logger.info("[worker %d] Reconnecting in %.1fs...", worker_id, backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60.0)

def chunk_list(seq: List[str], max_size: int) -> List[List[str]]:
    return [seq[i:i + max_size] for i in range(0, len(seq), max_size)]

async def main():
    logger.info("Starting Async Crypto Producer.")
    
    await start_health_server()
    asyncio.create_task(kafka_poll_task())
    
    async with aiohttp.ClientSession() as session:
        symbols = await fetch_usdt_symbols(session)
        
        # Max streams per Binance combined connection is 1024. 
        # We use 2 streams per symbol (trade + depth). So max 500 symbols per connection.
        symbol_chunks = chunk_list(symbols, 400)
        
        logger.info("Split %d symbols into %d workers.", len(symbols), len(symbol_chunks))
        
        workers = []
        for i, chunk in enumerate(symbol_chunks):
            workers.append(asyncio.create_task(binance_ws_worker(session, chunk, i)))
            
        await asyncio.gather(*workers)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down; flushing Kafka producer...")
        kafka_producer.flush(timeout=5)
