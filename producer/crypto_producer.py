#!/usr/bin/env python3
"""
Binance Crypto WebSocket Producer
=================================
Connects to Binance WebSocket streams for real-time cryptocurrency trades and
forwards them to a Kafka topic.

Dynamically fetches all active USDT trading pairs from Binance and shards them
across NUM_SHARDS parallel connections (default: 2). Each shard reconnects
independently with exponential backoff + jitter.

Also runs a tiny HTTP health server on port 8080 so Docker / a monitoring
system can tell if the producer is actually receiving messages.
"""

import os
import json
import logging
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import List

import requests
import websocket
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("crypto-producer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC          = os.getenv("KAFKA_TOPIC", "crypto-trades")
NUM_SHARDS     = int(os.getenv("NUM_SHARDS", "2"))
HEALTH_PORT    = int(os.getenv("HEALTH_PORT", "8080"))
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_INFO   = "https://api.binance.com/api/v3/exchangeInfo"

# Backoff: cap at 5 min, exponential from a 5s base, plus jitter to avoid thundering herd.
BACKOFF_BASE_SEC = 5.0
BACKOFF_MAX_SEC  = 300.0

# ---------------------------------------------------------------------------
# Shared Kafka producer (confluent_kafka.Producer is thread-safe)
# ---------------------------------------------------------------------------
kafka_producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "binance-crypto-producer",
    "linger.ms": 5,
    "compression.type": "lz4",
})

# ---------------------------------------------------------------------------
# Health state (shared across shards)
# ---------------------------------------------------------------------------
_health_lock = threading.Lock()
_health_state = {
    "started_at":      time.time(),
    "last_message_at": None,
    "messages_sent":   0,
    "shards_alive":    0,
}


def _touch_message() -> None:
    with _health_lock:
        _health_state["last_message_at"] = time.time()
        _health_state["messages_sent"] += 1


def _shard_alive(delta: int) -> None:
    with _health_lock:
        _health_state["shards_alive"] += delta


# ---------------------------------------------------------------------------
# Health HTTP server
# ---------------------------------------------------------------------------
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (stdlib naming)
        if self.path != "/health":
            self.send_response(404); self.end_headers(); return
        with _health_lock:
            state = dict(_health_state)
        now = time.time()
        age = (now - state["last_message_at"]) if state["last_message_at"] else None
        # Healthy = at least one shard alive AND we've seen a message in the last 60s
        # (after a 30s startup grace period so Docker doesn't flap the container).
        healthy = state["shards_alive"] > 0 and (
            (age is not None and age < 60)
            or (now - state["started_at"]) < 30
        )
        body = json.dumps({
            "status": "ok" if healthy else "unhealthy",
            "shards_alive":  state["shards_alive"],
            "messages_sent": state["messages_sent"],
            "seconds_since_last_message": round(age, 2) if age is not None else None,
            "uptime_seconds": round(now - state["started_at"], 2),
        }).encode()
        self.send_response(200 if healthy else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args, **_kwargs):  # silence default access log
        pass


def _start_health_server() -> None:
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    logger.info("Health server listening on :%d", HEALTH_PORT)
    threading.Thread(target=server.serve_forever, daemon=True, name="health").start()


# ---------------------------------------------------------------------------
# Symbol discovery
# ---------------------------------------------------------------------------
def get_all_usdt_symbols() -> List[str]:
    """Fetch all active USDT trading pairs from Binance. Falls back to majors on error."""
    try:
        logger.info("Fetching exchange info from Binance...")
        response = requests.get(BINANCE_INFO, timeout=10)
        response.raise_for_status()
        data = response.json()

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


def chunk(seq: List[str], n: int) -> List[List[str]]:
    """Split `seq` into `n` roughly-equal chunks."""
    if n <= 1 or len(seq) <= 1:
        return [seq]
    size = (len(seq) + n - 1) // n
    return [seq[i:i + size] for i in range(0, len(seq), size)]


# ---------------------------------------------------------------------------
# Message transform (extracted for unit-testability)
# ---------------------------------------------------------------------------
def transform_trade(msg: dict):
    """Convert a Binance trade message into the compact Kafka schema.

    Returns None for non-trade frames (e.g. subscription acks).
    Pure function — safe to import and unit-test.
    """
    if not isinstance(msg, dict):
        return None
    if msg.get("e") != "trade":
        return None
    try:
        return {
            "symbol":         msg.get("s"),
            "price":          float(msg.get("p", 0.0)),
            "quantity":       float(msg.get("q", 0.0)),
            "trade_time":     msg.get("T"),
            "is_buyer_maker": bool(msg.get("m", False)),
        }
    except (TypeError, ValueError):
        return None


def _delivery_report(err, msg):
    if err is not None:
        logger.error("❌ Kafka delivery failed: %s", err)


# ---------------------------------------------------------------------------
# Shard runner
# ---------------------------------------------------------------------------
class Shard:
    """Owns a single Binance WebSocket connection and forwards its trades to Kafka."""

    def __init__(self, shard_id: int, symbols: List[str]):
        self.shard_id = shard_id
        self.symbols = symbols
        self.attempt = 0

    def _on_open(self, ws):
        logger.info("[shard %d] connected; subscribing to %d streams", self.shard_id, len(self.symbols))
        streams = [f"{s}@trade" for s in self.symbols]
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": self.shard_id}))
        self.attempt = 0  # reset backoff on successful open
        _shard_alive(+1)

    def _on_message(self, _ws, message):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        # Subscription ack / non-trade frame
        if "result" in data:
            return
        trade = transform_trade(data)
        if trade is None or not trade.get("symbol"):
            return
        try:
            kafka_producer.produce(
                TOPIC,
                key=trade["symbol"],
                value=json.dumps(trade),
                callback=_delivery_report,
            )
            kafka_producer.poll(0)
            _touch_message()
        except BufferError:
            logger.warning("[shard %d] Kafka queue full, flushing...", self.shard_id)
            kafka_producer.flush(timeout=5)

    def _on_error(self, _ws, error):
        logger.error("[shard %d] WebSocket error: %s", self.shard_id, error)

    def _on_close(self, _ws, code, reason):
        logger.info("[shard %d] WebSocket closed (code=%s reason=%s)", self.shard_id, code, reason)
        _shard_alive(-1)

    def run(self):
        while True:
            try:
                ws = websocket.WebSocketApp(
                    BINANCE_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                ws.run_forever(ping_interval=60, ping_timeout=10)
            except Exception as e:
                logger.error("[shard %d] run_forever crashed: %s", self.shard_id, e)

            # Exponential backoff with jitter
            self.attempt += 1
            delay = min(BACKOFF_BASE_SEC * (2 ** (self.attempt - 1)), BACKOFF_MAX_SEC)
            delay += random.uniform(0, min(delay, 5))
            logger.info("[shard %d] reconnecting in %.1fs (attempt %d)", self.shard_id, delay, self.attempt)
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("Starting Crypto Producer. Kafka=%s Topic=%s Shards=%d", KAFKA_BROKER, TOPIC, NUM_SHARDS)
    _start_health_server()

    symbols = get_all_usdt_symbols()
    shards = chunk(symbols, NUM_SHARDS)
    logger.info("Sharding %d symbols across %d connections: %s",
                len(symbols), len(shards), [len(s) for s in shards])

    threads = []
    for i, syms in enumerate(shards):
        shard = Shard(shard_id=i, symbols=syms)
        t = threading.Thread(target=shard.run, name=f"shard-{i}", daemon=True)
        t.start()
        threads.append(t)

    # Block main thread; shard threads restart themselves on failure.
    try:
        while True:
            time.sleep(60)
            kafka_producer.poll(0)
    except KeyboardInterrupt:
        logger.info("Shutting down; flushing Kafka producer...")
        kafka_producer.flush(timeout=10)


if __name__ == "__main__":
    main()