"""
Crypto Realtime Dashboard — FastAPI backend
============================================
Exposes:
    GET  /api/data              — one-shot snapshot from Redis
    GET  /api/data/stream       — SSE push, 2s cadence (replaces client polling)
    GET  /api/history?symbols=  — historical 1-min bars from ClickHouse
    GET  /api/alerts/stream     — SSE for whale/downtrend alerts from Redis Pub/Sub
    GET  /health                — liveness + Redis/ClickHouse connectivity check
    GET  /                      — static frontend
"""

import os
import json
import logging
import asyncio
from typing import Any, Dict

import redis
import clickhouse_connect
from fastapi import FastAPI, Query, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("dashboard-api")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
SSE_INTERVAL_SEC = float(os.getenv("SSE_INTERVAL_SEC", "2.0"))

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(_app):
    logger.info(
        "dashboard-api starting: REDIS_HOST=%s CLICKHOUSE_HOST=%s:%s",
        REDIS_HOST, CLICKHOUSE_HOST, CLICKHOUSE_PORT,
    )
    # Best-effort connectivity check — do NOT fail startup if downstream is not up
    # yet, because docker-compose may bring services up in parallel.
    try:
        r.ping()
        logger.info("Redis reachable at %s:%s", REDIS_HOST, REDIS_PORT)
    except redis.RedisError as e:
        logger.warning("Redis not reachable at startup (%s). Will retry per-request.", e)
    try:
        ch.ping()
        logger.info("ClickHouse reachable at %s:%s", CLICKHOUSE_HOST, CLICKHOUSE_PORT)
    except Exception as e:
        logger.warning("ClickHouse not reachable at startup (%s). Will retry per-request.", e)
    yield


app = FastAPI(title="Crypto Realtime Dashboard API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis client with a small retry budget so a transient blip doesn't fail a request.
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True,
    socket_connect_timeout=3,
    socket_timeout=3,
    retry_on_timeout=True,
    health_check_interval=30,
)
ch = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    database="cryptodb",
    connect_timeout=5,
    send_receive_timeout=5,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fetch_snapshot() -> Dict[str, Any]:
    """Read the current snapshot out of Redis. Raises redis.RedisError on failure."""
    prices_raw = r.hgetall("crypto:avg_price")
    volumes_raw = r.hgetall("crypto:volume")
    trades_raw = r.hgetall("crypto:trades")
    smas_raw = r.hgetall("crypto:sma_1m")
    preds_raw = r.hgetall("crypto:prediction")

    return {
        "prices":      {k: float(v) for k, v in prices_raw.items()}   if prices_raw  else {},
        "volumes":     {k: float(v) for k, v in volumes_raw.items()}  if volumes_raw else {},
        "trades":      {k: int(float(v)) for k, v in trades_raw.items()} if trades_raw else {},
        "smas":        {k: float(v) for k, v in smas_raw.items()}     if smas_raw    else {},
        "predictions": {k: json.loads(v) for k, v in preds_raw.items()} if preds_raw else {},
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    """Liveness + dependency check. Returns 503 if Redis is down (ClickHouse is optional)."""
    redis_ok = False
    ch_ok = False
    try:
        redis_ok = bool(r.ping())
    except redis.RedisError as e:
        logger.warning("health: Redis ping failed: %s", e)
    try:
        ch_ok = bool(ch.ping())
    except Exception as e:
        logger.warning("health: ClickHouse ping failed: %s", e)

    body = {"status": "ok" if redis_ok else "degraded", "redis": redis_ok, "clickhouse": ch_ok}
    if not redis_ok:
        raise HTTPException(status_code=503, detail=body)
    return body


@app.get("/api/data")
def get_crypto_data() -> Dict[str, Any]:
    """Snapshot of the latest dashboard data from Redis."""
    try:
        return {"status": "success", "data": _fetch_snapshot()}
    except redis.RedisError as e:
        logger.error("/api/data Redis error: %s", e)
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}")
    except Exception as e:
        logger.exception("/api/data unexpected error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/data/stream")
async def data_stream() -> StreamingResponse:
    """Push the dashboard snapshot to the client every SSE_INTERVAL_SEC seconds.

    Replaces the previous 1-second client-side polling of /api/data.
    On Redis errors, streams an `event: error` frame instead of terminating the
    connection — the client keeps its EventSource alive and retries transparently.
    """
    async def gen():
        while True:
            try:
                snapshot = _fetch_snapshot()
                yield f"data: {json.dumps({'status': 'success', 'data': snapshot})}\n\n"
            except redis.RedisError as e:
                logger.warning("/api/data/stream Redis error: %s", e)
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
            except Exception as e:
                logger.exception("/api/data/stream unexpected error")
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
            await asyncio.sleep(SSE_INTERVAL_SEC)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/history")
def get_crypto_history(symbols: str = Query("BTCUSDT", description="Comma-separated symbols")) -> Dict[str, Any]:
    """Historical 1-minute bars from ClickHouse, up to 60 per symbol."""
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()] or ["BTCUSDT"]
    history_data: Dict[str, list] = {}
    try:
        for sym in symbol_list:
            result = ch.query(
                "SELECT window_start, avg_price, total_volume, trade_count "
                "FROM crypto_by_minute "
                "WHERE symbol = {sym:String} "
                "ORDER BY window_start DESC "
                "LIMIT 60",
                parameters={"sym": sym},
            )
            records = []
            for row in result.result_rows:
                records.append({
                    "time":   str(row[0]),
                    "price":  row[1],
                    "volume": row[2],
                    "trades": row[3],
                })
            records.reverse()  # oldest → newest
            history_data[sym] = records
        return {"status": "success", "data": history_data}
    except Exception as e:
        logger.error("/api/history error for symbols=%s: %s", symbols, e)
        # Return 503 rather than 200 so client-side error handling can trigger.
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {e}")


@app.get("/api/alerts/stream")
async def alerts_stream() -> StreamingResponse:
    """SSE fan-out for whale + downtrend alerts published on Redis Pub/Sub."""
    async def event_generator():
        pubsub = r.pubsub()
        try:
            pubsub.subscribe("crypto_alerts_channel")
            while True:
                try:
                    message = pubsub.get_message(ignore_subscribe_messages=True)
                    if message:
                        yield f"data: {message['data']}\n\n"
                except redis.RedisError as e:
                    logger.warning("/api/alerts/stream Redis error: %s", e)
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass
        finally:
            try:
                pubsub.unsubscribe("crypto_alerts_channel")
                pubsub.close()
            except Exception:
                pass

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Static frontend (must be mounted last so /api/* takes precedence)
# ---------------------------------------------------------------------------
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)