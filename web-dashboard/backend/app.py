"""
Crypto Realtime Dashboard — FastAPI backend (V2)
================================================
Exposes:
    GraphQL /graphql           — flexible queries for user data
    POST /api/auth/register
    POST /api/auth/login
    GET  /api/data             — one-shot snapshot from Redis
    GET  /api/data/stream      — SSE push
    GET  /api/history          — historical 1-min bars from ClickHouse
    GET  /api/alerts/stream    — SSE for alerts
    GET  /health               — liveness
"""

import os
import json
import logging
import asyncio
from typing import Any, Dict
from contextlib import asynccontextmanager

import redis
import clickhouse_connect
from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from strawberry.fastapi import GraphQLRouter
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from auth import get_password_hash, verify_password, create_access_token, get_current_user, security
from models import Base, User, Watchlist, Alert
from schema import schema

# ---------------------------------------------------------------------------
# Logging & Config
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("dashboard-api")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
SSE_INTERVAL_SEC = float(os.getenv("SSE_INTERVAL_SEC", "2.0"))
SQLITE_URL = "sqlite:///./dashboard.db"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
engine = create_engine(SQLITE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------------------------------------------------------------------
# Redis & ClickHouse Clients
# ---------------------------------------------------------------------------
import redis.asyncio as aioredis

r = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True,
    socket_connect_timeout=3, socket_timeout=3, retry_on_timeout=True
)
ch = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database="cryptodb",
    connect_timeout=5, send_receive_timeout=5,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Init Rate Limiter (needs aioredis)
    redis_async = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", encoding="utf-8", decode_responses=True)
    await FastAPILimiter.init(redis_async)
    
    try:
        r.ping()
        logger.info("Redis reachable.")
    except redis.RedisError as e:
        logger.warning("Redis unreachable: %s", e)
    
    yield

app = FastAPI(title="Crypto Dashboard API V2", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ---------------------------------------------------------------------------
# GraphQL
# ---------------------------------------------------------------------------
async def get_graphql_context(request: Request, db=Depends(get_db)):
    # Try to extract user from Authorization header if present
    user = None
    if "Authorization" in request.headers:
        try:
            token = request.headers["Authorization"].split(" ")[1]
            import jwt
            from auth import SECRET_KEY, ALGORITHM
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            user = payload.get("sub")
        except Exception:
            pass
    request.state.user = user
    return {"db": db, "request": request}

graphql_app = GraphQLRouter(schema, context_getter=get_graphql_context)
app.include_router(graphql_app, prefix="/graphql")

# ---------------------------------------------------------------------------
# Auth Endpoints
# ---------------------------------------------------------------------------
class UserCreate(BaseModel):
    username: str
    password: str

@app.post("/api/auth/register")
def register(user: UserCreate, db=Depends(get_db)):
    if db.query(User).filter(User.username == user.username).first():
        raise HTTPException(status_code=400, detail="Username registered")
    
    new_user = User(username=user.username, password_hash=get_password_hash(user.password))
    db.add(new_user)
    db.commit()
    return {"status": "success"}

@app.post("/api/auth/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db=Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    token = create_access_token({"sub": user.username})
    return {"access_token": token, "token_type": "bearer"}

# ---------------------------------------------------------------------------
# Snapshot & Stream Endpoints (Rate Limited)
# ---------------------------------------------------------------------------
def _fetch_snapshot() -> Dict[str, Any]:
    prices = r.hgetall("crypto:avg_price")
    volumes = r.hgetall("crypto:volume")
    trades = r.hgetall("crypto:trades")
    smas = r.hgetall("crypto:sma_1m")
    preds = r.hgetall("crypto:prediction")
    return {
        "prices": {k: float(v) for k, v in prices.items()} if prices else {},
        "volumes": {k: float(v) for k, v in volumes.items()} if volumes else {},
        "trades": {k: int(float(v)) for k, v in trades.items()} if trades else {},
        "smas": {k: float(v) for k, v in smas.items()} if smas else {},
        "predictions": {k: json.loads(v) for k, v in preds.items()} if preds else {},
    }

@app.get("/api/data", dependencies=[Depends(RateLimiter(times=5, seconds=1))])
def get_crypto_data():
    try:
        return {"status": "success", "data": _fetch_snapshot()}
    except redis.RedisError as e:
        raise HTTPException(status_code=503, detail=str(e))

@app.get("/api/data/stream")
async def data_stream():
    async def gen():
        while True:
            try:
                snapshot = _fetch_snapshot()
                yield f"data: {json.dumps({'status': 'success', 'data': snapshot})}\n\n"
            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
            await asyncio.sleep(SSE_INTERVAL_SEC)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/history")
def get_crypto_history(symbols: str = Query("BTCUSDT")):
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()] or ["BTCUSDT"]
    history_data = {}
    try:
        for sym in symbol_list:
            result = ch.query(
                "SELECT window_start, avg_price, total_volume, trade_count FROM crypto_by_minute WHERE symbol = {sym:String} ORDER BY window_start DESC LIMIT 60",
                parameters={"sym": sym},
            )
            records = [{"time": str(r[0]), "price": r[1], "volume": r[2], "trades": r[3]} for r in result.result_rows]
            records.reverse()
            history_data[sym] = records
        return {"status": "success", "data": history_data}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

@app.get("/api/alerts/stream")
async def alerts_stream():
    async def event_generator():
        pubsub = r.pubsub()
        try:
            pubsub.subscribe("crypto_alerts_channel")
            while True:
                message = pubsub.get_message(ignore_subscribe_messages=True)
                if message:
                    yield f"data: {message['data']}\n\n"
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass
        finally:
            try:
                pubsub.unsubscribe("crypto_alerts_channel")
                pubsub.close()
            except:
                pass
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/health")
def health():
    return {"status": "ok"}

# Frontend Mount
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)