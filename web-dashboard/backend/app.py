import os
import json
import redis
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from elasticsearch import Elasticsearch

app = FastAPI(title="Crypto Realtime Dashboard API")

# Cho phép CORS (phòng hờ nếu serve frontend riêng)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

# Kết nối Redis
r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

# Kết nối Elasticsearch
es = Elasticsearch(ES_HOST)

@app.get("/api/data")
def get_crypto_data():
    """
    API endpoint để trả về dữ liệu giá và khối lượng từ Redis.
    """
    try:
        prices_raw = r.hgetall("crypto:avg_price")
        volumes_raw = r.hgetall("crypto:volume")
        trades_raw = r.hgetall("crypto:trades")
        smas_raw = r.hgetall("crypto:sma_1m")
        preds_raw = r.hgetall("crypto:prediction")
        
        prices = {k: float(v) for k, v in prices_raw.items()} if prices_raw else {}
        volumes = {k: float(v) for k, v in volumes_raw.items()} if volumes_raw else {}
        trades = {k: int(float(v)) for k, v in trades_raw.items()} if trades_raw else {}
        smas = {k: float(v) for k, v in smas_raw.items()} if smas_raw else {}
        predictions = {k: json.loads(v) for k, v in preds_raw.items()} if preds_raw else {}
        
        return {
            "status": "success",
            "data": {
                "prices": prices,
                "volumes": volumes,
                "trades": trades,
                "smas": smas,
                "predictions": predictions
            }
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "data": {
                "prices": {},
                "volumes": {},
                "trades": {},
                "smas": {},
                "predictions": {}
            }
        }

from fastapi import Query

@app.get("/api/history")
def get_crypto_history(symbols: str = Query("BTCUSDT", description="Comma separated symbols")):
    try:
        symbol_list = [s.strip() for s in symbols.split(',') if s.strip()]
        if not symbol_list:
             symbol_list = ['BTCUSDT']
        history_data = {}
        for sym in symbol_list:
            resp = es.search(
                index="crypto-by-minute",
                body={
                    "query": { "term": { "symbol": sym } },
                    "sort": [ { "window_start": "desc" } ],
                    "size": 60
                }
            )
            records = []
            for hit in resp['hits']['hits']:
                src = hit['_source']
                records.append({
                    "time": src['window_start'],
                    "price": src['avg_price'],
                    "volume": src['total_volume'],
                    "trades": src['trade_count']
                })
            records.reverse() # Oldest to newest
            history_data[sym] = records
            
        return {
            "status": "success",
            "data": history_data
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

import asyncio
from fastapi.responses import StreamingResponse

@app.get("/api/alerts/stream")
async def alerts_stream():
    async def event_generator():
        pubsub = r.pubsub()
        pubsub.subscribe('crypto_alerts_channel')
        try:
            while True:
                message = pubsub.get_message(ignore_subscribe_messages=True)
                if message:
                    yield f"data: {message['data']}\n\n"
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pubsub.unsubscribe('crypto_alerts_channel')
            
    return StreamingResponse(event_generator(), media_type="text/event-stream")

# Mount thư mục frontend để phục vụ các file HTML, CSS, JS tĩnh
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")

if __name__ == "__main__":
    import uvicorn
    # Chạy server ở port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)
