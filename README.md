# 🪙 Real-time Crypto Streaming Dashboard

Real-time market dashboard for **~450 Binance USDT pairs**. Ingests live trades
over WebSocket, aggregates them with Apache Flink (SQL), fans out to Redis for
sub-second reads, and serves a browser dashboard with live candlesticks,
volume/dominance charts, an orderbook feed, and a momentum signal.

![status](https://img.shields.io/badge/status-demo-blue)
![license](https://img.shields.io/badge/license-MIT-green)

---

## Architecture

```
                        ┌────────────────────────────────────────┐
Binance WebSocket ─────►│ crypto_producer.py  (~450 USDT pairs)  │
                        └─────────────────┬──────────────────────┘
                                          ▼
                                    ┌───────────┐
                                    │   Kafka   │  topic: crypto-trades
                                    └─────┬─────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    ▼                     ▼                     ▼
          ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
          │  Flink SQL       │  │  Flink SQL       │  │  Flink SQL       │
          │  aggregation     │  │  indicators      │  │  alerts (CEP)    │
          │  (5s tumbling)   │  │  (1m SMA / HOP)  │  │  whale + drops   │
          └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
                   │                     │                     │
                   ▼                     ▼                     ▼
          ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
          │  ClickHouse      │  │  Kafka           │  │  Kafka           │
          │  crypto_by_min   │  │  crypto-         │  │  crypto-alerts   │
          │  (MergeTree)     │  │  indicators      │  │                  │
          └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
                   │                     ▼                     ▼
                   │           ┌──────────────────┐  ┌──────────────────┐
                   │           │ indicator_       │  │ alert_consumer   │
                   │           │ consumer → Redis │  │ → Redis Pub/Sub  │
                   │           └────────┬─────────┘  └────────┬─────────┘
                   │                    │                     │
                   │           ┌────────┴─────────┐           │
                   │           │  momentum-signal │           │
                   │           │  (consumes       │           │
                   │           │   crypto-trades, │           │
                   │           │   → Redis hash)  │           │
                   │           └────────┬─────────┘           │
                   ▼                    ▼                     ▼
                             ┌───────────────────────┐
                             │  FastAPI backend      │
                             │  /api/data            │
                             │  /api/history  (CH)   │
                             │  /api/alerts/stream   │  (SSE)
                             └───────────┬───────────┘
                                         ▼
                             ┌───────────────────────┐
                             │  Static frontend      │
                             │  (Chart.js +          │
                             │   Lightweight Charts) │
                             └───────────────────────┘
```

| Layer               | Tech                                   | Purpose                                     |
| ------------------- | -------------------------------------- | ------------------------------------------- |
| Ingest              | Python + `websocket-client`            | Binance trade streams → Kafka               |
| Message broker      | Apache Kafka (Confluent 7.6)           | Buffer for ~thousands of trades/sec         |
| Stream processing   | Apache Flink 1.19 (SQL, tumbling/HOP)  | Aggregation, SMA, alerts                    |
| Fast store          | Redis 7                                | Sub-ms reads for prices/volumes/SMAs        |
| Analytics store     | ClickHouse 24.3 (MergeTree)            | 1-minute historical bars                    |
| Momentum signal     | Python + NumPy                         | Rolling-window UPTREND / DOWNTREND flag     |
| API + frontend      | FastAPI + Chart.js + Lightweight Charts | Dashboard at `http://localhost:8000`        |
| Infrastructure      | Docker Compose                         | One-command local deployment                |

---

## What the dashboard shows

- **Top 8 price cards** (by 24h volume) with live-updating prices and per-coin
  glow accents. Click any card for a quick-view modal (price, SMA, ~1-minute
  change, volume, trades, momentum signal, jump-to-charts button).
- **Overview tab** — real-time price-trend line chart (% change or per-coin) +
  live volume bar chart.
- **Market Dominance tab** — volume doughnut + trade-count polar area.
- **Trading Activity tab** — volume-vs-price bubble chart + coin-comparison radar.
- **Professional tab** — real 1-minute candlestick chart pulled from Binance
  klines, with RSI overlay, plus a live orderbook (10 asks / 10 bids, updated
  every 100 ms via Binance depth stream) and the current momentum signal.
- **Toasts** — pushed live from Redis Pub/Sub via SSE for whale trades
  (>$50k in a single fill) and 3-consecutive-drop downtrends.

---

## Quick start

### Prerequisites

- Docker Engine ≥ 24, Docker Compose ≥ 2.20
- **6 GB+ RAM recommended** — Flink wants ~512 MB min; ClickHouse is lightweight.

### 1. Start infrastructure

```bash
docker compose up -d
```

This spins up Zookeeper, Kafka (auto-creates `crypto-trades`,
`crypto-indicators`, `crypto-alerts`), Flink JobManager + TaskManager,
Redis, ClickHouse, the Python producer, stream-processing consumers,
momentum-signal service, and the FastAPI dashboard.

### 2. Install Flink connectors + submit SQL jobs

The Flink SQL jobs need the Kafka + JDBC connector JARs (plus the ClickHouse
JDBC driver) and have to be submitted once. A helper script does it all:

```bash
./scripts/setup-pipeline.sh
```

This downloads the connector JARs into the Flink containers, restarts Flink,
creates the `crypto_by_minute` ClickHouse table, and submits all three SQL
jobs (`crypto_aggregation`, `crypto_indicators`, `crypto_alerts`).

### 3. Open the dashboard

| Service               | URL                                     |
| --------------------- | --------------------------------------- |
| **Crypto Dashboard**  | http://localhost:8000                   |
| Backend health check  | http://localhost:8000/health            |
| Producer health check | http://localhost:8080/health *(inside the container network — expose the port if you want to hit it from the host)* |
| Flink Web UI          | http://localhost:8081                   |
| ClickHouse HTTP       | http://localhost:8123                   |
| Redis CLI             | `docker exec -it redis redis-cli`       |

### Run the test suite

```bash
pip install -r requirements-dev.txt \
            -r web-dashboard/backend/requirements.txt \
            -r ml-service/requirements.txt \
            -r producer/requirements.txt
pytest
```

24 tests cover the momentum indicator, producer transform + sharding, and
the FastAPI endpoints (`/health`, `/api/data`, `/api/history`) using
`fakeredis` and a stubbed ClickHouse client.

### Stop everything

```bash
docker compose down -v   # -v also wipes Kafka/Redis/ClickHouse volumes
```

---

## Project layout

```
crypto-realtime-dashboard/
├── docker-compose.yml              # All services + volumes + network
├── .env.example                    # Env-var overrides for local runs
├── producer/
│   ├── crypto_producer.py          # Binance WS → Kafka crypto-trades
│   ├── requirements.txt
│   └── Dockerfile
├── stream-processing/
│   ├── flink-sql-jobs/
│   │   ├── crypto_aggregation.sql  # 5s tumbling → ClickHouse crypto_by_minute
│   │   ├── crypto_indicators.sql   # 1m SMA via HOP window → Kafka
│   │   └── crypto_alerts.sql       # Whale + 3-drop CEP → Kafka
│   ├── indicator_consumer.py       # crypto-indicators → Redis hash
│   ├── alert_consumer.py           # crypto-alerts → Redis Pub/Sub
│   ├── redis_sink_crypto.py        # ClickHouse crypto_by_minute → Redis (prices/vol/trades)
│   └── Dockerfile
├── ml-service/
│   ├── predictor.py                # Momentum signal (NOT ML — see below)
│   ├── requirements.txt
│   └── Dockerfile
├── web-dashboard/
│   ├── backend/
│   │   ├── app.py                  # FastAPI: /api/data, /api/history, SSE
│   │   └── requirements.txt
│   ├── frontend/
│   │   ├── index.html
│   │   ├── app.js
│   │   ├── styles.css
│   │   └── lightweight-charts.js
│   └── Dockerfile
├── scripts/
│   ├── create-topics.sh            # Manual topic setup (usually not needed)
│   └── setup-pipeline.sh           # Connectors + ClickHouse table + Flink jobs
└── README.md
```

---

## Data schemas

### `crypto-trades` (Kafka, from producer)

```json
{
  "symbol": "BTCUSDT",
  "price": 65431.20,
  "quantity": 0.0123,
  "trade_time": 1721654591123,
  "is_buyer_maker": false
}
```

### `crypto-indicators` (Kafka, from Flink)

```json
{ "window_end": "2026-07-22 14:23:00", "symbol": "BTCUSDT", "sma_1m": 65420.15 }
```

### `crypto-alerts` (Kafka, from Flink)

```json
{
  "alert_time": "2026-07-22 14:23:11",
  "symbol": "BTCUSDT",
  "alert_type": "WHALE_ALERT",
  "message": "Whale spotted! 12.5 BTCUSDT traded at $65431.20"
}
```

Alert types: `WHALE_ALERT` (single trade > $50k), `DOWNTREND` (3 consecutive
price drops per symbol).

### Redis keys

| Key                     | Type | Content                                            |
| ----------------------- | ---- | -------------------------------------------------- |
| `crypto:avg_price`      | hash | `symbol → latest 5s avg_price`                     |
| `crypto:volume`         | hash | `symbol → cumulative USD volume`                   |
| `crypto:trades`         | hash | `symbol → cumulative trade count`                  |
| `crypto:sma_1m`         | hash | `symbol → 1-minute SMA`                            |
| `crypto:prediction`     | hash | `symbol → {"direction":"UPTREND","confidence":72}` |
| `crypto_alerts_channel` | pub/sub | alerts pushed for SSE fan-out                   |

---

## About the "Momentum Signal"

The `ml-service/` directory contains a **momentum indicator**, not a machine-learning model. The rule is deliberately simple:

```
window     = last 20 trade prices for symbol
change     = (mean(last 5) - mean(first 5)) / mean(first 5)

change >  0.001  → UPTREND
change < -0.001  → DOWNTREND
otherwise        → SIDEWAYS
```

The `confidence` number displayed in the UI (`|change| × 10000`, capped at 99)
is a rough visual proxy, not a calibrated probability. The service is called
"momentum-signal" in the codebase for accuracy; the folder is still named
`ml-service/` for compatibility with existing docker-compose service names and
Redis keys.

A real ML model (ARIMA, Prophet, LSTM, etc.) could drop straight into
`predictor.py` — the pipeline is already streaming per-symbol trade history to it.

---

## Windowing strategies used

| Type         | Config                   | Job                    | Use case                           |
| ------------ | ------------------------ | ---------------------- | ---------------------------------- |
| **Tumbling** | 5 s, no overlap          | `crypto_aggregation`   | Volume / trade count / avg price   |
| **Hopping**  | 1 min window, 5 s slide  | `crypto_indicators`    | 1-minute simple moving average     |
| **Ad-hoc**   | Per-record CEP           | `crypto_alerts`        | Whale trades, 3-consecutive-drops  |

Watermark: `event_time - 5 seconds` across all jobs (Binance events can arrive
slightly out of order).

---

## Known limitations & roadmap

- ~~Frontend polls `/api/data` every 1 s.~~ ✅ Replaced with SSE
  (`/api/data/stream`, 2 s cadence) — matches the underlying 5 s Flink window.
- ~~All ~450 USDT pairs on a single WebSocket connection.~~ ✅ Now sharded
  across `NUM_SHARDS` connections (default 2), each with independent
  exponential-backoff reconnect and a shared health tracker.
- **Momentum signal is a heuristic, not ML.** Replace `predict_trend()` in
  `ml-service/predictor.py` to plug in a real model.
- **No CI pipeline yet.** Tests exist locally (`pytest`) but nothing wires them
  into GitHub Actions on push.

---

## License

MIT