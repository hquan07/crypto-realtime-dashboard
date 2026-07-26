# 🚀 Realtime Streaming Data Pipeline

> Real-time event processing pipeline for e-commerce purchase/click events with windowed aggregation, live dashboards, and fault tolerance.

## Architecture

```
Producer (Python) → Kafka → Apache Flink (Windowing) → Redis + Elasticsearch → Kibana Dashboard
```

| Layer | Tech | Purpose |
|---|---|---|
| Message Broker | Apache Kafka | High-throughput event buffer |
| Ingest | Python + Faker | Simulated purchase/click events |
| Stream Processing | Apache Flink (SQL + PyFlink) | Windowed aggregation with watermarks |
| Fast Store | Redis | Sub-millisecond counter reads |
| Analytics Store | Elasticsearch | Time-series storage & visualization |
| Dashboard | Kibana | Real-time monitoring UI |
| Infrastructure | Docker Compose | One-command local deployment |

## Quick Start

### Prerequisites
- Docker Engine ≥ 24.0
- Docker Compose ≥ 2.20
- 8 GB+ RAM recommended (Elasticsearch + Flink are memory-hungry)

### Start the pipeline

```bash
# Start all services
docker-compose up -d

# Check all services are healthy
docker-compose ps

# Follow producer logs
docker-compose logs -f producer
```

### Service URLs

| Service | URL |
|---|---|
| Kibana Dashboard | http://localhost:5601 |
| Flink Web UI | http://localhost:8081 |
| Elasticsearch | http://localhost:9200 |
| Redis CLI | `docker exec -it redis redis-cli` |

### Run load test

```bash
# Install dependencies locally
pip install confluent-kafka faker

# Run load test with default settings (100 → 1000 events/sec spike)
python scripts/load-test.py

# Custom spike: peak 2000 events/sec, longer duration
python scripts/load-test.py --peak-rate 2000 --spike-duration 120
```

### Stop everything

```bash
docker-compose down -v   # Remove volumes too
```

## Project Structure

```
realtime-streaming-pipeline/
├── docker-compose.yml              # All services
├── .env                            # Environment variables
├── producer/
│   ├── producer.py                 # Event generator
│   ├── requirements.txt
│   └── Dockerfile
├── stream-processing/
│   ├── flink-sql-jobs/
│   │   └── revenue_aggregation.sql # Flink SQL windowing job
│   └── pyflink-jobs/
│       └── windowed_revenue.py     # PyFlink equivalent
├── dashboard/
│   └── kibana-dashboard-export.ndjson
├── scripts/
│   ├── create-topics.sh            # Kafka topic setup
│   └── load-test.py                # Spike traffic simulation
└── README.md
```

## Event Schema

**Purchase Event:**
```json
{
  "event_id": "uuid",
  "event_type": "purchase",
  "user_id": "U1234",
  "product_id": "P5567",
  "product_name": "Wireless Mouse",
  "category": "Electronics",
  "quantity": 2,
  "unit_price": 15.99,
  "total_amount": 31.98,
  "currency": "USD",
  "timestamp": "2026-07-22T14:23:11.123Z",
  "session_id": "uuid"
}
```

## Windowing Strategies

| Type | Config | Use Case |
|---|---|---|
| **Tumbling** | 1 minute, no overlap | Revenue per minute per category |
| **Sliding** | 5 min window, 30s slide | Rolling 5-min average |
| **Session** | 5 min gap | Per-user session analysis |

## Performance Targets

- Producer: ≥ 100 events/sec sustained
- End-to-end latency: ≤ 5 seconds
- Load test: ≥ 500 events/sec without data loss
- Flink checkpointing: exactly-once semantics

## License

MIT
