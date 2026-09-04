"""API tests for the FastAPI dashboard backend.

Uses fakeredis to stand in for a live Redis and monkeypatches ClickHouse
so tests don't touch the network.
"""
import json

import fakeredis
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch):
    """Fresh FastAPI TestClient with fakeredis + a stubbed ClickHouse client."""
    # Substitute redis.Redis BEFORE importing the app, so the module-level
    # `r = redis.Redis(...)` call gets our fake.
    import redis as redis_lib
    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    monkeypatch.setattr(redis_lib, "Redis", lambda **kw: fake)

    # Stub clickhouse_connect so its get_client doesn't try to open a real socket.
    class _StubQueryResult:
        def __init__(self, rows=None):
            self.result_rows = rows or []

    class _StubCH:
        def __init__(self, *a, **kw):
            pass
        def ping(self):
            return True
        def query(self, query_str, parameters=None, **kw):
            return _StubQueryResult()

    import clickhouse_connect
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kw: _StubCH())

    # Re-import app cleanly to pick up the patches.
    import importlib, sys
    if "app" in sys.modules:
        del sys.modules["app"]
    import app as app_module

    with TestClient(app_module.app) as tc:
        # Also expose the fake redis to tests via the client for seeding data
        tc.fake_redis = fake
        yield tc


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------
def test_health_returns_ok_when_redis_available(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["redis"] is True


def test_health_returns_503_when_redis_down(client):
    # Simulate Redis outage by blowing up ping()
    def _boom(*_a, **_kw):
        import redis
        raise redis.RedisError("connection refused")
    client.fake_redis.ping = _boom
    resp = client.get("/health")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# /api/data
# ---------------------------------------------------------------------------
def test_api_data_empty_when_redis_empty(client):
    resp = client.get("/api/data")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert body["data"]["prices"] == {}
    assert body["data"]["volumes"] == {}
    # Contract: dashboard relies on all five keys being present, even if empty.
    for key in ("prices", "volumes", "trades", "smas", "predictions"):
        assert key in body["data"]


def test_api_data_returns_seeded_values(client):
    r = client.fake_redis
    r.hset("crypto:avg_price", "BTCUSDT", "65431.20")
    r.hset("crypto:avg_price", "ETHUSDT", "3421.50")
    r.hset("crypto:volume",    "BTCUSDT", "1500000.5")
    r.hset("crypto:trades",    "BTCUSDT", "1234")
    r.hset("crypto:sma_1m",    "BTCUSDT", "65420.15")
    r.hset("crypto:prediction","BTCUSDT",
           json.dumps({"direction": "UPTREND", "confidence": 72.5}))

    body = client.get("/api/data").json()
    d = body["data"]
    assert d["prices"]["BTCUSDT"] == 65431.20
    assert d["prices"]["ETHUSDT"] == 3421.50
    assert d["volumes"]["BTCUSDT"] == 1500000.5
    assert d["trades"]["BTCUSDT"] == 1234           # coerced from float-string to int
    assert d["smas"]["BTCUSDT"] == 65420.15
    assert d["predictions"]["BTCUSDT"] == {"direction": "UPTREND", "confidence": 72.5}


def test_api_data_returns_503_on_redis_error(client):
    def _boom(*_a, **_kw):
        import redis
        raise redis.RedisError("boom")
    client.fake_redis.hgetall = _boom
    resp = client.get("/api/data")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# /api/history
# ---------------------------------------------------------------------------
def test_api_history_shape_with_no_data(client):
    resp = client.get("/api/history?symbols=BTCUSDT,ETHUSDT")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    # Both requested symbols must appear in the response even with no CH data.
    assert set(body["data"].keys()) == {"BTCUSDT", "ETHUSDT"}
    assert body["data"]["BTCUSDT"] == []


def test_api_history_defaults_to_btcusdt(client):
    resp = client.get("/api/history")
    assert resp.status_code == 200
    assert "BTCUSDT" in resp.json()["data"]
