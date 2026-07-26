"""Unit tests for pure logic inside producer/crypto_producer.py.

Kafka + WebSocket are not needed here — we only exercise the transform + shard
helpers. Importing crypto_producer creates a module-level Kafka Producer, but
confluent_kafka.Producer is lazy about connections so it won't actually reach
out to a broker on construction.
"""
import pytest

from crypto_producer import transform_trade, chunk


# ---------------------------------------------------------------------------
# transform_trade
# ---------------------------------------------------------------------------
def test_transform_trade_valid_message():
    msg = {
        "e": "trade", "s": "BTCUSDT", "p": "65431.20",
        "q": "0.0123", "T": 1721654591123, "m": False,
    }
    out = transform_trade(msg)
    assert out == {
        "symbol": "BTCUSDT", "price": 65431.20, "quantity": 0.0123,
        "trade_time": 1721654591123, "is_buyer_maker": False,
    }


def test_transform_trade_ignores_subscription_ack():
    # Binance sends {"result": null, "id": 1} on successful SUBSCRIBE
    assert transform_trade({"result": None, "id": 1}) is None


def test_transform_trade_ignores_wrong_event_type():
    # Any frame that isn't a trade event should be dropped silently.
    assert transform_trade({"e": "24hrTicker", "s": "BTCUSDT"}) is None
    assert transform_trade({"e": "kline"}) is None


def test_transform_trade_rejects_non_dict():
    assert transform_trade(None) is None
    assert transform_trade("some string") is None
    assert transform_trade([1, 2, 3]) is None


def test_transform_trade_handles_malformed_numbers():
    msg = {"e": "trade", "s": "BTCUSDT", "p": "not-a-number", "q": "1", "T": 1, "m": False}
    assert transform_trade(msg) is None


def test_transform_trade_missing_fields_default_to_zero():
    # Defensive: if Binance omits p/q we should still not crash.
    out = transform_trade({"e": "trade", "s": "ETHUSDT"})
    assert out["price"] == 0.0
    assert out["quantity"] == 0.0


# ---------------------------------------------------------------------------
# chunk (used for sharding symbols across WebSocket connections)
# ---------------------------------------------------------------------------
def test_chunk_splits_evenly():
    symbols = ["a", "b", "c", "d"]
    assert chunk(symbols, 2) == [["a", "b"], ["c", "d"]]


def test_chunk_handles_uneven_split():
    symbols = ["a", "b", "c", "d", "e"]
    result = chunk(symbols, 2)
    # Concatenation must be lossless and each shard non-empty
    assert sum(result, []) == symbols
    assert all(len(part) > 0 for part in result)


def test_chunk_returns_single_shard_when_n_is_one():
    assert chunk(["a", "b", "c"], 1) == [["a", "b", "c"]]


def test_chunk_with_realistic_binance_load():
    # ~450 active USDT pairs → 2 shards → each ≤ 225, total lossless.
    symbols = [f"sym{i}usdt" for i in range(450)]
    result = chunk(symbols, 2)
    assert sum(result, []) == symbols
    assert max(len(s) for s in result) <= 225
