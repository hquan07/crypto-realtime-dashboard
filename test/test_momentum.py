"""Unit tests for the momentum signal indicator (ml-service/predictor.py)."""
import pytest

from predictor import predict_trend, WINDOW_SIZE


def test_returns_none_when_window_not_full():
    # Anything shorter than WINDOW_SIZE must yield None (indicator not ready yet).
    assert predict_trend([100.0] * (WINDOW_SIZE - 1)) is None
    assert predict_trend([]) is None


def test_uptrend_detected_on_rising_prices():
    # Start at 100, end at 105 → +5% >> +0.1% threshold
    prices = [100.0 + i * 0.25 for i in range(WINDOW_SIZE)]
    result = predict_trend(prices)
    assert result is not None
    assert result["direction"] == "UPTREND"
    assert 0 < result["confidence"] <= 99.0


def test_downtrend_detected_on_falling_prices():
    prices = [100.0 - i * 0.25 for i in range(WINDOW_SIZE)]
    result = predict_trend(prices)
    assert result is not None
    assert result["direction"] == "DOWNTREND"
    assert 0 < result["confidence"] <= 99.0


def test_sideways_when_price_barely_moves():
    # < 0.1% change: falls into SIDEWAYS band
    prices = [100.0 + (0.001 if i % 2 else -0.001) for i in range(WINDOW_SIZE)]
    result = predict_trend(prices)
    assert result is not None
    assert result["direction"] == "SIDEWAYS"


def test_confidence_is_capped_at_99():
    # Extreme rally shouldn't produce absurd confidences (spec says cap at 99).
    prices = [100.0] * 5 + [200.0] * (WINDOW_SIZE - 5)
    result = predict_trend(prices)
    assert result is not None
    assert result["confidence"] <= 99.0


def test_handles_zero_old_mean_without_crashing():
    # Regression: original code did `(recent - old) / old` with no zero-guard.
    prices = [0.0] * 5 + [1.0] * (WINDOW_SIZE - 5)
    # Must return None instead of raising ZeroDivisionError / RuntimeWarning-crash.
    assert predict_trend(prices) is None


def test_result_shape_is_dashboard_compatible():
    """The dashboard reads exactly these two keys — protect them."""
    prices = [100.0 + i for i in range(WINDOW_SIZE)]
    result = predict_trend(prices)
    assert set(result.keys()) == {"direction", "confidence"}
    assert isinstance(result["confidence"], float)
    assert result["direction"] in {"UPTREND", "DOWNTREND", "SIDEWAYS"}
