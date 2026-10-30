"""
Unit tests for EventValidator and DeadLetterEnvelope dispatch.
"""

import copy
from datetime import datetime, timedelta, timezone
import json
import pytest

from consumer.schemas import DeadLetterErrorType, SensorReading
from consumer.validation import EventValidator


def test_valid_event_passes(valid_reading_payload):
    validator = EventValidator()
    raw = json.dumps(valid_reading_payload).encode("utf-8")
    reading, dlq = validator.validate(raw, partition=1, offset=100)

    assert dlq is None
    assert reading is not None
    assert isinstance(reading, SensorReading)
    assert reading.device_id == "DEV-0001"
    assert reading.temperature_c == 5.2


def test_corrupt_json_produces_dlq():
    validator = EventValidator()
    raw = b"{invalid_json: true,,"
    reading, dlq = validator.validate(raw, partition=0, offset=42)

    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.CORRUPT_JSON.value
    assert "JSON parse error" in dlq.error_detail
    assert dlq.partition == 0
    assert dlq.offset == 42


def test_missing_identifier_fields(valid_reading_payload):
    validator = EventValidator()

    # Missing device_id
    payload = copy.deepcopy(valid_reading_payload)
    del payload["device_id"]
    raw = json.dumps(payload).encode("utf-8")
    reading, dlq = validator.validate(raw)
    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.MISSING_FIELD.value

    # Missing shipment_id
    payload = copy.deepcopy(valid_reading_payload)
    del payload["shipment_id"]
    raw = json.dumps(payload).encode("utf-8")
    reading, dlq = validator.validate(raw)
    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.MISSING_FIELD.value


def test_unknown_cargo_type(valid_reading_payload):
    validator = EventValidator()
    payload = copy.deepcopy(valid_reading_payload)
    payload["cargo_type"] = "HAZARDOUS_CHEMICALS"
    raw = json.dumps(payload).encode("utf-8")
    reading, dlq = validator.validate(raw)

    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.UNKNOWN_CARGO_TYPE.value
    assert "Unknown or invalid cargo_type" in dlq.error_detail


def test_null_temperature_fails_schema(valid_reading_payload):
    validator = EventValidator()
    payload = copy.deepcopy(valid_reading_payload)
    payload["temperature_c"] = None
    raw = json.dumps(payload).encode("utf-8")
    reading, dlq = validator.validate(raw)

    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.SCHEMA_VALIDATION_ERROR.value


def test_temperature_out_of_range_produces_invalid_range(valid_reading_payload):
    validator = EventValidator()

    # Sentinel value 999.9
    payload_high = copy.deepcopy(valid_reading_payload)
    payload_high["temperature_c"] = 999.9
    reading, dlq = validator.validate(json.dumps(payload_high).encode("utf-8"))
    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.INVALID_RANGE.value
    assert "999.9" in dlq.error_detail

    # Extreme sub-zero -85 C
    payload_low = copy.deepcopy(valid_reading_payload)
    payload_low["temperature_c"] = -85.0
    reading, dlq = validator.validate(json.dumps(payload_low).encode("utf-8"))
    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.INVALID_RANGE.value


def test_wrong_type_fails_schema(valid_reading_payload):
    validator = EventValidator()
    payload = copy.deepcopy(valid_reading_payload)
    payload["humidity_pct"] = "VERY_HUMID"
    raw = json.dumps(payload).encode("utf-8")
    reading, dlq = validator.validate(raw)

    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.SCHEMA_VALIDATION_ERROR.value


def test_future_timestamp_rejected(valid_reading_payload):
    # Anchor current time to 2026-05-01T12:00:00Z
    frozen_now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    validator = EventValidator(now_fn=lambda: frozen_now, max_future_seconds=300.0)

    # 10 minutes in the future (> 5 min threshold)
    payload = copy.deepcopy(valid_reading_payload)
    future_dt = frozen_now + timedelta(minutes=10)
    payload["timestamp"] = future_dt.isoformat()

    reading, dlq = validator.validate(json.dumps(payload).encode("utf-8"))
    assert reading is None
    assert dlq is not None
    assert dlq.error_type == DeadLetterErrorType.FUTURE_TIMESTAMP.value
    assert "in the future" in dlq.error_detail


def test_mild_clock_skew_under_5_min_accepted(valid_reading_payload):
    frozen_now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    validator = EventValidator(now_fn=lambda: frozen_now, max_future_seconds=300.0)

    # 2 minutes in the future (within acceptable 5-min threshold)
    payload = copy.deepcopy(valid_reading_payload)
    future_dt = frozen_now + timedelta(minutes=2)
    payload["timestamp"] = future_dt.isoformat()

    reading, dlq = validator.validate(json.dumps(payload).encode("utf-8"))
    assert reading is not None
    assert dlq is None
