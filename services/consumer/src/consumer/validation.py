"""
Validation logic for incoming sensor telemetry events.
Applies schema parsing, Pydantic validation, and domain-level range/timeliness checks.
Dispatches invalid payloads to DLQ envelopes without throwing uncaught exceptions.
"""

from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Any, Callable, Dict, Optional, Tuple, Union
from pydantic import ValidationError

from .schemas import (
    CargoType,
    DeadLetterEnvelope,
    DeadLetterErrorType,
    SensorReading,
)

logger = logging.getLogger("coldchainiq.consumer.validation")


class EventValidator:
    """
    Validates raw Kafka messages against structural schema and domain constraints.
    """

    def __init__(
        self,
        consumer_version: str = "1.0.0",
        now_fn: Optional[Callable[[], datetime]] = None,
        max_future_seconds: float = 300.0,  # 5 minutes
        temp_min_c: float = -60.0,
        temp_max_c: float = 60.0,
    ):
        self.consumer_version = consumer_version
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self.max_future_seconds = max_future_seconds
        self.temp_min_c = temp_min_c
        self.temp_max_c = temp_max_c

    def validate(
        self,
        raw_payload: Union[bytes, str],
        partition: int = 0,
        offset: int = 0,
        source_topic: str = "sensor.readings",
    ) -> Tuple[Optional[SensorReading], Optional[DeadLetterEnvelope]]:
        """
        Parses and validates a message.
        Returns (reading, None) on success, or (None, dead_letter) on failure.
        Never raises exceptions.
        """
        raw_str = (
            raw_payload.decode("utf-8", errors="replace")
            if isinstance(raw_payload, bytes)
            else str(raw_payload)
        )

        # 1. Parse JSON
        try:
            data = json.loads(raw_str)
        except Exception as e:
            logger.warning(f"Corrupt JSON at partition={partition} offset={offset}: {e}")
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.CORRUPT_JSON.value,
                error_detail=f"JSON parse error: {str(e)}",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )

        if not isinstance(data, dict):
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.SCHEMA_VALIDATION_ERROR.value,
                error_detail="Root JSON entity is not an object/dict",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )

        # 2. Check required identifier fields
        dev_id = data.get("device_id")
        shp_id = data.get("shipment_id")
        if not dev_id or not shp_id:
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.MISSING_FIELD.value,
                error_detail="Missing or null required field 'device_id' or 'shipment_id'",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )

        # 3. Check cargo_type known enumeration
        raw_cargo = data.get("cargo_type")
        valid_cargos = {c.value for c in CargoType}
        if raw_cargo not in valid_cargos:
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.UNKNOWN_CARGO_TYPE.value,
                error_detail=f"Unknown or invalid cargo_type '{raw_cargo}'. Expected one of {sorted(valid_cargos)}",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )

        # 4. Pydantic structural validation
        try:
            reading = SensorReading.model_validate(data)
        except ValidationError as e:
            # Check if failure is due to sentinel/range before generic schema error
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.SCHEMA_VALIDATION_ERROR.value,
                error_detail=f"Pydantic validation failed: {e.errors()}",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )
        except Exception as e:
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.SCHEMA_VALIDATION_ERROR.value,
                error_detail=f"Unexpected parsing exception: {str(e)}",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )

        # 5. Domain-level physical range checks (e.g. sentinel value 999, negative Kelvin)
        if not (self.temp_min_c <= reading.temperature_c <= self.temp_max_c):
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.INVALID_RANGE.value,
                error_detail=(
                    f"Temperature {reading.temperature_c} C out of physical bounds "
                    f"[{self.temp_min_c}, {self.temp_max_c}] C"
                ),
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )

        if not (0.0 <= reading.humidity_pct <= 100.0):
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.INVALID_RANGE.value,
                error_detail=f"Humidity {reading.humidity_pct}% out of bounds [0.0, 100.0]",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )

        # 6. Timestamp timeliness and future-drift check
        try:
            ts_clean = reading.timestamp.replace("Z", "+00:00")
            event_dt = datetime.fromisoformat(ts_clean)
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)
        except Exception as e:
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.SCHEMA_VALIDATION_ERROR.value,
                error_detail=f"Unparseable ISO-8601 timestamp '{reading.timestamp}': {e}",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=self.now_fn().isoformat(),
                consumer_version=self.consumer_version,
            )

        now_dt = self.now_fn()
        max_allowed_dt = now_dt + timedelta(seconds=self.max_future_seconds)
        if event_dt > max_allowed_dt:
            drift_secs = (event_dt - now_dt).total_seconds()
            return None, DeadLetterEnvelope(
                original_payload=raw_str,
                error_type=DeadLetterErrorType.FUTURE_TIMESTAMP.value,
                error_detail=f"Timestamp {reading.timestamp} is {drift_secs:.1f}s in the future (max allowed: {self.max_future_seconds}s)",
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                failed_at=now_dt.isoformat(),
                consumer_version=self.consumer_version,
            )

        return reading, None
