"""
Schema and validation contract tests using Pydantic v2.
"""

from datetime import datetime
import pytest
from pydantic import ValidationError

from simulator.faults import SensorFaultInjector
from simulator.models import CargoType, CompressorStatus, SensorReading


def test_valid_reading_passes_schema():
    payload = {
        "schema_version": "1.0",
        "event_id": "c71a39f0-2f3b-4876-8095-23db03ffb612",
        "shipment_id": "SHP-000101",
        "device_id": "DEV-0001",
        "timestamp": "2026-05-01T12:00:00Z",
        "temperature_c": 4.85,
        "humidity_pct": 65.2,
        "door_open": False,
        "compressor_on": True,
        "compressor_status": "OK",
        "ambient_temp_c": 34.2,
        "latitude": 22.7196,
        "longitude": 75.8577,
        "speed_kmph": 62.4,
        "cargo_type": "VACCINE",
        "setpoint_c": 5.0,
        "battery_pct": 98.4,
    }
    reading = SensorReading(**payload)
    assert reading.schema_version == "1.0"
    assert reading.cargo_type == CargoType.VACCINE
    assert reading.compressor_status == CompressorStatus.OK


def test_corrupt_payload_fails_validation(rng):
    """
    Verifies that the fault injector's bad payload generator creates invalid events
    that trigger Pydantic ValidationError (used for DLQ testing in Phase 2).
    """
    fault_injector = SensorFaultInjector(rng=rng)
    base_reading = SensorReading(
        schema_version="1.0",
        event_id="3f0243bc-42b9-4a0b-93ff-15103a8ec49f",
        shipment_id="SHP-000101",
        device_id="DEV-0001",
        timestamp="2026-05-01T12:00:00Z",
        temperature_c=5.0,
        humidity_pct=60.0,
        door_open=False,
        compressor_on=True,
        compressor_status=CompressorStatus.OK,
        ambient_temp_c=30.0,
        latitude=20.0,
        longitude=75.0,
        speed_kmph=55.0,
        cargo_type=CargoType.VACCINE,
        setpoint_c=5.0,
        battery_pct=95.0,
    )

    # Force bad payload injection in live mode
    payloads = fault_injector.process(
        reading=base_reading,
        current_time=datetime(2026, 5, 1, 12, 0, 0),
        is_live_mode=True,
        bad_event_rate=1.0,
    )

    assert len(payloads) == 1
    bad_dict = payloads[0]
    assert isinstance(bad_dict, dict)

    # Attempting to construct SensorReading MUST raise ValidationError
    with pytest.raises(ValidationError):
        SensorReading(**bad_dict)
