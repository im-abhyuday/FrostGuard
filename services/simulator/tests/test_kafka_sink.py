"""
Unit tests for KafkaSink using a mock producer.
Verifies topic routing, key partitioning by device_id, and JSON serialization.
"""

import json
from unittest.mock import MagicMock
from simulator.models import CargoType, CompressorStatus, SensorReading
from simulator.sinks import KafkaSink


class MockKafkaProducer:
    """Mock implementation of confluent_kafka.Producer."""

    def __init__(self):
        self.produced_messages = []

    def produce(self, topic, key, value, on_delivery=None):
        self.produced_messages.append({
            "topic": topic,
            "key": key,
            "value": value,
            "on_delivery": on_delivery,
        })

    def poll(self, timeout):
        pass

    def flush(self, timeout=None):
        pass


def test_kafka_sink_keying_and_json_payload():
    mock_producer = MockKafkaProducer()
    sink = KafkaSink(
        bootstrap_servers="mock:9092",
        topic="sensor.readings",
        producer_instance=mock_producer,
    )

    reading = SensorReading(
        schema_version="1.0",
        event_id="e4b6c310-184f-4d32-8418-4a92358fb81e",
        shipment_id="SHP-000101",
        device_id="DEV-0007",
        timestamp="2026-05-01T14:30:00Z",
        temperature_c=4.5,
        humidity_pct=62.0,
        door_open=False,
        compressor_on=True,
        compressor_status=CompressorStatus.OK,
        ambient_temp_c=36.0,
        latitude=19.0760,
        longitude=72.8777,
        speed_kmph=58.0,
        cargo_type=CargoType.VACCINE,
        setpoint_c=5.0,
        battery_pct=96.0,
    )

    sink.write(reading)
    sink.flush()

    assert len(mock_producer.produced_messages) == 1
    msg = mock_producer.produced_messages[0]

    assert msg["topic"] == "sensor.readings"
    # Strict partition affinity check: message key MUST be device_id
    assert msg["key"] == b"DEV-0007"

    # Value MUST be valid UTF-8 JSON matching SensorReading contract
    payload_dict = json.loads(msg["value"].decode("utf-8"))
    assert payload_dict["device_id"] == "DEV-0007"
    assert payload_dict["cargo_type"] == "VACCINE"
    assert payload_dict["temperature_c"] == 4.5
    assert payload_dict["schema_version"] == "1.0"
