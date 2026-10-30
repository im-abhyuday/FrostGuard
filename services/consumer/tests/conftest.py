"""
Pytest fixtures for consumer service tests.
"""

from datetime import datetime, timezone
import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from coldchain_common.db_models import Base
from consumer.config import ConsumerSettings
from consumer.repository import PostgresRepository
from consumer.schemas import CargoType, CompressorStatus, SensorReading


@pytest.fixture
def valid_reading_payload():
    return {
        "schema_version": "1.0",
        "event_id": "c1f7a0b5-7761-4fa2-bf5a-96e00cf748d1",
        "shipment_id": "SHP-0001-0001",
        "device_id": "DEV-0001",
        "timestamp": "2026-05-01T12:00:00Z",
        "temperature_c": 5.2,
        "humidity_pct": 52.0,
        "door_open": False,
        "compressor_on": True,
        "compressor_status": "OK",
        "ambient_temp_c": 32.5,
        "latitude": 19.0760,
        "longitude": 72.8777,
        "speed_kmph": 58.5,
        "cargo_type": "VACCINE",
        "setpoint_c": 5.0,
        "battery_pct": 98.0,
    }


@pytest.fixture
def valid_sensor_reading(valid_reading_payload):
    return SensorReading.model_validate(valid_reading_payload)


@pytest.fixture
def consumer_settings():
    return ConsumerSettings(
        KAFKA_BOOTSTRAP="localhost:9092",
        KAFKA_TOPIC="sensor.readings",
        KAFKA_DLQ_TOPIC="sensor.dlq",
        KAFKA_GROUP_ID="test-group",
        BATCH_SIZE=10,
        BATCH_MAX_WAIT_MS=100,
        REDIS_URL="redis://localhost:6379/15",
        POSTGRES_DSN="sqlite:///:memory:",
        WINDOW_MINUTES=60,
        MAX_RETRIES=2,
    )


@pytest.fixture
def sqlite_engine():
    """In-memory SQLite database engine for fast local unit testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def sqlite_repo(consumer_settings, sqlite_engine):
    return PostgresRepository(settings=consumer_settings, engine=sqlite_engine)
