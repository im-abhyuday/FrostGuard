"""
Unit tests for idempotency, deduplication filter, and database conflict handling.
"""

from unittest.mock import MagicMock
import pytest
from sqlalchemy import func, select

from coldchain_common.db_models import Reading
from consumer.dedupe import DedupeFilter
from consumer.schemas import SensorReading


class FakeRedisClient:
    """In-memory Redis fake for fast isolated unit tests."""
    def __init__(self):
        self.store = set()

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store.add(key)
        return True

    def pipeline(self):
        return FakeRedisPipeline(self)


class FakeRedisPipeline:
    def __init__(self, parent: FakeRedisClient):
        self.parent = parent
        self.commands = []

    def set(self, key, value, nx=False, ex=None):
        self.commands.append((key, nx))
        return self

    def execute(self):
        results = []
        for key, nx in self.commands:
            if nx and key in self.parent.store:
                results.append(None)
            else:
                self.parent.store.add(key)
                results.append(True)
        self.commands = []
        return results


def test_redis_dedupe_filter_single_event(valid_sensor_reading):
    fake_redis = FakeRedisClient()
    dedupe = DedupeFilter(redis_client=fake_redis, ttl_seconds=3600)

    # First sight
    is_dupe_1 = dedupe.is_duplicate(valid_sensor_reading)
    assert is_dupe_1 is False

    # Second sight with identical device_id and timestamp
    is_dupe_2 = dedupe.is_duplicate(valid_sensor_reading)
    assert is_dupe_2 is True


def test_redis_dedupe_filter_batch(valid_sensor_reading):
    fake_redis = FakeRedisClient()
    dedupe = DedupeFilter(redis_client=fake_redis, ttl_seconds=3600)

    # Create reading with different timestamp
    reading_2 = valid_sensor_reading.model_copy(update={"timestamp": "2026-05-01T12:01:00Z"})

    # Batch containing duplicate of reading 1
    batch = [valid_sensor_reading, reading_2, valid_sensor_reading]
    unique, duplicates = dedupe.filter_batch(batch)

    assert len(unique) == 2
    assert len(duplicates) == 1
    assert duplicates[0] == valid_sensor_reading


def test_postgres_idempotent_insert_on_conflict(sqlite_repo, valid_sensor_reading):
    # Insert first reading
    inserted1, conflicted1 = sqlite_repo.insert_batch([valid_sensor_reading])
    assert inserted1 == 1
    assert conflicted1 == 0

    # Attempt to insert identical reading again (same device_id and timestamp)
    inserted2, conflicted2 = sqlite_repo.insert_batch([valid_sensor_reading])
    assert inserted2 == 0
    assert conflicted2 == 1

    # Verify database holds exactly 1 row
    with sqlite_repo.session_factory() as session:
        count = session.scalar(select(func.count(Reading.id)))
        assert count == 1
