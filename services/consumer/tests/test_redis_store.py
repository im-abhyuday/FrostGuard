"""
Unit tests for RedisStore latest state and rolling window management.
"""

from datetime import datetime, timezone
import json
import pytest

from consumer.config import ConsumerSettings
from consumer.redis_store import (
    RedisStore,
    parse_timestamp_to_epoch,
    reading_to_window_member,
)
from consumer.schemas import SensorReading


class FakeRedisForStore:
    """Mock Redis client simulating Hashes, Sorted Sets, and Script evaluation."""

    def __init__(self):
        self.strings = {}     # key -> str
        self.hashes = {}      # key -> dict
        self.zsets = {}       # key -> dict(member: score)
        self.ttls = {}        # key -> ttl
        self._scripts = {}

    def set(self, key: str, value: str, ex: int = None):
        self.strings[key] = str(value)
        if ex is not None:
            self.ttls[key] = ex
        return True

    def get(self, key: str):
        return self.strings.get(key)

    def register_script(self, script_str: str):
        def _execute(keys, args):
            key = keys[0]
            new_iso = args[0]
            new_epoch = float(args[1])
            updated_at = args[2]
            ttl = int(args[3])

            h = self.hashes.get(key, {})
            existing_epoch_str = h.get("timestamp_epoch")

            if existing_epoch_str is None or new_epoch > float(existing_epoch_str):
                # Update fields
                for i in range(4, len(args), 2):
                    h[args[i]] = args[i + 1]
                h["timestamp"] = new_iso
                h["timestamp_epoch"] = str(new_epoch)
                h["updated_at"] = updated_at
                self.hashes[key] = h
                self.ttls[key] = ttl
                return 1
            else:
                return 0

        return _execute

    def hget(self, key: str, field: str):
        h = self.hashes.get(key, {})
        return h.get(field)

    def hgetall(self, key: str):
        return self.hashes.get(key, {})

    def expire(self, key: str, ttl: int):
        self.ttls[key] = ttl
        return True

    def zadd(self, key: str, mapping: dict):
        z = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            z[member] = float(score)
        return len(mapping)

    def zrangebyscore(self, key: str, min_score, max_score):
        z = self.zsets.get(key, {})
        min_val = float("-inf") if min_score in ("-inf", "-infinity") else float(str(min_score).lstrip("("))
        max_val = float("inf") if max_score in ("+inf", "+infinity") else float(str(max_score).lstrip("("))

        filtered = [
            (m, s) for m, s in z.items()
            if min_val <= s <= max_val
        ]
        filtered.sort(key=lambda x: x[1])
        return [m for m, s in filtered]

    def zremrangebyscore(self, key: str, min_score, max_score):
        z = self.zsets.get(key, {})
        max_val = float(str(max_score).lstrip("("))
        to_remove = [m for m, s in z.items() if s < max_val]
        for m in to_remove:
            del z[m]
        return len(to_remove)

    def zrange(self, key: str, start: int, stop: int):
        z = self.zsets.get(key, {})
        sorted_members = sorted(z.items(), key=lambda x: x[1])
        return [m for m, s in sorted_members]

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, parent: FakeRedisForStore):
        self.parent = parent
        self.calls = []

    def hget(self, key: str, field: str):
        self.calls.append(("hget", key, field))
        return self

    def zadd(self, key: str, mapping: dict):
        self.calls.append(("zadd", key, mapping))
        return self

    def expire(self, key: str, ttl: int):
        self.calls.append(("expire", key, ttl))
        return self

    def zremrangebyscore(self, key: str, min_score, max_score):
        self.calls.append(("zremrangebyscore", key, min_score, max_score))
        return self

    def execute(self):
        results = []
        for call in self.calls:
            cmd = call[0]
            if cmd == "hget":
                results.append(self.parent.hget(call[1], call[2]))
            elif cmd == "zadd":
                results.append(self.parent.zadd(call[1], call[2]))
            elif cmd == "expire":
                results.append(self.parent.expire(call[1], call[2]))
            elif cmd == "zremrangebyscore":
                results.append(self.parent.zremrangebyscore(call[1], call[2], call[3]))
        self.calls = []
        return results


@pytest.fixture
def fake_redis_store(consumer_settings):
    fake_client = FakeRedisForStore()
    return RedisStore(settings=consumer_settings, redis_client=fake_client)


def test_atomic_lua_latest_state_newer_overwrites_older(fake_redis_store, valid_sensor_reading):
    # Event 1 at 12:00:00
    r1 = valid_sensor_reading.model_copy(update={"timestamp": "2026-05-01T12:00:00Z", "temperature_c": 5.0})
    updated1 = fake_redis_store.update_latest_state(r1)
    assert updated1 is True

    latest = fake_redis_store.get_latest("DEV-0001")
    assert latest["timestamp"] == "2026-05-01T12:00:00Z"
    assert latest["temperature_c"] == 5.0

    # Event 2 at 12:05:00 (newer -> should overwrite)
    r2 = valid_sensor_reading.model_copy(update={"timestamp": "2026-05-01T12:05:00Z", "temperature_c": 5.8})
    updated2 = fake_redis_store.update_latest_state(r2)
    assert updated2 is True

    latest = fake_redis_store.get_latest("DEV-0001")
    assert latest["timestamp"] == "2026-05-01T12:05:00Z"
    assert latest["temperature_c"] == 5.8

    # Event 3 arrived late at 12:02:00 (older than 12:05:00 -> must NOT overwrite)
    r3 = valid_sensor_reading.model_copy(update={"timestamp": "2026-05-01T12:02:00Z", "temperature_c": 6.2})
    updated3 = fake_redis_store.update_latest_state(r3)
    assert updated3 is False

    latest = fake_redis_store.get_latest("DEV-0001")
    assert latest["timestamp"] == "2026-05-01T12:05:00Z"
    assert latest["temperature_c"] == 5.8  # Unchanged!


def test_window_idempotency_member_uniqueness(valid_sensor_reading):
    # Same reading produces exact same member string
    m1 = reading_to_window_member(valid_sensor_reading)
    m2 = reading_to_window_member(valid_sensor_reading)
    assert m1 == m2

    data = json.loads(m1)
    assert data["ts"] == valid_sensor_reading.timestamp
    assert data["temperature_c"] == valid_sensor_reading.temperature_c


def test_window_trimming_and_late_drop(fake_redis_store, valid_sensor_reading):
    # Anchor device latest at 13:00:00
    r_current = valid_sensor_reading.model_copy(update={"timestamp": "2026-05-01T13:00:00Z"})
    fake_redis_store.write_batch([r_current])

    # Event within 60 min window (12:30:00 -> 30 mins old)
    r_within = valid_sensor_reading.model_copy(update={"timestamp": "2026-05-01T12:30:00Z"})

    # Event older than 60 min window (11:45:00 -> 75 mins old)
    r_stale = valid_sensor_reading.model_copy(update={"timestamp": "2026-05-01T11:45:00Z"})

    _, late_dropped = fake_redis_store.write_batch([r_within, r_stale])
    assert late_dropped == 1

    # Check window contents
    window = fake_redis_store.get_window("DEV-0001", minutes=60)
    timestamps = [item["ts"] for item in window]
    assert "2026-05-01T13:00:00Z" in timestamps
    assert "2026-05-01T12:30:00Z" in timestamps
    assert "2026-05-01T11:45:00Z" not in timestamps
