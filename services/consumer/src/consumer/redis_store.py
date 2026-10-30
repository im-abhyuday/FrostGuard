"""
Redis store managing latest device state (HASH) and rolling telemetry window (ZSET).
Implements atomic compare-by-timestamp via Lua scripts and pipeline batching.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import redis
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import ConsumerSettings
from .schemas import SensorReading

logger = logging.getLogger("coldchainiq.consumer.redis")

# Atomic Lua Script: Only updates device:{id}:latest if new_timestamp_epoch > existing_timestamp_epoch
LUA_UPDATE_LATEST = """
local key = KEYS[1]
local new_iso = ARGV[1]
local new_epoch = tonumber(ARGV[2])
local updated_at = ARGV[3]
local ttl = tonumber(ARGV[4])

local existing_epoch = redis.call('HGET', key, 'timestamp_epoch')

if not existing_epoch or new_epoch > tonumber(existing_epoch) then
    for i = 5, #ARGV, 2 do
        redis.call('HSET', key, ARGV[i], ARGV[i+1])
    end
    redis.call('HSET', key, 'timestamp', new_iso, 'timestamp_epoch', tostring(new_epoch), 'updated_at', updated_at)
    redis.call('EXPIRE', key, ttl)
    return 1
else
    return 0
end
"""


def parse_timestamp_to_epoch(ts_str: str) -> float:
    """Converts ISO-8601 UTC timestamp string to epoch seconds."""
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return dt.timestamp()


def reading_to_window_member(reading: SensorReading) -> str:
    """
    Serializes essential feature fields into compact, deterministic JSON member.
    Includes 'ts' so identical duplicate events map to identical ZSET member (idempotent ZADD).
    """
    data = {
        "ts": reading.timestamp,
        "temperature_c": reading.temperature_c,
        "humidity_pct": reading.humidity_pct,
        "door_open": reading.door_open,
        "compressor_on": reading.compressor_on,
        "compressor_status": reading.compressor_status.value,
        "ambient_temp_c": reading.ambient_temp_c,
        "speed_kmph": reading.speed_kmph,
        "setpoint_c": reading.setpoint_c,
    }
    return json.dumps(data, separators=(",", ":"), sort_keys=True)


class RedisStore:
    """
    High-performance caching and rolling feature store for cold chain devices.
    """

    def __init__(self, settings: ConsumerSettings, redis_client: Optional[redis.Redis] = None):
        self.settings = settings
        if redis_client is not None:
            self.client = redis_client
        else:
            self.client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)

        # Register Lua script
        self._lua_update_latest = self.client.register_script(LUA_UPDATE_LATEST)
        self.window_seconds = settings.WINDOW_MINUTES * 60
        self.latest_ttl = settings.LATEST_TTL_SECONDS
        self.window_ttl = settings.LATEST_TTL_SECONDS

    def update_latest_state(self, reading: SensorReading) -> bool:
        """
        Conditionally updates device latest state hash using atomic Lua comparison.
        Returns True if state was updated, False if incoming reading is older than stored.
        """
        epoch = parse_timestamp_to_epoch(reading.timestamp)
        key = f"device:{reading.device_id}:latest"
        now_iso = datetime.now(timezone.utc).isoformat()

        # Flatten reading fields for HSET
        flat_fields = [
            "device_id", reading.device_id,
            "shipment_id", reading.shipment_id,
            "temperature_c", str(reading.temperature_c),
            "humidity_pct", str(reading.humidity_pct),
            "door_open", "1" if reading.door_open else "0",
            "compressor_on", "1" if reading.compressor_on else "0",
            "compressor_status", reading.compressor_status.value,
            "ambient_temp_c", str(reading.ambient_temp_c),
            "latitude", str(reading.latitude),
            "longitude", str(reading.longitude),
            "speed_kmph", str(reading.speed_kmph),
            "cargo_type", reading.cargo_type.value,
            "setpoint_c", str(reading.setpoint_c),
            "battery_pct", str(reading.battery_pct),
        ]

        args = [reading.timestamp, str(epoch), now_iso, str(self.latest_ttl)] + flat_fields
        result = self._lua_update_latest(keys=[key], args=args)
        return bool(result == 1)

    def write_batch(
        self, readings: List[SensorReading]
    ) -> Tuple[int, int]:
        """
        Executes pipelined writes for latest state and rolling window across a micro-batch.
        Returns (latest_updated_count, late_dropped_from_window_count).
        """
        if not readings:
            return 0, 0

        latest_updated = 0
        late_dropped = 0

        # Step 1: Execute latest updates via Lua
        for r in readings:
            if self.update_latest_state(r):
                latest_updated += 1

        # Step 2: Query latest known epoch per device to enforce rolling window horizon
        devices = {r.device_id for r in readings}
        latest_epochs: Dict[str, float] = {}

        pipe = self.client.pipeline()
        for dev in devices:
            pipe.hget(f"device:{dev}:latest", "timestamp_epoch")
        stored_epochs = pipe.execute()

        for dev, ep in zip(devices, stored_epochs):
            if ep:
                try:
                    latest_epochs[dev] = float(ep)
                except (ValueError, TypeError):
                    latest_epochs[dev] = 0.0
            else:
                latest_epochs[dev] = 0.0

        # Step 3: Pipelined ZSET window insertions and trims
        win_pipe = self.client.pipeline()
        trim_candidates: Dict[str, float] = {}

        for r in readings:
            epoch = parse_timestamp_to_epoch(r.timestamp)
            max_known = max(latest_epochs.get(r.device_id, 0.0), epoch)
            latest_epochs[r.device_id] = max_known

            # Drop event from rolling window if older than 60 mins behind device's latest timestamp
            cutoff = max_known - self.window_seconds
            if epoch < cutoff:
                late_dropped += 1
                continue

            win_key = f"device:{r.device_id}:window"
            member = reading_to_window_member(r)
            win_pipe.zadd(win_key, {member: epoch})
            win_pipe.expire(win_key, self.window_ttl)

            # Record latest cutoff for trim
            trim_candidates[win_key] = cutoff

        # Trim expired members
        for win_key, cutoff in trim_candidates.items():
            win_pipe.zremrangebyscore(win_key, "-inf", f"({cutoff}")

        win_pipe.execute()
        return latest_updated, late_dropped

    def get_window(self, device_id: str, minutes: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retrieves telemetry records from rolling window sorted chronologically.
        Used by Phase 5 real-time feature computation.
        """
        win_key = f"device:{device_id}:window"
        win_mins = minutes or self.settings.WINDOW_MINUTES
        win_secs = win_mins * 60

        # Look up device latest timestamp epoch
        latest_epoch_str = self.client.hget(f"device:{device_id}:latest", "timestamp_epoch")
        if not latest_epoch_str:
            # Fallback to querying all members
            members = self.client.zrange(win_key, 0, -1)
        else:
            latest_epoch = float(latest_epoch_str)
            min_score = latest_epoch - win_secs
            members = self.client.zrangebyscore(win_key, min_score, "+inf")

        records = []
        for m in members:
            try:
                records.append(json.loads(m))
            except Exception as e:
                logger.warning(f"Error parsing window member JSON: {e}")

        # Ensure sorted by timestamp
        records.sort(key=lambda x: x.get("ts", ""))
        return records

    def get_latest(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the latest state dict for a device.
        Used by API and alerting services.
        """
        key = f"device:{device_id}:latest"
        data = self.client.hgetall(key)
        if not data:
            return None

        # Convert numeric fields
        parsed: Dict[str, Any] = dict(data)
        for num_field in ["temperature_c", "humidity_pct", "ambient_temp_c", "latitude", "longitude", "speed_kmph", "setpoint_c", "battery_pct"]:
            if num_field in parsed:
                try:
                    parsed[num_field] = float(parsed[num_field])
                except (ValueError, TypeError):
                    pass

        for bool_field in ["door_open", "compressor_on"]:
            if bool_field in parsed:
                parsed[bool_field] = parsed[bool_field] == "1"

        return parsed

    def write_heartbeat(self, consumer_id: str = "ingest-1") -> None:
        """Writes consumer heartbeat key in Redis with 15s TTL."""
        self.client.set(
            "consumer:heartbeat",
            json.dumps({"consumer_id": consumer_id, "timestamp": datetime.now(timezone.utc).isoformat()}),
            ex=15,
        )

    def ping(self) -> bool:
        """Health check probe."""
        try:
            return bool(self.client.ping())
        except Exception:
            return False


def rebuild_redis_from_postgres(
    session: Session,
    redis_store: RedisStore,
    minutes: int = 60,
) -> int:
    """
    Rebuilds Redis latest states and rolling windows from PostgreSQL readings table.
    Ensures disaster recovery capability if Redis cache is lost or restarted.
    """
    from coldchain_common.db_models import Reading
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    stmt = (
        select(Reading)
        .where(Reading.ts >= cutoff)
        .order_by(Reading.ts.asc())
    )
    rows = session.scalars(stmt).all()
    logger.info(f"Rebuilding Redis from Postgres: found {len(rows)} readings within last {minutes}m")

    readings_to_rebuild = []
    for row in rows:
        r = SensorReading(
            schema_version="1.0",
            event_id=str(row.event_id) if row.event_id else "rebuilt",
            shipment_id=row.shipment_id,
            device_id=row.device_id,
            timestamp=row.ts.isoformat(),
            temperature_c=row.temperature_c,
            humidity_pct=row.humidity_pct,
            door_open=row.door_open,
            compressor_on=row.compressor_on,
            compressor_status=row.compressor_status,
            ambient_temp_c=row.ambient_temp_c,
            latitude=row.latitude,
            longitude=row.longitude,
            speed_kmph=row.speed_kmph,
            cargo_type=row.cargo_type,
            setpoint_c=row.setpoint_c,
            battery_pct=row.battery_pct,
        )
        readings_to_rebuild.append(r)

    redis_store.write_batch(readings_to_rebuild)
    return len(readings_to_rebuild)
