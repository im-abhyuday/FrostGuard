"""
PostgreSQL data access layer for ColdChainIQ ingestion pipeline.
Utilizes sync SQLAlchemy 2.0 with psycopg v3 for high-throughput batch operations.
Implements idempotent upserts and automatic shipment/device dimension discovery.
"""

from datetime import datetime, timezone
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from coldchain_common.db_models import Base, Device, Reading, Shipment
from .config import ConsumerSettings
from .schemas import SensorReading, get_cargo_profile

logger = logging.getLogger("coldchainiq.consumer.repository")


class PostgresRepository:
    """
    Authoritative persistence layer for sensor telemetry.

    Architecture Note:
    Uses synchronous SQLAlchemy 2.0 with psycopg v3 rather than async.
    The consumer operates as a sequential micro-batch pipeline where CPU parsing
    and blocking database chunk inserts occur in a dedicated worker thread.
    Sync execution avoids async event-loop overhead and provides straightforward
    transaction boundary control.
    """

    def __init__(self, settings: ConsumerSettings, engine: Optional[Engine] = None):
        self.settings = settings
        if engine is not None:
            self.engine = engine
        else:
            dsn = settings.POSTGRES_DSN
            # Ensure proper psycopg3 dialect prefix
            if dsn.startswith("postgresql://"):
                dsn = dsn.replace("postgresql://", "postgresql+psycopg://", 1)
            elif dsn.startswith("postgres://"):
                dsn = dsn.replace("postgres://", "postgresql+psycopg://", 1)

            self.engine = create_engine(
                dsn,
                pool_size=settings.DB_POOL_SIZE,
                max_overflow=settings.DB_MAX_OVERFLOW,
                pool_pre_ping=True,
            )

        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.is_postgresql = "postgresql" in self.engine.dialect.name

    def ping(self) -> bool:
        """Lightweight database connectivity probe for health checks."""
        try:
            with self.engine.connect() as conn:
                conn.execute(select(1))
            return True
        except Exception as e:
            logger.debug(f"Postgres health ping failed: {e}")
            return False

    def insert_batch(
        self, readings: List[SensorReading]
    ) -> Tuple[int, int]:
        """
        Inserts a micro-batch of telemetry readings with automatic dimension upserts.
        Retries with exponential backoff and jitter on database transients.
        Returns (inserted_count, conflicted_count).
        """
        if not readings:
            return 0, 0

        # Delegate to retrying inner worker
        return self._insert_batch_with_retry(readings)

    def _insert_batch_with_retry(
        self, readings: List[SensorReading]
    ) -> Tuple[int, int]:
        """Decorated retry implementation."""
        retryer = retry(
            reraise=True,
            stop=stop_after_attempt(self.settings.MAX_RETRIES),
            wait=wait_exponential_jitter(
                multiplier=self.settings.RETRY_MIN_WAIT_SECONDS,
                max=self.settings.RETRY_MAX_WAIT_SECONDS,
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        return retryer(self._execute_batch_transaction)(readings)

    def _execute_batch_transaction(
        self, readings: List[SensorReading]
    ) -> Tuple[int, int]:
        """
        Executes atomic batch transaction:
        1. Upsert missing shipments
        2. Upsert devices (update last_seen_at using GREATEST)
        3. Insert readings with ON CONFLICT (device_id, ts) DO NOTHING
        """
        with self.session_factory() as session:
            with session.begin():
                # 1. Dimension Upsert: Shipments
                self._upsert_shipments(session, readings)

                # 2. Dimension Upsert: Devices
                self._upsert_devices(session, readings)

                # 3. Fact Table Batch Insert: Readings
                inserted, conflicted = self._insert_readings(session, readings)

            return inserted, conflicted

    def _upsert_shipments(self, session: Session, readings: List[SensorReading]) -> None:
        """Auto-discovers and registers shipment records on first sight."""
        unique_shipments: Dict[str, SensorReading] = {}
        for r in readings:
            if r.shipment_id not in unique_shipments:
                unique_shipments[r.shipment_id] = r

        for shp_id, r in unique_shipments.items():
            profile = get_cargo_profile(r.cargo_type)
            if self.is_postgresql:
                stmt = (
                    pg_insert(Shipment)
                    .values(
                        id=shp_id,
                        cargo_type=r.cargo_type.value,
                        setpoint_c=profile.setpoint_c,
                        safe_min_c=profile.safe_min_c,
                        safe_max_c=profile.safe_max_c,
                        status="IN_TRANSIT",
                    )
                    .on_conflict_do_nothing(index_elements=["id"])
                )
                session.execute(stmt)
            else:
                # SQLite fallback for test environments
                existing = session.get(Shipment, shp_id)
                if not existing:
                    session.add(
                        Shipment(
                            id=shp_id,
                            cargo_type=r.cargo_type.value,
                            setpoint_c=profile.setpoint_c,
                            safe_min_c=profile.safe_min_c,
                            safe_max_c=profile.safe_max_c,
                            status="IN_TRANSIT",
                        )
                    )

    def _upsert_devices(self, session: Session, readings: List[SensorReading]) -> None:
        """Upserts devices and advances last_seen_at using GREATEST(existing, new)."""
        device_events: Dict[str, Tuple[str, datetime]] = {}
        for r in readings:
            dt = datetime.fromisoformat(r.timestamp.replace("Z", "+00:00"))
            if r.device_id not in device_events or dt > device_events[r.device_id][1]:
                device_events[r.device_id] = (r.shipment_id, dt)

        for dev_id, (shp_id, dt) in device_events.items():
            if self.is_postgresql:
                stmt = (
                    pg_insert(Device)
                    .values(
                        id=dev_id,
                        shipment_id=shp_id,
                        first_seen_at=dt,
                        last_seen_at=dt,
                    )
                    .on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "shipment_id": shp_id,
                            "last_seen_at": func.greatest(Device.last_seen_at, dt),
                        },
                    )
                )
                session.execute(stmt)
            else:
                dev = session.get(Device, dev_id)
                if not dev:
                    session.add(Device(id=dev_id, shipment_id=shp_id, first_seen_at=dt, last_seen_at=dt))
                else:
                    dev.shipment_id = shp_id
                    prev = dev.last_seen_at
                    if prev is not None and prev.tzinfo is None:
                        prev = prev.replace(tzinfo=timezone.utc)
                    dev.last_seen_at = max(prev or dt, dt)

    def _insert_readings(
        self, session: Session, readings: List[SensorReading]
    ) -> Tuple[int, int]:
        """
        Executes bulk insert of telemetry readings with conflict resolution.
        """
        rows: List[Dict[str, Any]] = []
        for r in readings:
            dt = datetime.fromisoformat(r.timestamp.replace("Z", "+00:00"))
            ev_id = None
            try:
                ev_id = uuid.UUID(r.event_id)
            except Exception:
                pass

            rows.append(
                {
                    "device_id": r.device_id,
                    "shipment_id": r.shipment_id,
                    "ts": dt,
                    "temperature_c": r.temperature_c,
                    "humidity_pct": r.humidity_pct,
                    "door_open": r.door_open,
                    "compressor_on": r.compressor_on,
                    "compressor_status": r.compressor_status.value,
                    "ambient_temp_c": r.ambient_temp_c,
                    "latitude": r.latitude,
                    "longitude": r.longitude,
                    "speed_kmph": r.speed_kmph,
                    "setpoint_c": r.setpoint_c,
                    "battery_pct": r.battery_pct,
                    "cargo_type": r.cargo_type.value,
                    "event_id": ev_id,
                }
            )

        total_rows = len(rows)
        if self.is_postgresql:
            stmt = (
                pg_insert(Reading)
                .values(rows)
                .on_conflict_do_nothing(index_elements=["device_id", "ts"])
                .returning(Reading.id)
            )
            result = session.execute(stmt)
            inserted_count = len(result.fetchall())
            conflicted_count = total_rows - inserted_count
            return inserted_count, conflicted_count
        else:
            # SQLite fallback logic
            inserted_count = 0
            for row in rows:
                existing = session.execute(
                    select(Reading.id).where(
                        Reading.device_id == row["device_id"],
                        Reading.ts == row["ts"],
                    )
                ).first()
                if not existing:
                    session.add(Reading(**row))
                    inserted_count += 1
            conflicted_count = total_rows - inserted_count
            return inserted_count, conflicted_count
