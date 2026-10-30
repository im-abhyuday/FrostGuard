"""
SQLAlchemy 2.0 database models for ColdChainIQ.
Shared across API, ingestion consumer, and migration engines.
"""

import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON


class Base(DeclarativeBase):
    """Base declarative class for SQLAlchemy 2.0 ORM."""
    pass


# Use JSONB for PostgreSQL, fallback to standard JSON for SQLite/testing
JsonType = JSON().with_variant(JSONB(), "postgresql")


class Shipment(Base):
    """Shipment tracking entity defining cargo requirements and safe thermal boundaries."""
    __tablename__ = "shipments"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    cargo_type: Mapped[str] = mapped_column(String(32), nullable=False)
    origin: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    destination: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    route_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    setpoint_c: Mapped[float] = mapped_column(Float, nullable=False)
    safe_min_c: Mapped[float] = mapped_column(Float, nullable=False)
    safe_max_c: Mapped[float] = mapped_column(Float, nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="IN_TRANSIT", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    devices: Mapped[list["Device"]] = relationship("Device", back_populates="shipment")
    readings: Mapped[list["Reading"]] = relationship("Reading", back_populates="shipment")
    alerts: Mapped[list["Alert"]] = relationship("Alert", back_populates="shipment")


class Device(Base):
    """Telemetry sensor hardware identity entity."""
    __tablename__ = "devices"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    shipment_id: Mapped[Optional[str]] = mapped_column(
        String(64), ForeignKey("shipments.id", ondelete="SET NULL"), nullable=True
    )
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    shipment: Mapped[Optional["Shipment"]] = relationship("Shipment", back_populates="devices")
    readings: Mapped[list["Reading"]] = relationship("Reading", back_populates="device")
    alerts: Mapped[list["Alert"]] = relationship("Alert", back_populates="device")


class Reading(Base):
    """
    Immutable time-series telemetry sensor reading.
    Natural uniqueness constraint on (device_id, ts) ensures idempotent stream ingestion.
    """
    __tablename__ = "readings"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    device_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=False
    )
    shipment_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("shipments.id", ondelete="CASCADE"), nullable=False
    )
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    temperature_c: Mapped[float] = mapped_column(Float, nullable=False)
    humidity_pct: Mapped[float] = mapped_column(Float, nullable=False)
    door_open: Mapped[bool] = mapped_column(Boolean, nullable=False)
    compressor_on: Mapped[bool] = mapped_column(Boolean, nullable=False)
    compressor_status: Mapped[str] = mapped_column(String(32), nullable=False)
    ambient_temp_c: Mapped[float] = mapped_column(Float, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    speed_kmph: Mapped[float] = mapped_column(Float, nullable=False)
    setpoint_c: Mapped[float] = mapped_column(Float, nullable=False)
    battery_pct: Mapped[float] = mapped_column(Float, nullable=False)
    cargo_type: Mapped[str] = mapped_column(String(32), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    event_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)

    # Relationships
    device: Mapped["Device"] = relationship("Device", back_populates="readings")
    shipment: Mapped["Shipment"] = relationship("Shipment", back_populates="readings")

    __table_args__ = (
        UniqueConstraint("device_id", "ts", name="uq_readings_device_ts"),
        Index("ix_readings_shipment_ts_desc", "shipment_id", ts.desc()),
        Index("ix_readings_device_ts_desc", "device_id", ts.desc()),
        Index("ix_readings_ts_brin", "ts", postgresql_using="brin"),
    )


class Alert(Base):
    """Predictive and threshold violation alerts generated by ML and rules engine (Phases 3-5)."""
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    shipment_id: Mapped[Optional[str]] = mapped_column(
        String(64), ForeignKey("shipments.id", ondelete="CASCADE"), nullable=True
    )
    device_id: Mapped[Optional[str]] = mapped_column(
        String(64), ForeignKey("devices.id", ondelete="CASCADE"), nullable=True
    )
    severity: Mapped[str] = mapped_column(String(32), nullable=False)  # INFO, WARNING, CRITICAL
    risk_score: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    status: Mapped[str] = mapped_column(String(32), default="OPEN", nullable=False)  # OPEN, ACKNOWLEDGED, RESOLVED
    acknowledged_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    reasons: Mapped[Optional[Dict[str, Any]]] = mapped_column(JsonType, nullable=True)

    # Relationships
    shipment: Mapped[Optional["Shipment"]] = relationship("Shipment", back_populates="alerts")
    device: Mapped[Optional["Device"]] = relationship("Device", back_populates="alerts")
