"""
Canonical Pydantic v2 schemas and cargo profiles for ColdChainIQ.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union
from pydantic import BaseModel, ConfigDict, Field

from .enums import (
    CargoType,
    CompressorStatus,
    DeadLetterErrorType,
    GroundTruthEventType,
    RootCauseTag,
)


@dataclass(frozen=True)
class CargoProfile:
    cargo_type: CargoType
    safe_min_c: float
    safe_max_c: float
    setpoint_c: float
    thermal_mass: float         # kJ/K equivalent: higher = slower temperature response
    cooling_power: float        # kW equivalent effective cooling capacity
    excursion_tolerance_mins: int  # Minutes outside safe range before critical violation
    humidity_nominal_pct: float
    precool_band_c: float       # Allowed variance for a valid pre-cooled container


CARGO_PROFILES: Dict[CargoType, CargoProfile] = {
    CargoType.VACCINE: CargoProfile(
        cargo_type=CargoType.VACCINE,
        safe_min_c=2.0,
        safe_max_c=8.0,
        setpoint_c=5.0,
        thermal_mass=1400.0,
        cooling_power=2.8,
        excursion_tolerance_mins=30,
        humidity_nominal_pct=50.0,
        precool_band_c=0.5,
    ),
    CargoType.FROZEN_FOOD: CargoProfile(
        cargo_type=CargoType.FROZEN_FOOD,
        safe_min_c=-25.0,
        safe_max_c=-18.0,
        setpoint_c=-21.0,
        thermal_mass=2800.0,
        cooling_power=4.5,
        excursion_tolerance_mins=60,
        humidity_nominal_pct=70.0,
        precool_band_c=1.5,
    ),
    CargoType.DAIRY: CargoProfile(
        cargo_type=CargoType.DAIRY,
        safe_min_c=1.0,
        safe_max_c=4.0,
        setpoint_c=2.5,
        thermal_mass=2100.0,
        cooling_power=3.2,
        excursion_tolerance_mins=45,
        humidity_nominal_pct=75.0,
        precool_band_c=0.8,
    ),
    CargoType.FRESH_PRODUCE: CargoProfile(
        cargo_type=CargoType.FRESH_PRODUCE,
        safe_min_c=4.0,
        safe_max_c=10.0,
        setpoint_c=7.0,
        thermal_mass=2500.0,
        cooling_power=3.0,
        excursion_tolerance_mins=120,
        humidity_nominal_pct=85.0,
        precool_band_c=1.0,
    ),
}


def get_cargo_profile(cargo_type: Union[CargoType, str]) -> CargoProfile:
    """Retrieves standard cargo profile by enum or string."""
    if isinstance(cargo_type, str):
        cargo_type = CargoType(cargo_type)
    return CARGO_PROFILES[cargo_type]


class SensorReading(BaseModel):
    """
    Sensor reading payload emitted by onboard telematics.
    Kafka key: device_id. Natural deduplication key: (device_id, timestamp).
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: str = Field(default="1.0", description="Semantic contract version")
    event_id: str = Field(..., description="Unique UUID per emission; not used for dedupe")
    shipment_id: str = Field(..., description="Shipment business identifier e.g. SHP-000123")
    device_id: str = Field(..., description="Hardware sensor device ID e.g. DEV-0007")
    timestamp: str = Field(..., description="ISO-8601 UTC timestamp of reported event time")
    temperature_c: float = Field(..., description="Reported internal container air temperature")
    humidity_pct: float = Field(..., ge=0.0, le=100.0, description="Relative humidity %")
    door_open: bool = Field(..., description="Physical reed switch door status")
    compressor_on: bool = Field(..., description="Thermostat commanded cooling status")
    compressor_status: CompressorStatus = Field(..., description="Compressor health state")
    ambient_temp_c: float = Field(..., description="External ambient atmospheric temperature")
    latitude: float = Field(..., ge=-90.0, le=90.0, description="WGS-84 Latitude")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="WGS-84 Longitude")
    speed_kmph: float = Field(..., ge=0.0, description="Ground vehicle speed in km/h")
    cargo_type: CargoType = Field(..., description="Cargo classification")
    setpoint_c: float = Field(..., description="Target thermostat setpoint temperature in C")
    battery_pct: float = Field(..., ge=0.0, le=100.0, description="Onboard telematics battery percentage")


class DeadLetterEnvelope(BaseModel):
    """
    Standard envelope published to topic `sensor.dlq` when validation fails.
    Preserves raw bytes/string, telemetry coordinates, error classification, and consumer metadata.
    """
    model_config = ConfigDict(frozen=True)

    original_payload: str = Field(..., description="Raw message payload string/bytes")
    error_type: str = Field(..., description="Classification category for DLQ routing and alerts")
    error_detail: str = Field(..., description="Specific parse or validation error message")
    source_topic: str = Field(default="sensor.readings", description="Origin Kafka topic")
    partition: int = Field(..., description="Source topic partition")
    offset: int = Field(..., description="Source message offset")
    failed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO-8601 UTC timestamp of rejection"
    )
    consumer_version: str = Field(default="1.0.0", description="Consumer service version")


class GroundTruthEvent(BaseModel):
    """
    Ground-truth log for ML validation, label generation, and attribution.
    Never streamed to live Kafka topics.
    """
    model_config = ConfigDict(frozen=True)

    shipment_id: str
    device_id: str
    event_type: GroundTruthEventType
    start_time: str
    end_time: str
    caused_excursion: bool
    excursion_start: Optional[str] = None
    excursion_end: Optional[str] = None
    root_cause_tag: RootCauseTag


class ShipmentMetadata(BaseModel):
    """
    Trip-level metadata record for auditability.
    """
    model_config = ConfigDict(frozen=True)

    shipment_id: str
    device_id: str
    truck_id: str
    route_id: str
    origin: str
    destination: str
    cargo_type: CargoType
    start_time: str
    end_time: str
    duration_hours: float
