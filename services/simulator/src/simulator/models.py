"""
Pydantic v2 schemas for ColdChainIQ telemetry and ground-truth events.
"""

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class CompressorStatus(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class CargoType(str, Enum):
    VACCINE = "VACCINE"
    FROZEN_FOOD = "FROZEN_FOOD"
    DAIRY = "DAIRY"
    FRESH_PRODUCE = "FRESH_PRODUCE"


class GroundTruthEventType(str, Enum):
    DOOR_OPEN = "DOOR_OPEN"
    COMPRESSOR_DEGRADATION = "COMPRESSOR_DEGRADATION"
    COMPRESSOR_FAILURE = "COMPRESSOR_FAILURE"
    TRAFFIC_STOP = "TRAFFIC_STOP"
    PRECOOL_MISSING = "PRECOOL_MISSING"
    SENSOR_FAULT = "SENSOR_FAULT"


class RootCauseTag(str, Enum):
    DOOR_LEFT_OPEN = "door_left_open"
    COMPRESSOR_FAILURE = "compressor_failure"
    COMPRESSOR_DEGRADATION = "compressor_degradation"
    LONG_STOP_HOT_AMBIENT = "long_stop_hot_ambient"
    INSUFFICIENT_PRECOOL = "insufficient_precool"
    NONE = "none"


class SensorReading(BaseModel):
    """
    Sensor reading payload emitted by onboard telematics.
    Kafka key: device_id. Deduplication key: (device_id, timestamp).
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: str = Field(default="1.0", description="Semantic contract version")
    event_id: str = Field(..., description="Unique UUID per emission; not used for dedupe")
    shipment_id: str = Field(..., description="Shipment business identifier e.g. SHP-000123")
    device_id: str = Field(..., description="Hardware sensor device ID e.g. DEV-0007")
    timestamp: str = Field(..., description="ISO-8601 UTC timestamp of reported time")
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


class GroundTruthEvent(BaseModel):
    """
    Separate ground-truth log for ML validation, label generation, and SHAP alignment.
    NEVER streamed to live Kafka topics.
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
    Trip-level metadata record for auditability and batch slices.
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
    distance_km: float
    has_excursion: bool
    precooled: bool
