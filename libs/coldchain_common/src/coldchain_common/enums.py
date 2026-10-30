"""
Domain enumerations shared across ColdChainIQ services.
"""

from enum import Enum


class CargoType(str, Enum):
    VACCINE = "VACCINE"
    FROZEN_FOOD = "FROZEN_FOOD"
    DAIRY = "DAIRY"
    FRESH_PRODUCE = "FRESH_PRODUCE"


class CompressorStatus(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


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


class ShipmentStatus(str, Enum):
    IN_TRANSIT = "IN_TRANSIT"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


class AlertSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertStatus(str, Enum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


class DeadLetterErrorType(str, Enum):
    CORRUPT_JSON = "CORRUPT_JSON"
    SCHEMA_VALIDATION_ERROR = "SCHEMA_VALIDATION_ERROR"
    INVALID_RANGE = "INVALID_RANGE"
    FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
    UNKNOWN_CARGO_TYPE = "UNKNOWN_CARGO_TYPE"
    MISSING_FIELD = "MISSING_FIELD"
