"""
Data schemas for consumer service.
Re-exported from coldchain_common for modularity.
"""

from coldchain_common import (
    CARGO_PROFILES,
    CargoProfile,
    CargoType,
    CompressorStatus,
    DeadLetterEnvelope,
    DeadLetterErrorType,
    SensorReading,
    get_cargo_profile,
)

__all__ = [
    "CARGO_PROFILES",
    "CargoProfile",
    "CargoType",
    "CompressorStatus",
    "DeadLetterEnvelope",
    "DeadLetterErrorType",
    "SensorReading",
    "get_cargo_profile",
]
