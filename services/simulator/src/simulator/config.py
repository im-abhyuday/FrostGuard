"""
Simulator configuration using pydantic-settings.
Supports environment variables (.env), defaults, and CLI parameter overrides.
"""

from typing import Dict, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from .models import CargoType


class SimulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SIM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # General / Fleet settings
    trucks: int = Field(default=50, ge=1, le=500, description="Total number of trucks in fleet")
    seed: int = Field(default=42, description="Master deterministic seed")
    excursion_prob: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Base probability scalar for degradation/failure injection to calibrate positive class ratio",
    )
    cargo_mix: str = Field(
        default="VACCINE:0.3,DAIRY:0.3,FROZEN_FOOD:0.25,FRESH_PRODUCE:0.15",
        description="Comma-separated ratio distribution of cargo types",
    )
    start_date: str = Field(
        default="2026-05-01T00:00:00Z",
        description="Simulation epoch start date (ISO-8601 UTC)",
    )

    # Batch mode settings
    days: int = Field(default=30, ge=1, le=365, description="Number of simulation days")
    interval_sec: int = Field(default=60, ge=5, le=3600, description="Physics time step dt (seconds)")
    out_dir: str = Field(default="./data/raw", description="Output directory for generated datasets")

    # Live mode settings
    time_scale: float = Field(
        default=60.0,
        ge=1.0,
        le=1000.0,
        description="Sim-to-real speed multiplier (e.g. 60x: 1 real second = 60 sim seconds)",
    )
    bad_event_rate: float = Field(
        default=0.002,
        ge=0.0,
        le=0.1,
        description="Probability of injecting malformed payload for Dead Letter Queue testing (live mode only)",
    )
    force_excursion: Optional[str] = Field(
        default=None,
        description="Specific device_id (e.g. DEV-0001) to force-trigger an excursion for live demos",
    )
    kafka_bootstrap: str = Field(
        default="localhost:9092",
        description="Kafka / Redpanda broker bootstrap connection string",
    )
    kafka_topic: str = Field(
        default="sensor.readings",
        description="Destination Kafka topic for sensor telemetry",
    )
    kafka_partitions: int = Field(
        default=6,
        description="Number of partitions to provision on auto-created topic",
    )

    @field_validator("cargo_mix")
    @classmethod
    def validate_cargo_mix(cls, v: str) -> str:
        parts = v.split(",")
        total = 0.0
        for part in parts:
            if ":" not in part:
                raise ValueError(f"Invalid cargo mix component: {part}. Expected TYPE:WEIGHT")
            name, weight_str = part.strip().split(":")
            CargoType(name.strip())
            total += float(weight_str.strip())
        if abs(total - 1.0) > 1e-3:
            raise ValueError(f"Cargo mix weights must sum to 1.0, got {total:.4f}")
        return v

    def parse_cargo_mix(self) -> Dict[CargoType, float]:
        dist: Dict[CargoType, float] = {}
        for part in self.cargo_mix.split(","):
            name, weight_str = part.strip().split(":")
            dist[CargoType(name.strip())] = float(weight_str.strip())
        return dist
