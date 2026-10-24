"""
Cargo profiles and thermal threshold definitions.
"""

from dataclasses import dataclass
from typing import Dict
from .models import CargoType


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
        thermal_mass=1400.0,       # High sensitivity: smaller physical shipment volume
        cooling_power=2.8,
        excursion_tolerance_mins=30,  # WHO PQS standard: ultra-tight compliance window
        humidity_nominal_pct=50.0,
        precool_band_c=0.5,
    ),
    CargoType.FROZEN_FOOD: CargoProfile(
        cargo_type=CargoType.FROZEN_FOOD,
        safe_min_c=-25.0,
        safe_max_c=-18.0,
        setpoint_c=-21.0,
        thermal_mass=2800.0,      # High mass: frozen pallet thermal inertia
        cooling_power=4.5,        # High power sub-zero refrigeration compressor
        excursion_tolerance_mins=60,
        humidity_nominal_pct=70.0,
        precool_band_c=1.5,
    ),
    CargoType.DAIRY: CargoProfile(
        cargo_type=CargoType.DAIRY,
        safe_min_c=1.0,
        safe_max_c=4.0,
        setpoint_c=2.5,
        thermal_mass=2100.0,      # Moderate mass: milk/butter crates
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
        thermal_mass=2500.0,      # High respiration & moisture buffering
        cooling_power=3.0,
        excursion_tolerance_mins=120, # Higher biological tolerance before decay
        humidity_nominal_pct=85.0,
        precool_band_c=1.0,
    ),
}


def get_cargo_profile(cargo_type: CargoType | str) -> CargoProfile:
    if isinstance(cargo_type, str):
        cargo_type = CargoType(cargo_type)
    return CARGO_PROFILES[cargo_type]
