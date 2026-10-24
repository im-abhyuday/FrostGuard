"""
Shared pytest fixtures for simulator test suite.
"""

from pathlib import Path
import sys
import numpy as np
import pytest

# Ensure simulator source directory is in sys.path
src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from simulator.cargo_profiles import CARGO_PROFILES, get_cargo_profile
from simulator.config import SimulatorSettings
from simulator.models import CargoType


@pytest.fixture
def rng():
    """Provides a deterministic generator for unit tests."""
    return np.random.default_rng(42)


@pytest.fixture
def vaccine_profile():
    return get_cargo_profile(CargoType.VACCINE)


@pytest.fixture
def frozen_profile():
    return get_cargo_profile(CargoType.FROZEN_FOOD)


@pytest.fixture
def default_settings():
    return SimulatorSettings(
        trucks=2,
        days=1,
        interval_sec=60,
        seed=100,
        excursion_prob=0.08,
    )
