"""
Unit tests for deterministic reproducibility.
"""

from simulator.config import SimulatorSettings
from simulator.fleet import Fleet
from simulator.models import SensorReading


def _generate_telemetry_sequence(seed: int, steps: int = 120):
    settings = SimulatorSettings(
        trucks=2,
        seed=seed,
        days=1,
        interval_sec=60,
    )
    fleet = Fleet(settings)
    records = []
    for _ in range(steps):
        for truck in fleet.trucks:
            payloads = truck.advance_step(dt_seconds=60, is_live_mode=False, bad_event_rate=0.0)
            for p in payloads:
                if isinstance(p, SensorReading):
                    d = p.model_dump()
                    # Omit randomly generated event_id uuid, check physical and temporal attributes
                    del d["event_id"]
                    records.append(d)
    return records


def test_same_seed_produces_identical_telemetry():
    """
    Verifies that running the simulator with the same master seed produces
    bit-for-bit identical telemetry state sequences across all trucks.
    """
    run_1 = _generate_telemetry_sequence(seed=1337, steps=100)
    run_2 = _generate_telemetry_sequence(seed=1337, steps=100)

    assert len(run_1) == len(run_2)
    assert len(run_1) > 0
    assert run_1 == run_2, "Identical seed produced diverging telemetry data!"


def test_different_seeds_produce_diverging_telemetry():
    """
    Verifies that distinct master seeds produce non-identical pseudorandom sequences.
    """
    run_a = _generate_telemetry_sequence(seed=101, steps=50)
    run_b = _generate_telemetry_sequence(seed=202, steps=50)

    assert run_a != run_b, "Different seeds should produce differing telemetry sequences"
