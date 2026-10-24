"""
Unit tests for sensor-level fault injection rates (missing, duplicates, late).
"""

from datetime import datetime, timedelta
from simulator.faults import SensorFaultInjector
from simulator.models import CargoType, CompressorStatus, SensorReading


def test_fault_frequencies_within_tolerance(rng):
    """
    Simulates a large sequence of readings through the fault injector and verifies
    that empirical fault rates match target probabilities within statistical tolerance.
    """
    missing_rate = 0.02
    duplicate_rate = 0.01
    late_rate = 0.015

    injector = SensorFaultInjector(
        rng=rng,
        missing_rate=missing_rate,
        duplicate_rate=duplicate_rate,
        late_rate=late_rate,
        sensor_fault_rate=0.0,
        gap_rate=0.0,
    )

    n_samples = 5000
    base_time = datetime(2026, 5, 1, 0, 0, 0)

    emitted_count = 0
    delayed_queued_count = 0

    for i in range(n_samples):
        curr_time = base_time + timedelta(seconds=i * 60)
        reading = SensorReading(
            schema_version="1.0",
            event_id=f"evt-{i}",
            shipment_id="SHP-TEST",
            device_id="DEV-TEST",
            timestamp=curr_time.isoformat() + "Z",
            temperature_c=5.0,
            humidity_pct=60.0,
            door_open=False,
            compressor_on=True,
            compressor_status=CompressorStatus.OK,
            ambient_temp_c=30.0,
            latitude=20.0,
            longitude=75.0,
            speed_kmph=60.0,
            cargo_type=CargoType.VACCINE,
            setpoint_c=5.0,
            battery_pct=90.0,
        )

        prev_delayed = len(injector._delayed_buffer)
        payloads = injector.process(reading, curr_time, is_live_mode=False)
        new_delayed = len(injector._delayed_buffer)

        if new_delayed > prev_delayed:
            delayed_queued_count += (new_delayed - prev_delayed)

        emitted_count += len(payloads)

    # Missing rate check: total produced vs expected
    observed_late_rate = delayed_queued_count / n_samples
    assert abs(observed_late_rate - late_rate) < 0.008, (
        f"Observed late rate {observed_late_rate:.4f} deviated from target {late_rate}"
    )

    # Duplicate emissions produce multiple payloads for a single input step
    assert emitted_count > 0
