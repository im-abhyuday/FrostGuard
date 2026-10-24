"""
Unit tests for the container thermal physics engine.
"""

from simulator.cargo_profiles import get_cargo_profile
from simulator.models import CargoType, CompressorStatus
from simulator.physics import ContainerPhysicsEngine


def test_temperature_rises_when_compressor_failed(rng, vaccine_profile):
    """
    Verifies that when the compressor is in FAILED state and ambient temp is higher
    than container temp, internal temperature monotonically drifts upward.
    """
    engine = ContainerPhysicsEngine(vaccine_profile, rng=rng)
    engine.state.compressor_status = CompressorStatus.FAILED
    engine.state.compressor_effectiveness = 0.0

    initial_t = engine.state.air_temp_c
    ambient_t = 38.0  # Hot Indian highway ambient

    # Simulate 150 minutes (at ~1.5C/hr drift, 2.5 hours brings 5C to > 8C)
    for _ in range(150):
        engine.step(dt_seconds=60, ambient_temp_c=ambient_t, ambient_humidity_pct=40.0, door_open=False)

    final_t = engine.state.air_temp_c
    assert final_t > initial_t, f"Temperature failed to rise: {initial_t} -> {final_t}"
    assert final_t > vaccine_profile.safe_max_c, f"Failed compressor should induce excursion beyond 8C in 150 mins, got {final_t}"


def test_temperature_stabilizes_near_setpoint_with_healthy_compressor(rng, vaccine_profile):
    """
    Verifies that under severe ambient heat (38 C), a healthy compressor and thermostat
    maintain container temperature within the safe cargo band.
    """
    engine = ContainerPhysicsEngine(vaccine_profile, rng=rng)
    ambient_t = 38.0

    temps = []
    # Simulate 120 minutes of operation
    for _ in range(120):
        state = engine.step(dt_seconds=60, ambient_temp_c=ambient_t, ambient_humidity_pct=40.0, door_open=False)
        temps.append(state.air_temp_c)

    # After initial stabilization, mean temperature should be very close to setpoint
    steady_state_temps = temps[30:]
    mean_temp = sum(steady_state_temps) / len(steady_state_temps)

    assert abs(mean_temp - vaccine_profile.setpoint_c) < 1.0
    for t in steady_state_temps:
        assert vaccine_profile.safe_min_c <= t <= vaccine_profile.safe_max_c


def test_door_open_accelerates_thermal_ingress(rng, vaccine_profile):
    """
    Verifies that open doors significantly increase the rate of heat ingress (higher dT).
    """
    # 1. Door closed drift
    engine_closed = ContainerPhysicsEngine(vaccine_profile, rng=rng)
    engine_closed.state.compressor_on = False
    engine_closed.state.compressor_status = CompressorStatus.FAILED
    initial_t = engine_closed.state.air_temp_c

    for _ in range(15):
        engine_closed.step(dt_seconds=60, ambient_temp_c=40.0, ambient_humidity_pct=40.0, door_open=False)
    delta_closed = engine_closed.state.air_temp_c - initial_t

    # 2. Door open drift
    engine_open = ContainerPhysicsEngine(vaccine_profile, rng=rng)
    engine_open.state.compressor_on = False
    engine_open.state.compressor_status = CompressorStatus.FAILED

    for _ in range(15):
        engine_open.step(dt_seconds=60, ambient_temp_c=40.0, ambient_humidity_pct=40.0, door_open=True)
    delta_open = engine_open.state.air_temp_c - initial_t

    assert delta_open > (delta_closed * 3.0), (
        f"Door open heat ingress ({delta_open:.3f}C) should be substantially greater "
        f"than closed container wall leak ({delta_closed:.3f}C)"
    )
