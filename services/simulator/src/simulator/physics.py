"""
Thermal physics engine for refrigerated reefer containers.
Implements first-order heat transfer ODE with thermostat hysteresis and compressor states.
"""

from dataclasses import dataclass
import numpy as np
from .cargo_profiles import CargoProfile
from .models import CompressorStatus


# Physics engine constants (documented in PHYSICS.md)
HEAT_LEAK_COEFF: float = 18.0       # Container wall thermal conductance (W/K)
DOOR_HEAT_COEFF: float = 180.0      # Infiltration rate when door is open (W/K) - tuned for realistic infiltration
THERMOSTAT_BAND_C: float = 0.5      # Hysteresis half-band (+/- 0.5 C around setpoint)
PROCESS_NOISE_SIGMA: float = 0.045  # Stochastic turbulence / process noise stddev (C) - adjusted for boundary layer micro-gradients
AIR_CORE_COUPLING: float = 0.25     # Heat exchange coefficient between air volume and cargo mass


@dataclass
class ContainerThermalState:
    air_temp_c: float
    core_temp_c: float
    humidity_pct: float
    compressor_on: bool
    compressor_status: CompressorStatus
    compressor_effectiveness: float  # 1.0 = 100%, 0.4-1.0 = degraded, 0.0 = failed


class ContainerPhysicsEngine:
    """
    Computes discrete step evolution of container air and cargo core temperatures.
    Incorporates per-truck heterogeneity and unobserved latent factors.
    """

    def __init__(
        self,
        profile: CargoProfile,
        rng: np.random.Generator,
        initial_temp_offset_c: float = 0.0,
        heat_leak_mult: float = 1.0,
        cooling_power_mult: float = 1.0,
        latent_door_seal_factor: float = 1.0,
        latent_load_factor: float = 1.0,
    ):
        self.profile = profile
        self.rng = rng
        self.heat_leak_mult = heat_leak_mult
        self.cooling_power_mult = cooling_power_mult
        self.latent_door_seal_factor = latent_door_seal_factor
        self.latent_load_factor = latent_load_factor

        # Starting state: if initial_temp_offset_c != 0, represents pre-cool deficiency
        initial_temp = profile.setpoint_c + initial_temp_offset_c
        self.state = ContainerThermalState(
            air_temp_c=initial_temp,
            core_temp_c=initial_temp,
            humidity_pct=profile.humidity_nominal_pct,
            compressor_on=initial_temp > (profile.setpoint_c + THERMOSTAT_BAND_C),
            compressor_status=CompressorStatus.OK,
            compressor_effectiveness=1.0,
        )

    def step(
        self,
        dt_seconds: float,
        ambient_temp_c: float,
        ambient_humidity_pct: float,
        door_open: bool,
    ) -> ContainerThermalState:
        """
        Advances the thermal state by dt_seconds using explicit Euler integration.
        """
        curr_t = self.state.air_temp_c
        setpoint = self.profile.setpoint_c

        # 1. Thermostat hysteresis logic
        # Turns ON above setpoint + band; turns OFF below setpoint - band
        if curr_t > (setpoint + THERMOSTAT_BAND_C):
            self.state.compressor_on = True
        elif curr_t < (setpoint - THERMOSTAT_BAND_C):
            self.state.compressor_on = False
        # else: retain previous compressor_on state within deadband

        # If compressor is in failed status, it cannot physically generate cooling
        if self.state.compressor_status == CompressorStatus.FAILED:
            effective_cooling = 0.0
        elif not self.state.compressor_on:
            effective_cooling = 0.0
        else:
            # Cooling actively pulls temperature toward setpoint
            # Scaled by per-truck cooling multiplier and active effectiveness
            effective_cooling = (
                self.profile.cooling_power * 1000.0 * self.cooling_power_mult * self.state.compressor_effectiveness
            )

        # 2. Heat ingress/loss calculations (Watts)
        # Heat conduction through container walls (scaled by per-truck insulation & latent seal quality)
        wall_heat_flow = (HEAT_LEAK_COEFF * self.heat_leak_mult * self.latent_door_seal_factor) * (ambient_temp_c - curr_t)

        # Infiltration heat flow when door is opened (scaled by seal/gasket condition)
        door_heat_flow = (DOOR_HEAT_COEFF * self.latent_door_seal_factor) * (ambient_temp_c - curr_t) if door_open else 0.0

        # Solar radiation absorption on container roof/walls when ambient is warm
        solar_heat_flow = 0.0
        if ambient_temp_c >= 28.0 and not door_open:
            solar_heat_flow = 320.0 * ((ambient_temp_c - 25.0) / 15.0) * self.heat_leak_mult

        # Net thermal rate of change (dT in degrees C per step)
        # Thermal mass scaled by unobserved cargo load factor
        mass_joules = (self.profile.thermal_mass * self.latent_load_factor) * 1000.0
        net_power_watts = wall_heat_flow + door_heat_flow + solar_heat_flow - effective_cooling

        dt_factor = dt_seconds / mass_joules
        dt_air = net_power_watts * dt_factor

        # 3. Air-to-core thermal inertia coupling
        core_delta = (curr_t - self.state.core_temp_c) * AIR_CORE_COUPLING * (dt_seconds / 3600.0)
        self.state.core_temp_c += core_delta

        # 4. Stochastic process noise (micro-turbulence) & occasional thermal disturbance
        noise = float(self.rng.normal(0.0, PROCESS_NOISE_SIGMA * np.sqrt(dt_seconds / 60.0)))

        # Rare micro thermal disturbance (e.g. coil defrost cycle pulse or air turbulence channeling)
        thermal_pulse = 0.0
        if self.rng.random() < 0.0012:
            thermal_pulse = float(self.rng.choice([-1.0, 1.0]) * self.rng.uniform(0.15, 0.35))

        next_air_temp = curr_t + dt_air + noise + thermal_pulse
        # Clamp to realistic physical bounds (-40 C to +60 C)
        self.state.air_temp_c = max(-40.0, min(60.0, next_air_temp))

        # 5. Humidity evolution
        curr_hum = self.state.humidity_pct
        if door_open:
            # Rapid equilibration with ambient moisture
            d_hum = (ambient_humidity_pct - curr_hum) * 0.06 * (dt_seconds / 60.0)
        elif self.state.compressor_on and self.state.compressor_status != CompressorStatus.FAILED:
            # Evaporator condensation gently dehumidifies air
            d_hum = -0.04 * (dt_seconds / 60.0)
        else:
            # Cargo moisture buffering relaxes toward nominal cargo humidity
            d_hum = (self.profile.humidity_nominal_pct - curr_hum) * 0.015 * (dt_seconds / 60.0)

        hum_noise = float(self.rng.normal(0.0, 0.1 * np.sqrt(dt_seconds / 60.0)))
        next_hum = max(10.0, min(99.0, curr_hum + d_hum + hum_noise))
        self.state.humidity_pct = next_hum

        return self.state
