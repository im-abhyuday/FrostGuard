"""
Truck agent: encapsulates container physics, route progression, telemetry generation,
and disruptive event orchestration for an individual refrigerated transport vehicle.
"""

import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np

from .cargo_profiles import CargoProfile, get_cargo_profile
from .environment import EnvironmentModel
from .events import EventManager
from .faults import SensorFaultInjector
from .models import (
    CargoType,
    CompressorStatus,
    GroundTruthEvent,
    SensorReading,
    ShipmentMetadata,
)
from .physics import ContainerPhysicsEngine
from .routes import INDIAN_ROUTES, Route, RouteInterpolator


class ScheduledStop:
    def __init__(self, start_sec: float, end_sec: float, stop_type: str):
        self.start_sec = start_sec
        self.end_sec = end_sec
        self.stop_type = stop_type


class Truck:
    """
    Simulates a refrigerated truck advancing through continuous trips along Indian highway routes.
    Maintains reproducible state via its dedicated numpy Generator.
    """

    def __init__(
        self,
        truck_index: int,
        rng: np.random.Generator,
        cargo_mix: Dict[CargoType, float],
        excursion_prob: float = 0.05,
        start_time: datetime = datetime(2026, 5, 1, 0, 0, 0),
    ):
        self.truck_index = truck_index
        self.truck_id = f"TRK-{truck_index:04d}"
        self.device_id = f"DEV-{truck_index:04d}"
        self.rng = rng
        self.cargo_mix = cargo_mix
        self.excursion_prob = excursion_prob
        self.current_time = start_time

        # Seeded per-truck physical heterogeneity & unobserved latent factors (B4)
        self.heat_leak_mult = float(self.rng.uniform(0.88, 1.12))
        self.cooling_power_mult = float(self.rng.uniform(0.88, 1.12))
        self.latent_door_seal_factor = float(self.rng.uniform(0.90, 1.25))
        self.latent_load_factor = float(self.rng.uniform(0.85, 1.15))

        self.shipment_seq = 1
        self.completed_shipments: List[ShipmentMetadata] = []
        self.completed_ground_truth: List[GroundTruthEvent] = []

        # Active excursion tracker
        self.in_excursion: bool = False
        self.excursion_start_time: Optional[datetime] = None
        self.trip_has_excursion: bool = False

        # Initialize first trip
        self._start_new_shipment()

    def _choose_cargo(self) -> CargoType:
        types = list(self.cargo_mix.keys())
        probs = [self.cargo_mix[t] for t in types]
        chosen_idx = self.rng.choice(len(types), p=probs)
        return types[chosen_idx]

    def _choose_route(self) -> Route:
        idx = self.rng.choice(len(INDIAN_ROUTES))
        return INDIAN_ROUTES[idx]

    def _start_new_shipment(self) -> None:
        self.shipment_id = f"SHP-{self.truck_index:04d}-{self.shipment_seq:04d}"
        self.cargo_type = self._choose_cargo()
        self.profile: CargoProfile = get_cargo_profile(self.cargo_type)
        self.route: Route = self._choose_route()
        self.interpolator = RouteInterpolator(self.route)

        self.shipment_start_time = self.current_time
        self.trip_elapsed_secs = 0.0

        # Estimate average transit speed ~55 km/h + stops to calculate total trip time
        transit_hours = self.route.total_distance_km / float(self.rng.uniform(50.0, 65.0))
        # Add loading (0.75h) + 1-2 rest stops (1.5h) + unloading (0.75h)
        total_hours = transit_hours + float(self.rng.uniform(2.5, 4.0))
        self.trip_total_secs = total_hours * 3600.0

        # Construct scheduled stationary stops
        self.stops: List[ScheduledStop] = []
        # 1. Loading stop at origin (first 30 to 50 mins)
        loading_duration = float(self.rng.uniform(30.0, 50.0)) * 60.0
        self.stops.append(ScheduledStop(0.0, loading_duration, "LOADING"))

        # 2. En-route rest / fuel stops
        num_mid_stops = int(self.rng.choice([1, 2]))
        for i in range(num_mid_stops):
            stop_center = self.trip_total_secs * ((i + 1) / (num_mid_stops + 1))
            dur = float(self.rng.uniform(20.0, 45.0)) * 60.0
            self.stops.append(ScheduledStop(stop_center - dur / 2.0, stop_center + dur / 2.0, "REST_STOP"))

        # 3. Unloading stop at destination
        unloading_dur = float(self.rng.uniform(30.0, 60.0)) * 60.0
        self.stops.append(ScheduledStop(self.trip_total_secs - unloading_dur, self.trip_total_secs, "UNLOADING"))

        # Pre-cooling defect injection: ~16% of shipments start poorly pre-cooled (B2)
        is_precooled = bool(self.rng.random() >= 0.16)
        initial_offset = 0.0
        if not is_precooled:
            # Milder offsets: some recover, some breach safe max
            initial_offset = float(self.rng.uniform(1.2, 4.0))

        self.physics = ContainerPhysicsEngine(
            profile=self.profile,
            rng=self.rng,
            initial_temp_offset_c=initial_offset,
            heat_leak_mult=self.heat_leak_mult,
            cooling_power_mult=self.cooling_power_mult,
            latent_door_seal_factor=self.latent_door_seal_factor,
            latent_load_factor=self.latent_load_factor,
        )
        self.env_model = EnvironmentModel(rng=self.rng)
        self.event_mgr = EventManager(
            rng=self.rng,
            excursion_prob=self.excursion_prob,
            shipment_id=self.shipment_id,
            device_id=self.device_id,
        )
        if not is_precooled:
            self.event_mgr.inject_precool_defect(self.current_time, duration_mins=int(self.rng.uniform(60, 150)))

        self.fault_injector = SensorFaultInjector(rng=self.rng)
        self.battery_pct = float(self.rng.uniform(92.0, 99.5))
        self.in_excursion = False
        self.excursion_start_time = None
        self.trip_has_excursion = False
        self.precooled_status = is_precooled

    def advance_step(
        self,
        dt_seconds: float,
        is_live_mode: bool = False,
        bad_event_rate: float = 0.0,
    ) -> List[Union[SensorReading, Dict[str, Any]]]:
        """
        Advances the truck by dt_seconds in simulated time.
        """
        step_time = self.current_time

        # 1. Determine vehicle movement and scheduled stop state
        is_scheduled_stop = False
        for s in self.stops:
            if s.start_sec <= self.trip_elapsed_secs <= s.end_sec:
                is_scheduled_stop = True
                break

        # Calculate transit progress fraction (0.0 to 1.0)
        # Progress along route happens between end of loading and start of unloading
        loading_end = self.stops[0].end_sec
        unloading_start = self.stops[-1].start_sec
        transit_duration = max(1.0, unloading_start - loading_end)

        if self.trip_elapsed_secs <= loading_end:
            route_fraction = 0.0
            base_speed = 0.0
        elif self.trip_elapsed_secs >= unloading_start:
            route_fraction = 1.0
            base_speed = 0.0
        else:
            route_fraction = (self.trip_elapsed_secs - loading_end) / transit_duration
            base_speed = float(self.rng.normal(62.0, 6.0)) if not is_scheduled_stop else 0.0

        lat, lon, base_ambient_temp = self.interpolator.interpolate(route_fraction)

        # 2. Ambient environment calculation
        ambient_temp, ambient_hum = self.env_model.compute_ambient(base_ambient_temp, step_time)

        # 3. Disruptive event simulation
        door_open, comp_status, comp_eff, traffic_stop = self.event_mgr.step(
            current_time=step_time,
            dt_seconds=dt_seconds,
            is_scheduled_stop=is_scheduled_stop,
            ambient_temp_c=ambient_temp,
        )

        speed_kmph = 0.0 if (is_scheduled_stop or traffic_stop) else max(0.0, base_speed)

        # Idle heat soak during traffic stop in high ambient (B2)
        if traffic_stop and ambient_temp >= 29.0:
            comp_eff = min(comp_eff, 0.05)
            ambient_temp += 3.5

        # 4. Thermal physics evolution
        self.physics.state.compressor_status = comp_status
        self.physics.state.compressor_effectiveness = comp_eff

        thermal_state = self.physics.step(
            dt_seconds=dt_seconds,
            ambient_temp_c=ambient_temp,
            ambient_humidity_pct=ambient_hum,
            door_open=door_open,
        )

        # 5. Excursion tracking
        air_t = thermal_state.air_temp_c
        is_out_of_range = (air_t < self.profile.safe_min_c) or (air_t > self.profile.safe_max_c)

        if is_out_of_range:
            self.trip_has_excursion = True
            if not self.in_excursion:
                self.in_excursion = True
                self.excursion_start_time = step_time
                self.event_mgr.register_excursion_start(step_time)
        else:
            if self.in_excursion:
                self.in_excursion = False
                self.event_mgr.register_excursion_end(step_time)
                self.excursion_start_time = None

        # 6. Battery evolution
        if speed_kmph > 10.0:
            # Alternator charging
            self.battery_pct = min(100.0, self.battery_pct + 0.008 * (dt_seconds / 60.0))
        else:
            # Battery discharging
            self.battery_pct = max(15.0, self.battery_pct - 0.005 * (dt_seconds / 60.0))

        # 7. Construct true sensor reading
        raw_reading = SensorReading(
            schema_version="1.0",
            event_id=str(uuid.uuid4()),
            shipment_id=self.shipment_id,
            device_id=self.device_id,
            timestamp=step_time.isoformat() + "Z",
            temperature_c=round(air_t, 2),
            humidity_pct=round(thermal_state.humidity_pct, 1),
            door_open=door_open,
            compressor_on=thermal_state.compressor_on,
            compressor_status=comp_status,
            ambient_temp_c=ambient_temp,
            latitude=round(lat, 5),
            longitude=round(lon, 5),
            speed_kmph=round(speed_kmph, 1),
            cargo_type=self.cargo_type,
            setpoint_c=self.profile.setpoint_c,
            battery_pct=round(self.battery_pct, 1),
        )

        # 8. Apply sensor-level faults (noise, drops, delays, duplicates, bad payload)
        payloads = self.fault_injector.process(
            reading=raw_reading,
            current_time=step_time,
            is_live_mode=is_live_mode,
            bad_event_rate=bad_event_rate,
        )

        # 9. Advance timeline and handle trip wrap-around
        self.current_time += timedelta(seconds=dt_seconds)
        self.trip_elapsed_secs += dt_seconds

        if self.trip_elapsed_secs >= self.trip_total_secs:
            self._finalize_shipment()
            self._start_new_shipment()

        return payloads

    def _finalize_shipment(self) -> None:
        if self.in_excursion:
            self.event_mgr.register_excursion_end(self.current_time)
            self.in_excursion = False
            self.excursion_start_time = None

        meta = ShipmentMetadata(
            shipment_id=self.shipment_id,
            device_id=self.device_id,
            truck_id=self.truck_id,
            route_id=self.route.route_id,
            origin=self.route.origin,
            destination=self.route.destination,
            cargo_type=self.cargo_type,
            start_time=self.shipment_start_time.isoformat() + "Z",
            end_time=self.current_time.isoformat() + "Z",
            duration_hours=round(self.trip_elapsed_secs / 3600.0, 2),
            distance_km=self.route.total_distance_km,
            has_excursion=self.trip_has_excursion,
            precooled=self.precooled_status,
        )
        self.completed_shipments.append(meta)

        flushed_events = self.event_mgr.finalize()
        self.completed_ground_truth.extend(flushed_events)
        self.completed_ground_truth.extend(self.fault_injector.fault_events)
        self.fault_injector.fault_events.clear()

        self.shipment_seq += 1

    def force_failure(self) -> None:
        """Forces an immediate compressor failure on this truck (for live demos)."""
        self.event_mgr.trigger_forced_failure(self.current_time)

    def finalize(self) -> Tuple[List[ShipmentMetadata], List[GroundTruthEvent]]:
        """Called at end of simulation to extract all recorded metadata and ground truth."""
        self._finalize_shipment()
        return list(self.completed_shipments), list(self.completed_ground_truth)
