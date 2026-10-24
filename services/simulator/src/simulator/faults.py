"""
Sensor-level fault injection engine.
Applies measurement noise, dropouts, late arrivals, duplicates, stuck values,
and corrupt payloads for DLQ robustness testing.
"""

import copy
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
import numpy as np

from .models import SensorReading, GroundTruthEvent, GroundTruthEventType, RootCauseTag


@dataclass
class DelayedReading:
    emit_at: datetime
    reading: SensorReading


class SensorFaultInjector:
    """
    Simulates hardware imperfections, telemetry dropout, transmission latency,
    and corrupted payloads.
    """

    def __init__(
        self,
        rng: np.random.Generator,
        missing_rate: float = 0.015,     # 1.5% drop rate
        late_rate: float = 0.010,        # 1.0% out-of-order delay
        duplicate_rate: float = 0.005,   # 0.5% duplicate rate
        sensor_fault_rate: float = 0.0008,  # Rare stuck/drift sensor fault
        gap_rate: float = 0.001,         # Rare connectivity blackouts
    ):
        self.rng = rng
        self.missing_rate = missing_rate
        self.late_rate = late_rate
        self.duplicate_rate = duplicate_rate
        self.sensor_fault_rate = sensor_fault_rate
        self.gap_rate = gap_rate

        self._delayed_buffer: List[DelayedReading] = []

        # Active sensor fault state (stuck or drift)
        self.active_sensor_fault_until: Optional[datetime] = None
        self.stuck_temperature: Optional[float] = None
        self.sensor_drift_offset: float = 0.0

        # Active connectivity gap
        self.gap_until: Optional[datetime] = None

        # Ground truth fault events recorded
        self.fault_events: List[GroundTruthEvent] = []

    def process(
        self,
        reading: SensorReading,
        current_time: datetime,
        is_live_mode: bool = False,
        bad_event_rate: float = 0.0,
    ) -> List[Union[SensorReading, Dict[str, Any]]]:
        """
        Transforms raw physical state into observed telemetry.
        Returns a list of payloads to emit (may be 0, 1, or multiple if duplicates/delayed arrive).
        """
        output_payloads: List[Union[SensorReading, Dict[str, Any]]] = []

        # 1. Flush any delayed readings whose release time has arrived
        remaining_delayed: List[DelayedReading] = []
        for delayed in self._delayed_buffer:
            if current_time >= delayed.emit_at:
                output_payloads.append(delayed.reading)
            else:
                remaining_delayed.append(delayed)
        self._delayed_buffer = remaining_delayed

        # 2. Check or trigger connectivity gaps (5 to 20 minutes)
        if self.gap_until and current_time < self.gap_until:
            # Under blackout gap: reading is dropped
            return output_payloads
        elif self.gap_until and current_time >= self.gap_until:
            self.gap_until = None

        if self.rng.random() < self.gap_rate:
            gap_duration_mins = float(self.rng.uniform(5.0, 20.0))
            self.gap_until = current_time + timedelta(minutes=gap_duration_mins)
            return output_payloads

        # 3. Check or trigger rare sensor faults (stuck value or drift)
        if self.active_sensor_fault_until and current_time >= self.active_sensor_fault_until:
            self.active_sensor_fault_until = None
            self.stuck_temperature = None
            self.sensor_drift_offset = 0.0

        if not self.active_sensor_fault_until and self.rng.random() < self.sensor_fault_rate:
            fault_duration_mins = float(self.rng.uniform(20.0, 60.0))
            self.active_sensor_fault_until = current_time + timedelta(minutes=fault_duration_mins)
            is_stuck = bool(self.rng.random() < 0.5)
            if is_stuck:
                self.stuck_temperature = reading.temperature_c
            else:
                self.sensor_drift_offset = float(self.rng.choice([-2.5, 2.5, -3.0, 3.0]))

            gt_fault = GroundTruthEvent(
                shipment_id=reading.shipment_id,
                device_id=reading.device_id,
                event_type=GroundTruthEventType.SENSOR_FAULT,
                start_time=current_time.isoformat() + "Z",
                end_time=(current_time + timedelta(minutes=fault_duration_mins)).isoformat() + "Z",
                caused_excursion=False,
                root_cause_tag=RootCauseTag.NONE,
            )
            self.fault_events.append(gt_fault)

        # 4. Apply Gaussian measurement noise and active sensor fault
        if self.stuck_temperature is not None:
            observed_temp = self.stuck_temperature
        else:
            temp_noise = float(self.rng.normal(0.0, 0.18))
            observed_temp = round(reading.temperature_c + temp_noise + self.sensor_drift_offset, 2)

        hum_noise = float(self.rng.normal(0.0, 0.75))
        observed_hum = round(float(np.clip(reading.humidity_pct + hum_noise, 0.0, 100.0)), 1)

        # Create updated reading dict
        reading_dict = reading.model_dump()
        reading_dict["temperature_c"] = observed_temp
        reading_dict["humidity_pct"] = observed_hum

        # 5. Check bad payload injection (Live Mode Only, for DLQ testing)
        if is_live_mode and bad_event_rate > 0.0 and self.rng.random() < bad_event_rate:
            bad_variant = self.rng.integers(0, 4)
            corrupted = copy.deepcopy(reading_dict)
            if bad_variant == 0:
                corrupted["temperature_c"] = 999.9  # Out of physical range
            elif bad_variant == 1:
                corrupted["temperature_c"] = None   # Null required float
            elif bad_variant == 2:
                corrupted["humidity_pct"] = "NOT_A_FLOAT"  # Wrong data type
            else:
                corrupted["cargo_type"] = "UNKNOWN_CARGO"  # Invalid enum value
            output_payloads.append(corrupted)
            return output_payloads

        # Validate standard reading
        clean_reading = SensorReading(**reading_dict)

        # 6. Missing reading simulation
        if self.rng.random() < self.missing_rate:
            return output_payloads  # Dropped packet

        # 7. Late reading arrival (Out of order)
        if self.rng.random() < self.late_rate:
            delay_seconds = float(self.rng.uniform(30.0, 600.0))
            self._delayed_buffer.append(
                DelayedReading(
                    emit_at=current_time + timedelta(seconds=delay_seconds),
                    reading=clean_reading,
                )
            )
            return output_payloads

        # 8. Duplicate reading emission (same device_id + timestamp, unique event_id)
        output_payloads.append(clean_reading)
        if self.rng.random() < self.duplicate_rate:
            dupe_dict = clean_reading.model_dump()
            dupe_dict["event_id"] = str(uuid.uuid4())
            output_payloads.append(SensorReading(**dupe_dict))

        return output_payloads

    def flush_remaining(self) -> List[SensorReading]:
        """Flushes all buffered delayed readings at simulation termination."""
        remaining = [d.reading for d in self._delayed_buffer]
        self._delayed_buffer.clear()
        return remaining
