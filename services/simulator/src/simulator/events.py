"""
Discrete event generation engine: door openings, compressor degradation/failures, traffic stops.
Logs ground-truth events with root cause tagging for ML explainability and validation.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional
import numpy as np

from .models import (
    CompressorStatus,
    GroundTruthEvent,
    GroundTruthEventType,
    RootCauseTag,
)


@dataclass
class ActiveEvent:
    event_type: GroundTruthEventType
    start_time: datetime
    end_time: datetime
    root_cause_tag: RootCauseTag
    caused_excursion: bool = False
    excursion_start: Optional[datetime] = None
    excursion_end: Optional[datetime] = None


class EventManager:
    """
    Simulates stochastic disruptions (door, mechanical degradation, sudden failure, traffic)
    and maintains ground-truth tracking for each shipment.
    """

    def __init__(
        self,
        rng: np.random.Generator,
        excursion_prob: float = 0.05,
        shipment_id: str = "SHP-000000",
        device_id: str = "DEV-0000",
    ):
        self.rng = rng
        self.excursion_prob = excursion_prob
        self.shipment_id = shipment_id
        self.device_id = device_id

        # Active disruptive states
        self.active_door: Optional[ActiveEvent] = None
        self.active_degradation: Optional[ActiveEvent] = None
        self.active_failure: Optional[ActiveEvent] = None
        self.active_traffic: Optional[ActiveEvent] = None
        self.active_precool: Optional[ActiveEvent] = None

        # Degraded compressor effectiveness progress
        self.degradation_total_secs: float = 0.0
        self.degradation_elapsed_secs: float = 0.0
        self.degradation_will_fail: bool = False

        # Completed events history for ground truth export
        self.recorded_events: List[GroundTruthEvent] = []

    def inject_precool_defect(self, start_time: datetime, duration_mins: int = 120) -> None:
        """Injects an insufficient precool state at shipment initiation."""
        end_time = start_time + timedelta(minutes=duration_mins)
        self.active_precool = ActiveEvent(
            event_type=GroundTruthEventType.PRECOOL_MISSING,
            start_time=start_time,
            end_time=end_time,
            root_cause_tag=RootCauseTag.INSUFFICIENT_PRECOOL,
        )

    def trigger_forced_failure(self, start_time: datetime, duration_mins: int = 180) -> None:
        """Scripted manual compressor failure trigger for live mode demos."""
        end_time = start_time + timedelta(minutes=duration_mins)
        self.active_failure = ActiveEvent(
            event_type=GroundTruthEventType.COMPRESSOR_FAILURE,
            start_time=start_time,
            end_time=end_time,
            root_cause_tag=RootCauseTag.COMPRESSOR_FAILURE,
        )

    def step(
        self,
        current_time: datetime,
        dt_seconds: float,
        is_scheduled_stop: bool,
        ambient_temp_c: float,
    ) -> tuple[bool, CompressorStatus, float, bool]:
        """
        Advances the event state by dt_seconds.
        Returns:
            (door_open, compressor_status, compressor_effectiveness, traffic_override_stop)
        """
        # --- 1. Check expiration of active events ---
        if self.active_door and current_time >= self.active_door.end_time:
            self._close_event(self.active_door)
            self.active_door = None

        if self.active_traffic and current_time >= self.active_traffic.end_time:
            self._close_event(self.active_traffic)
            self.active_traffic = None

        if self.active_failure and current_time >= self.active_failure.end_time:
            self._close_event(self.active_failure)
            self.active_failure = None

        if self.active_precool and current_time >= self.active_precool.end_time:
            self._close_event(self.active_precool)
            self.active_precool = None

        # --- 2. Compressor degradation evolution ---
        compressor_status = CompressorStatus.OK
        compressor_effectiveness = 1.0

        if self.active_degradation:
            self.degradation_elapsed_secs += dt_seconds
            frac = min(1.0, self.degradation_elapsed_secs / max(1.0, self.degradation_total_secs))
            # Power law decline: falls steadily below thermal capacity, generating detectable precursor
            compressor_effectiveness = max(0.08, 1.0 - 0.92 * (frac ** 0.8))
            compressor_status = CompressorStatus.DEGRADED

            if current_time >= self.active_degradation.end_time:
                deg_ev = self.active_degradation
                deg_ev.end_time = current_time
                self._close_event(deg_ev)
                self.active_degradation = None

                if self.degradation_will_fail and not self.active_failure:
                    # Transition from degradation directly into full failure
                    failure_duration_mins = int(self.rng.uniform(30, 180))
                    self.active_failure = ActiveEvent(
                        event_type=GroundTruthEventType.COMPRESSOR_FAILURE,
                        start_time=current_time,
                        end_time=current_time + timedelta(minutes=failure_duration_mins),
                        root_cause_tag=RootCauseTag.COMPRESSOR_FAILURE,
                    )

        if self.active_failure:
            compressor_status = CompressorStatus.FAILED
            compressor_effectiveness = 0.0

        # --- 3. Stochastic generation of new events ---
        rate_scale = self.excursion_prob / 0.05

        # (a) Door Openings: Poisson arrival process
        # Higher probability at stops (loading/unloading/customs) than while rolling
        if not self.active_door:
            door_rate_per_hour = 0.32 if is_scheduled_stop else 0.018
            p_door = 1.0 - np.exp(-(door_rate_per_hour * rate_scale * (dt_seconds / 3600.0)))
            if self.rng.random() < p_door:
                # B1: Tail duration scaling with ambient temperature at stops
                if is_scheduled_stop:
                    # Hot ambient stops have heavier duration tail (10-30 mins)
                    mu = 1.30 if ambient_temp_c >= 32.0 else 1.05
                    sigma = 0.75
                    duration_mins = float(np.clip(self.rng.lognormal(mean=mu, sigma=sigma), 1.0, 32.0))
                else:
                    duration_mins = float(np.clip(self.rng.lognormal(mean=0.65, sigma=0.50), 0.5, 10.0))
                self.active_door = ActiveEvent(
                    event_type=GroundTruthEventType.DOOR_OPEN,
                    start_time=current_time,
                    end_time=current_time + timedelta(minutes=duration_mins),
                    root_cause_tag=RootCauseTag.DOOR_LEFT_OPEN,
                )

        # (b) Compressor Degradation: Gradual precursor
        if not self.active_degradation and not self.active_failure:
            p_degrade = 1.0 - np.exp(-(0.0068 * rate_scale * (dt_seconds / 3600.0)))
            if self.rng.random() < p_degrade:
                duration_mins = float(self.rng.uniform(50.0, 130.0))
                self.degradation_total_secs = duration_mins * 60.0
                self.degradation_elapsed_secs = 0.0
                # B3: ~68% chance this degradation ends in complete failure
                self.degradation_will_fail = bool(self.rng.random() < 0.68)
                self.active_degradation = ActiveEvent(
                    event_type=GroundTruthEventType.COMPRESSOR_DEGRADATION,
                    start_time=current_time,
                    end_time=current_time + timedelta(minutes=duration_mins),
                    root_cause_tag=RootCauseTag.COMPRESSOR_DEGRADATION,
                )

        # (c) Sudden Compressor Failure (no precursor, ~37% of all failure cases)
        if not self.active_degradation and not self.active_failure:
            p_sudden_fail = 1.0 - np.exp(-(0.0019 * rate_scale * (dt_seconds / 3600.0)))
            if self.rng.random() < p_sudden_fail:
                duration_mins = float(self.rng.uniform(30.0, 180.0))
                self.active_failure = ActiveEvent(
                    event_type=GroundTruthEventType.COMPRESSOR_FAILURE,
                    start_time=current_time,
                    end_time=current_time + timedelta(minutes=duration_mins),
                    root_cause_tag=RootCauseTag.COMPRESSOR_FAILURE,
                )

        # (d) Traffic stop in hot ambient (highway congestion / toll plaza bottleneck)
        if not is_scheduled_stop and not self.active_traffic and ambient_temp_c >= 29.0:
            p_traffic = 1.0 - np.exp(-(0.009 * rate_scale * (dt_seconds / 3600.0)))
            if self.rng.random() < p_traffic:
                duration_mins = float(self.rng.uniform(40.0, 85.0))
                self.active_traffic = ActiveEvent(
                    event_type=GroundTruthEventType.TRAFFIC_STOP,
                    start_time=current_time,
                    end_time=current_time + timedelta(minutes=duration_mins),
                    root_cause_tag=RootCauseTag.LONG_STOP_HOT_AMBIENT,
                )

        door_is_open = self.active_door is not None
        traffic_stopped = self.active_traffic is not None

        return door_is_open, compressor_status, compressor_effectiveness, traffic_stopped

    def register_excursion_start(self, exc_start: datetime) -> None:
        """
        Called at the exact moment temperature leaves safe range.
        Binds the root cause immediately to the active disruptive event,
        or to the most recent event that closed within the thermal lag window.
        """
        candidates = [
            self.active_failure,
            self.active_degradation,
            self.active_door,
            self.active_traffic,
            self.active_precool,
        ]
        self._current_exc_active = None
        self._current_exc_closed_idx = None

        for ev in candidates:
            if ev is not None:
                ev.caused_excursion = True
                if ev.excursion_start is None or exc_start < ev.excursion_start:
                    ev.excursion_start = exc_start
                self._current_exc_active = ev
                return

        # If no disruptive event is actively running, check recently closed events (thermal lag within 25 mins)
        if hasattr(self, "_recent_closed") and self._recent_closed:
            cutoff = exc_start - timedelta(minutes=25)
            tag_map = {
                GroundTruthEventType.DOOR_OPEN: RootCauseTag.DOOR_LEFT_OPEN,
                GroundTruthEventType.COMPRESSOR_FAILURE: RootCauseTag.COMPRESSOR_FAILURE,
                GroundTruthEventType.COMPRESSOR_DEGRADATION: RootCauseTag.COMPRESSOR_DEGRADATION,
                GroundTruthEventType.TRAFFIC_STOP: RootCauseTag.LONG_STOP_HOT_AMBIENT,
                GroundTruthEventType.PRECOOL_MISSING: RootCauseTag.INSUFFICIENT_PRECOOL,
            }
            for ref_type, ref_end, idx in reversed(self._recent_closed):
                if ref_end >= cutoff and ref_type != GroundTruthEventType.SENSOR_FAULT:
                    prev = self.recorded_events[idx]
                    tag = tag_map.get(prev.event_type, RootCauseTag.NONE)
                    self.recorded_events[idx] = GroundTruthEvent(
                        shipment_id=prev.shipment_id,
                        device_id=prev.device_id,
                        event_type=prev.event_type,
                        start_time=prev.start_time,
                        end_time=prev.end_time,
                        caused_excursion=True,
                        excursion_start=prev.excursion_start or (exc_start.isoformat() + "Z"),
                        excursion_end=prev.excursion_end,
                        root_cause_tag=tag,
                    )
                    self._current_exc_closed_idx = idx
                    return

    def register_excursion_end(self, exc_end: datetime) -> None:
        """
        Called when temperature recovers back to the safe range, or at shipment end.
        Records excursion_end on the previously bound root cause event.
        """
        if getattr(self, "_current_exc_active", None) is not None:
            if self._current_exc_active.excursion_end is None or exc_end > self._current_exc_active.excursion_end:
                self._current_exc_active.excursion_end = exc_end
            self._current_exc_active = None
        elif getattr(self, "_current_exc_closed_idx", None) is not None:
            idx = self._current_exc_closed_idx
            if idx < len(self.recorded_events):
                prev = self.recorded_events[idx]
                self.recorded_events[idx] = GroundTruthEvent(
                    shipment_id=prev.shipment_id,
                    device_id=prev.device_id,
                    event_type=prev.event_type,
                    start_time=prev.start_time,
                    end_time=prev.end_time,
                    caused_excursion=prev.caused_excursion,
                    excursion_start=prev.excursion_start,
                    excursion_end=exc_end.isoformat() + "Z",
                    root_cause_tag=prev.root_cause_tag,
                )
            self._current_exc_closed_idx = None

    def register_excursion_interval(self, exc_start: datetime, exc_end: datetime) -> None:
        """Legacy helper for single-call interval registration."""
        self.register_excursion_start(exc_start)
        self.register_excursion_end(exc_end)

    def _close_event(self, ev: ActiveEvent) -> None:
        gt = GroundTruthEvent(
            shipment_id=self.shipment_id,
            device_id=self.device_id,
            event_type=ev.event_type,
            start_time=ev.start_time.isoformat() + "Z",
            end_time=ev.end_time.isoformat() + "Z",
            caused_excursion=ev.caused_excursion,
            excursion_start=ev.excursion_start.isoformat() + "Z" if ev.excursion_start else None,
            excursion_end=ev.excursion_end.isoformat() + "Z" if ev.excursion_end else None,
            root_cause_tag=ev.root_cause_tag if ev.caused_excursion else RootCauseTag.NONE,
        )
        self.recorded_events.append(gt)
        idx = len(self.recorded_events) - 1
        if getattr(self, "_current_exc_active", None) is ev:
            self._current_exc_closed_idx = idx
            self._current_exc_active = None

        if not hasattr(self, "_recent_closed"):
            self._recent_closed = []
        self._recent_closed.append((ev.event_type, ev.end_time, idx))
        if len(self._recent_closed) > 20:
            self._recent_closed.pop(0)

    def finalize(self) -> List[GroundTruthEvent]:
        """Flushes any currently active events to the ground truth record."""
        for ev in [
            self.active_failure,
            self.active_degradation,
            self.active_door,
            self.active_traffic,
            self.active_precool,
        ]:
            if ev is not None:
                self._close_event(ev)

        self.active_failure = None
        self.active_degradation = None
        self.active_door = None
        self.active_traffic = None
        self.active_precool = None

        return list(self.recorded_events)
