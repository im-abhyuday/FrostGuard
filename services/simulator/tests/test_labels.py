"""
Unit tests for ground-truth event labels, temporal monotonicity, and root cause alignment.
"""

from datetime import datetime
from simulator.config import SimulatorSettings
from simulator.fleet import Fleet
from simulator.models import RootCauseTag


def test_ground_truth_labels_consistency():
    """
    Verifies that all recorded ground-truth events have valid temporal intervals
    and correctly attributed root cause tags when marked as caused_excursion=True.
    """
    settings = SimulatorSettings(
        trucks=3,
        days=2,
        interval_sec=60,
        seed=777,
        excursion_prob=0.15,  # Higher probability to ensure multiple excursions
    )
    fleet = Fleet(settings)

    # Run for 2880 steps (2 days of 60s data)
    for _ in range(1440):
        for truck in fleet.trucks:
            truck.advance_step(dt_seconds=60, is_live_mode=False)

    all_gt = []
    for truck in fleet.trucks:
        _, gt_list = truck.finalize()
        all_gt.extend(gt_list)

    assert len(all_gt) > 0, "No ground truth events recorded"

    for ev in all_gt:
        start_dt = datetime.fromisoformat(ev.start_time.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(ev.end_time.replace("Z", "+00:00"))

        assert start_dt <= end_dt, f"Event start {start_dt} after end {end_dt}"

        if ev.caused_excursion:
            assert ev.root_cause_tag != RootCauseTag.NONE, (
                f"Excursion event {ev.event_type} must not have root_cause_tag=none"
            )
            if ev.excursion_start and ev.excursion_end:
                exc_start = datetime.fromisoformat(ev.excursion_start.replace("Z", "+00:00"))
                exc_end = datetime.fromisoformat(ev.excursion_end.replace("Z", "+00:00"))
                assert exc_start <= exc_end, f"Excursion start {exc_start} after end {exc_end}"
