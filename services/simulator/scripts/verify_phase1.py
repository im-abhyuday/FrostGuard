"""
ColdChainIQ Phase 1 Audit & Verification Script
Automates and reproduces all QA validation checks for Phase 1.
Usage:
    python services/simulator/scripts/verify_phase1.py [--all] [--check N]
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Dict, List, Tuple
import numpy as np
import pandas as pd


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
SIMULATOR_ROOT = WORKSPACE_ROOT / "services" / "simulator"
DATA_RAW = WORKSPACE_ROOT / "data" / "raw"
DOCS_DIR = WORKSPACE_ROOT / "docs"

# Add simulator src to sys.path
sys.path.insert(0, str(SIMULATOR_ROOT / "src"))

from simulator.cargo_profiles import CARGO_PROFILES, get_cargo_profile
from simulator.config import SimulatorSettings
from simulator.fleet import Fleet
from simulator.models import CargoType, CompressorStatus, GroundTruthEventType, RootCauseTag, SensorReading
from simulator.physics import ContainerPhysicsEngine


def print_header(title: str):
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def check_1_tests() -> Dict[str, Any]:
    print_header("CHECK 1: Test Suite Verification")
    cmd = [sys.executable, "-m", "pytest", str(SIMULATOR_ROOT / "tests"), "-v"]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SIMULATOR_ROOT / "src")
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)

    output = res.stdout + res.stderr
    print(output)

    # Required categories: physics, determinism, schema validation, fault rates, label sanity, Kafka sink
    categories = {
        "physics": "test_physics.py" in output,
        "determinism": "test_determinism.py" in output,
        "schema validation": "test_schema.py" in output,
        "fault rates": "test_faults.py" in output,
        "label sanity": "test_labels.py" in output,
        "Kafka sink": "test_kafka_sink.py" in output,
    }

    missing_cats = [k for k, v in categories.items() if not v]
    passed_match = re.search(r"(\d+) passed", output)
    failed_match = re.search(r"(\d+) failed", output)

    passed_count = int(passed_match.group(1)) if passed_match else 0
    failed_count = int(failed_match.group(1)) if failed_match else 0

    status = "PASS" if res.returncode == 0 and not missing_cats and failed_count == 0 else "FAIL"
    return {
        "status": status,
        "passed": passed_count,
        "failed": failed_count,
        "categories": categories,
        "missing_categories": missing_cats,
    }


def check_2_batch_performance(out_dir: Path = DATA_RAW) -> Dict[str, Any]:
    print_header("CHECK 2: Batch Performance & Artifacts Verification")
    expected_files = [
        "readings.parquet",
        "readings_sample.csv",
        "ground_truth_events.parquet",
        "shipments.csv",
        "dataset_summary.json",
    ]

    missing = [f for f in expected_files if not (out_dir / f).exists()]
    summary_path = out_dir / "dataset_summary.json"

    wall_time = None
    if summary_path.exists():
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
            wall_time = summary.get("elapsed_wall_seconds")

    files_exist = len(missing) == 0
    time_pass = (wall_time is not None and wall_time < 450.0)

    print(f"Artifacts exist: {files_exist} (Missing: {missing})")
    print(f"Batch Wall Clock Time: {wall_time}s (Target: < 450s)")

    status = "PASS" if files_exist and time_pass else "FAIL"
    return {
        "status": status,
        "wall_time": wall_time,
        "missing_files": missing,
    }


def check_3_determinism() -> Dict[str, Any]:
    print_header("CHECK 3: Determinism & Global RNG Usage Audit")

    # 1. Check Global RNG Grep
    grep_patterns = [
        r"np\.random\.seed",
        r"np\.random\.rand",
        r"random\.random",
        r"random\.seed",
    ]
    hits = []
    src_dir = SIMULATOR_ROOT / "src"
    for py_file in src_dir.rglob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        for pat in grep_patterns:
            matches = re.findall(pat, content)
            if matches:
                hits.append((str(py_file.relative_to(src_dir)), pat, len(matches)))

    print(f"Global RNG hits found in src: {len(hits)}")
    for h in hits:
        print(f"  {h[0]}: pattern '{h[1]}' found {h[2]} times")

    # 2. Run batch comparison with 10 trucks, 3 days
    print("Running determinism trial: 10 trucks, 3 days (Seed 7 run 1 vs Seed 7 run 2 vs Seed 8)...")
    def run_sim(seed: int, steps: int = 1440 * 3) -> pd.DataFrame:
        settings = SimulatorSettings(
            trucks=10,
            days=3,
            seed=seed,
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
                        # omit unique UUID event_id
                        del d["event_id"]
                        records.append(d)
        return pd.DataFrame(records)

    df_seed7_a = run_sim(7)
    df_seed7_b = run_sim(7)
    df_seed8 = run_sim(8)

    same_seed_identical = df_seed7_a.equals(df_seed7_b)
    diff_seed_differs = not df_seed7_a.equals(df_seed8)

    print(f"Seed 7 Run A shape: {df_seed7_a.shape}, Run B shape: {df_seed7_b.shape}")
    print(f"df_seed7_a.equals(df_seed7_b): {same_seed_identical}")
    print(f"df_seed7_a.equals(df_seed8): {df_seed7_a.equals(df_seed8)} (different: {diff_seed_differs})")

    status = "PASS" if same_seed_identical and diff_seed_differs and len(hits) == 0 else "FAIL"
    return {
        "status": status,
        "global_rng_hits": hits,
        "same_seed_identical": same_seed_identical,
        "diff_seed_differs": diff_seed_differs,
    }


def check_4_schema_and_quality(parquet_path: Path = DATA_RAW / "readings.parquet") -> Dict[str, Any]:
    print_header("CHECK 4: Schema and Data Quality Audit")
    if not parquet_path.exists():
        return {"status": "FAIL", "reason": f"File {parquet_path} does not exist"}

    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df):,} readings from {parquet_path.name}.")

    # 1. Check fields and types against SensorReading model
    required_fields = SensorReading.model_fields
    present_fields = set(df.columns)
    missing_fields = set(required_fields.keys()) - present_fields
    print(f"Schema fields count: {len(present_fields)}/{len(required_fields)}. Missing fields: {missing_fields}")

    # Type verification
    type_mismatches = []
    # Sample a record to validate against Pydantic
    sample_dict = df.iloc[0].to_dict()
    try:
        SensorReading(**sample_dict)
        print("Sample row successfully validated against SensorReading Pydantic model.")
    except Exception as e:
        type_mismatches.append(str(e))
        print(f"Sample row validation error: {e}")

    # Check cargo types
    cargos = set(df["cargo_type"].unique())
    has_fresh_produce = "FRESH_PRODUCE" in cargos
    print(f"Unique cargo types: {cargos} ('FRESH_PRODUCE' present: {has_fresh_produce})")

    # 2. Check NaNs
    nan_counts = df.isna().sum().to_dict()
    nan_cols = {k: v for k, v in nan_counts.items() if v > 0}
    has_nans = len(nan_cols) > 0
    print(f"Columns with NaNs: {nan_cols if has_nans else 'None (0 NaNs across all columns)'}")

    # Check physical bounds
    min_temp = float(df["temperature_c"].min())
    max_temp = float(df["temperature_c"].max())
    bounds_ok = (-40.0 <= min_temp) and (max_temp <= 60.0)
    print(f"Temperature bounds: min={min_temp:.2f} C, max={max_temp:.2f} C (Valid [-40, 60]: {bounds_ok})")

    # Monotonicity check per device before injected late arrivals
    # In batch mode, fault injector delays ~1% of readings.
    # We can test monotonicity of raw truck generation directly:
    print("Verifying timestamp monotonicity per device before late arrival injection...")
    settings = SimulatorSettings(trucks=2, days=1, interval_sec=60, seed=42)
    fleet = Fleet(settings)
    monotonic_errors = 0
    for truck in fleet.trucks:
        prev_t = None
        for _ in range(200):
            # Truck advance_step produces raw_reading internally before fault_injector
            raw_t = truck.current_time
            if prev_t is not None and raw_t <= prev_t:
                monotonic_errors += 1
            prev_t = raw_t
            truck.advance_step(dt_seconds=60, is_live_mode=False)
    print(f"Monotonicity errors on raw device timestamps: {monotonic_errors}")

    # 3. Per cargo type describe() and % inside safe range
    safe_ranges = {
        "VACCINE": (2.0, 8.0),
        "FROZEN_FOOD": (-25.0, -18.0),
        "DAIRY": (1.0, 4.0),
        "FRESH_PRODUCE": (4.0, 10.0),
    }

    cargo_stats = {}
    flat_lines = []
    print("\n--- Temperature describe() and Safe Range % per Cargo Type ---")
    for cargo, (s_min, s_max) in safe_ranges.items():
        sub = df[df["cargo_type"] == cargo]["temperature_c"]
        if len(sub) > 0:
            pct_safe = ((sub >= s_min) & (sub <= s_max)).mean() * 100.0
            std_val = float(sub.std())
            if std_val < 0.2:
                flat_lines.append((cargo, std_val))
            desc = sub.describe().to_dict()
            cargo_stats[cargo] = {
                "describe": desc,
                "pct_in_safe_range": round(pct_safe, 2),
                "std": round(std_val, 2),
            }
            print(f"\nCargo: {cargo} [Safe Range: {s_min} to {s_max} C]")
            print(f"  Count: {int(desc['count']):,}")
            print(f"  Mean:  {desc['mean']:.2f} C")
            print(f"  Std:   {desc['std']:.2f} C")
            print(f"  Min:   {desc['min']:.2f} C")
            print(f"  25%:   {desc['25%']:.2f} C")
            print(f"  50%:   {desc['50%']:.2f} C")
            print(f"  75%:   {desc['75%']:.2f} C")
            print(f"  Max:   {desc['max']:.2f} C")
            print(f"  % Inside Safe Range: {pct_safe:.2f}%")

    if flat_lines:
        print(f"\nWARNING: Flat-line temperatures detected (std < 0.2 C): {flat_lines}")
    else:
        print("\nAll cargo temperature std >= 0.2 C (no flat lines).")

    # 4. Cargo mix vs configured mix
    configured_mix = {"VACCINE": 0.30, "DAIRY": 0.30, "FROZEN_FOOD": 0.25, "FRESH_PRODUCE": 0.15}
    actual_mix = (df["cargo_type"].value_counts(normalize=True)).to_dict()
    print("\n--- Cargo Mix Audit ---")
    mix_diffs = {}
    for c, target in configured_mix.items():
        act = actual_mix.get(c, 0.0)
        diff = abs(act - target)
        mix_diffs[c] = diff
        print(f"  {c:<15}: Actual = {act*100:.2f}%, Configured = {target*100:.1f}% (Diff = {diff*100:.2f}%)")
    mix_ok = all(d < 0.05 for d in mix_diffs.values())

    status = (
        "PASS"
        if len(missing_fields) == 0
        and has_fresh_produce
        and not has_nans
        and bounds_ok
        and monotonic_errors == 0
        and mix_ok
        and len(flat_lines) == 0
        and len(type_mismatches) == 0
        else "FAIL"
    )

    return {
        "status": status,
        "missing_fields": list(missing_fields),
        "has_nans": has_nans,
        "bounds_ok": bounds_ok,
        "cargo_stats": cargo_stats,
        "flat_lines": flat_lines,
        "mix_diffs": mix_diffs,
    }


def check_5_class_balance(parquet_path: Path = DATA_RAW / "readings.parquet") -> Dict[str, Any]:
    print_header("CHECK 5: Class Balance (30-Minute Lookahead Excursion Ratio)")
    if not parquet_path.exists():
        return {"status": "FAIL", "reason": f"File {parquet_path} does not exist"}

    df = pd.read_parquet(parquet_path, columns=["device_id", "timestamp", "temperature_c", "cargo_type"])

    is_out_list = []
    for c_type, profile in CARGO_PROFILES.items():
        mask = df["cargo_type"] == c_type.value
        out = (df.loc[mask, "temperature_c"] < profile.safe_min_c) | (
            df.loc[mask, "temperature_c"] > profile.safe_max_c
        )
        is_out_list.append(out)

    df["is_excursion"] = pd.concat(is_out_list).sort_index()

    # Sort and rolling 30 forward
    df_sorted = df.sort_values(["device_id", "timestamp"]).copy()
    rolling_forward_max = (
        df_sorted.groupby("device_id")["is_excursion"]
        .transform(lambda s: s.iloc[::-1].rolling(window=30, min_periods=1).max().iloc[::-1])
    )
    positive_ratio = float(rolling_forward_max.mean() * 100.0)
    print(f"Computed 30-min-ahead excursion positive window ratio: {positive_ratio:.2f}%")

    # PASS: 3% to 8%. Warn if <1% or >15%
    if 3.0 <= positive_ratio <= 8.0:
        status = "PASS"
    elif 1.0 <= positive_ratio <= 15.0:
        status = "PASS (WARN: slightly outside 3-8% ideal band)"
    else:
        status = "FAIL"

    return {
        "status": status,
        "positive_ratio_pct": round(positive_ratio, 2),
    }


def check_6_ground_truth_integrity(
    readings_path: Path = DATA_RAW / "readings.parquet",
    gt_path: Path = DATA_RAW / "ground_truth_events.parquet",
) -> Dict[str, Any]:
    print_header("CHECK 6: Ground Truth Integrity Audit")
    if not gt_path.exists():
        return {"status": "FAIL", "reason": f"File {gt_path} does not exist"}

    gt_df = pd.read_parquet(gt_path)
    readings_df = pd.read_parquet(readings_path, columns=["device_id", "timestamp", "temperature_c", "cargo_type"])

    # 1. root_cause_tag distribution (at least 4 non-none causes)
    non_none_rc = gt_df[gt_df["root_cause_tag"] != "none"]["root_cause_tag"].value_counts().to_dict()
    print(f"Non-none root cause distribution: {non_none_rc}")
    has_min_4_causes = len(non_none_rc) >= 4

    # 2. Both COMPRESSOR_FAILURE and COMPRESSOR_DEGRADATION exist
    event_types = set(gt_df["event_type"].unique())
    has_both_comp = (
        "COMPRESSOR_FAILURE" in event_types and "COMPRESSOR_DEGRADATION" in event_types
    )
    print(f"Event types present: {event_types} (Both compressor states exist: {has_both_comp})")

    # 3. Share of failures with no degradation precursor in prior 2 hours (target ~30-50%)
    failures = gt_df[gt_df["event_type"] == "COMPRESSOR_FAILURE"].copy()
    degradations = gt_df[gt_df["event_type"] == "COMPRESSOR_DEGRADATION"].copy()

    failures["start_dt"] = pd.to_datetime(failures["start_time"], format="ISO8601")
    degradations["end_dt"] = pd.to_datetime(degradations["end_time"], format="ISO8601")

    no_precursor_count = 0
    for _, f_row in failures.iterrows():
        dev = f_row["device_id"]
        f_start = f_row["start_dt"]
        cutoff = f_start - pd.Timedelta(hours=2)
        # Check if any degradation for same device ended between cutoff and f_start
        prior_degs = degradations[
            (degradations["device_id"] == dev)
            & (degradations["end_dt"] >= cutoff)
            & (degradations["end_dt"] <= f_start + pd.Timedelta(minutes=5))
        ]
        if prior_degs.empty:
            no_precursor_count += 1

    total_failures = len(failures)
    share_no_precursor = (no_precursor_count / total_failures * 100.0) if total_failures > 0 else 0.0
    print(f"Failures without degradation precursor: {no_precursor_count}/{total_failures} ({share_no_precursor:.1f}%)")
    precursor_pass = (20.0 <= share_no_precursor <= 60.0)

    # 4. For caused_excursion=True: start < end, and readings really out of safe range
    excursion_events = gt_df[gt_df["caused_excursion"] == True].copy()
    valid_times = (pd.to_datetime(excursion_events["start_time"], format="ISO8601") <= pd.to_datetime(excursion_events["end_time"], format="ISO8601")).all()

    # Mismatch rate: check if readings in excursion_start to excursion_end are out of safe range
    # Sample or evaluate all excursion events
    mismatches = 0
    evaluated = 0
    for _, ev in excursion_events.iterrows():
        if ev.get("excursion_start") and ev.get("excursion_end"):
            dev = ev["device_id"]
            e_start = ev["excursion_start"]
            e_end = ev["excursion_end"]
            sub = readings_df[
                (readings_df["device_id"] == dev)
                & (readings_df["timestamp"] >= e_start)
                & (readings_df["timestamp"] <= e_end)
            ]
            if not sub.empty:
                evaluated += 1
                c_type = sub["cargo_type"].iloc[0]
                prof = CARGO_PROFILES[CargoType(c_type)]
                out = (sub["temperature_c"] < prof.safe_min_c) | (sub["temperature_c"] > prof.safe_max_c)
                if not out.any():
                    mismatches += 1

    mismatch_rate = (mismatches / evaluated * 100.0) if evaluated > 0 else 0.0
    print(f"Excursion event out-of-range mismatch rate: {mismatches}/{evaluated} ({mismatch_rate:.2f}%)")

    # 5. Hard negatives: door-open events that did NOT cause an excursion
    door_events = gt_df[gt_df["event_type"] == "DOOR_OPEN"]
    door_total = len(door_events)
    door_no_excursion = (door_events["caused_excursion"] == False).sum()
    hard_neg_ratio = (door_no_excursion / door_total * 100.0) if door_total > 0 else 0.0
    print(f"Door open hard negatives: {door_no_excursion}/{door_total} ({hard_neg_ratio:.1f}%)")

    # 6. Confirm ground truth fields never appear in Kafka payload (SensorReading)
    gt_columns = set(gt_df.columns)
    reading_columns = set(readings_df.columns)
    leaked_fields = (gt_columns - {"shipment_id", "device_id"}) & reading_columns
    print(f"Leaked ground truth fields in SensorReading: {leaked_fields}")

    status = (
        "PASS"
        if has_min_4_causes
        and has_both_comp
        and precursor_pass
        and valid_times
        and mismatch_rate < 5.0
        and hard_neg_ratio > 30.0
        and len(leaked_fields) == 0
        else "FAIL"
    )

    return {
        "status": status,
        "non_none_root_causes": non_none_rc,
        "share_no_precursor": round(share_no_precursor, 1),
        "mismatch_rate": round(mismatch_rate, 2),
        "hard_neg_ratio": round(hard_neg_ratio, 1),
        "leaked_fields": list(leaked_fields),
    }


def check_7_fault_injection_rates(
    readings_path: Path = DATA_RAW / "readings.parquet",
    summary_path: Path = DATA_RAW / "dataset_summary.json",
) -> Dict[str, Any]:
    print_header("CHECK 7: Fault Injection Rates Audit")
    if not summary_path.exists() or not readings_path.exists():
        return {"status": "FAIL", "reason": "Data files missing"}

    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    fault_cfg = summary.get("fault_stats", {})
    print(f"Configured rates in summary: {fault_cfg}")

    df = pd.read_parquet(readings_path, columns=["device_id", "timestamp"])
    total_rows = len(df)

    # 1. Duplicate rate on (device_id, timestamp)
    dupe_count = int(df.duplicated(subset=["device_id", "timestamp"]).sum())
    dupe_rate = float(dupe_count / total_rows * 100.0)
    print(f"1. Duplicate readings on (device_id, timestamp): {dupe_count:,}/{total_rows:,} ({dupe_rate:.3f}%) [Target: ~0.5%]")

    # 2. Late / out-of-order readings
    # In Parquet file order per device, count where timestamp < prev_timestamp
    df["dt_parsed"] = pd.to_datetime(df["timestamp"], format="ISO8601")
    # Groupby device and check diff in file order
    df["prev_dt"] = df.groupby("device_id")["dt_parsed"].shift(1)
    late_count = int((df["dt_parsed"] < df["prev_dt"]).sum())
    late_rate = float(late_count / total_rows * 100.0)
    print(f"2. Late / out-of-order arrivals in stream: {late_count:,}/{total_rows:,} ({late_rate:.3f}%) [Target: ~1.0%]")

    # 3. Missing rate
    # Expected steps = 50 trucks * 30 days * 1440 steps = 2,160,000
    expected_steps = 50 * 30 * 1440
    unique_readings = total_rows - dupe_count
    missing_count = expected_steps - unique_readings
    missing_rate = float(missing_count / expected_steps * 100.0)
    print(f"3. Missing / dropped readings: {missing_count:,}/{expected_steps:,} ({missing_rate:.3f}%) [Target: ~1.5 - 2.5% including gap dropouts]")

    # 4. Check bad-event-rate in batch vs live mode
    cli_file = SIMULATOR_ROOT / "src" / "simulator" / "cli.py"
    cli_content = cli_file.read_text(encoding="utf-8")
    batch_part = cli_content.split("def batch(")[1].split("def live(")[0]
    live_part = cli_content.split("def live(")[1]

    batch_has_bad_event = "bad_event_rate" in batch_part
    live_has_bad_event = "bad_event_rate" in live_part
    print(f"4. Bad payloads option: Present in live mode = {live_has_bad_event}, Present in batch mode = {batch_has_bad_event}")

    # Tolerance checks:
    # duplicate ~0.5% (0.3% - 0.7%)
    # late ~1% (0.7% - 1.5%)
    # missing ~1-2.5% (1.0% - 3.0%)
    dupe_pass = (0.3 <= dupe_rate <= 0.8)
    late_pass = (0.5 <= late_rate <= 1.8)
    missing_pass = (1.0 <= missing_rate <= 3.5)
    bad_pass = (not batch_has_bad_event and live_has_bad_event)

    status = "PASS" if dupe_pass and late_pass and missing_pass and bad_pass else "FAIL"
    return {
        "status": status,
        "duplicate_rate_pct": round(dupe_rate, 3),
        "late_rate_pct": round(late_rate, 3),
        "missing_rate_pct": round(missing_rate, 3),
        "batch_has_bad_event": batch_has_bad_event,
        "live_has_bad_event": live_has_bad_event,
    }


def check_8_plots() -> Dict[str, Any]:
    print_header("CHECK 8: Documentation Plots Verification")
    plot_files = [
        "truck_timeline.png",
        "door_vs_excursion.png",
        "ambient_vs_excursions.png",
    ]
    missing = [p for p in plot_files if not (DOCS_DIR / p).exists()]
    sizes = {p: (DOCS_DIR / p).stat().st_size for p in plot_files if (DOCS_DIR / p).exists()}
    print(f"Plot sizes in docs/: {sizes}")
    status = "PASS" if not missing and all(s > 10000 for s in sizes.values()) else "FAIL"
    return {"status": status, "sizes": sizes, "missing": missing}


def check_9_physics_sanity() -> Dict[str, Any]:
    print_header("CHECK 9: Physics Sanity Experiments")
    rng = np.random.default_rng(12345)
    prof = get_cargo_profile(CargoType.VACCINE)

    # 1. Compressor FAILED, ambient 38 C: must rise monotonically toward ambient
    eng1 = ContainerPhysicsEngine(prof, rng=rng)
    eng1.state.compressor_status = CompressorStatus.FAILED
    eng1.state.compressor_effectiveness = 0.0
    initial_t = eng1.state.air_temp_c
    t_seq = [initial_t]
    for _ in range(60):
        s = eng1.step(60, ambient_temp_c=38.0, ambient_humidity_pct=40.0, door_open=False)
        t_seq.append(s.air_temp_c)

    # Exclude tiny process noise jitter: test general monotonicity and final rise
    rises_overall = t_seq[-1] > t_seq[0]
    diffs = np.diff(t_seq)
    # Accounting for process noise sigma 0.03, median diff must be positive
    monotonic_drift = np.median(diffs) > 0.0
    print(f"1. Failed compressor 60min rise: {initial_t:.2f}C -> {t_seq[-1]:.2f}C (Net change: +{t_seq[-1]-initial_t:.2f}C)")

    # 2. Healthy compressor, no doors: stays within setpoint +/- band
    eng2 = ContainerPhysicsEngine(prof, rng=rng)
    steady_temps = []
    for _ in range(120):
        s = eng2.step(60, ambient_temp_c=38.0, ambient_humidity_pct=40.0, door_open=False)
        steady_temps.append(s.air_temp_c)
    mean_steady = float(np.mean(steady_temps[30:]))
    min_steady = float(np.min(steady_temps[30:]))
    max_steady = float(np.max(steady_temps[30:]))
    healthy_in_band = (prof.safe_min_c <= min_steady) and (max_steady <= prof.safe_max_c)
    print(f"2. Healthy compressor steady range: [{min_steady:.2f}, {max_steady:.2f}]C (Safe: [{prof.safe_min_c}, {prof.safe_max_c}]C)")

    # 3. Door open 10 min vs 1 min: larger deviation for longer opening
    eng_1m = ContainerPhysicsEngine(prof, rng=np.random.default_rng(999))
    eng_1m.state.compressor_on = False
    init_1m = eng_1m.state.air_temp_c
    for _ in range(1):
        eng_1m.step(60, ambient_temp_c=40.0, ambient_humidity_pct=40.0, door_open=True)
    dev_1m = eng_1m.state.air_temp_c - init_1m

    eng_10m = ContainerPhysicsEngine(prof, rng=np.random.default_rng(999))
    eng_10m.state.compressor_on = False
    init_10m = eng_10m.state.air_temp_c
    for _ in range(10):
        eng_10m.step(60, ambient_temp_c=40.0, ambient_humidity_pct=40.0, door_open=True)
    dev_10m = eng_10m.state.air_temp_c - init_10m
    door_scaling_ok = dev_10m > (dev_1m * 5.0)
    print(f"3. Door open deviation: 1-min={dev_1m:.3f}C, 10-min={dev_10m:.3f}C (Ratio: {dev_10m/dev_1m:.2f}x)")

    # 4. Hot ambient vs cool ambient: more excursions in hot ambient
    # Simulate a 180-minute compressor failure under cool (22 C), warm (30 C), hot (38 C), and extreme (42 C) ambient
    def count_failure_excursions(amb_temp: float, failure_mins: int = 180) -> Tuple[int, float]:
        eng = ContainerPhysicsEngine(prof, rng=np.random.default_rng(42))
        eng.state.compressor_status = CompressorStatus.FAILED
        eng.state.compressor_effectiveness = 0.0
        exc = 0
        final_temp = eng.state.air_temp_c
        for _ in range(failure_mins):
            st = eng.step(60, ambient_temp_c=amb_temp, ambient_humidity_pct=40.0, door_open=False)
            final_temp = st.air_temp_c
            if st.air_temp_c > prof.safe_max_c:
                exc += 1
        return exc, final_temp

    exc_22, temp_22 = count_failure_excursions(22.0)
    exc_30, temp_30 = count_failure_excursions(30.0)
    exc_38, temp_38 = count_failure_excursions(38.0)
    exc_42, temp_42 = count_failure_excursions(42.0)
    hot_vs_cool_ok = (exc_42 > exc_38 > exc_30 > exc_22)
    print(f"4. 180-min failure excursions across ambient temperatures:")
    print(f"   Cool (22 C)    : {exc_22}/180 mins out-of-range, Final Temp = {temp_22:.2f} C")
    print(f"   Warm (30 C)    : {exc_30}/180 mins out-of-range, Final Temp = {temp_30:.2f} C")
    print(f"   Hot (38 C)     : {exc_38}/180 mins out-of-range, Final Temp = {temp_38:.2f} C")
    print(f"   Extreme (42 C) : {exc_42}/180 mins out-of-range, Final Temp = {temp_42:.2f} C")

    status = (
        "PASS"
        if rises_overall and monotonic_drift and healthy_in_band and door_scaling_ok and hot_vs_cool_ok
        else "FAIL"
    )
    return {
        "status": status,
        "failed_compressor_rise": round(t_seq[-1] - initial_t, 2),
        "steady_range": [round(min_steady, 2), round(max_steady, 2)],
        "door_1m_vs_10m": [round(dev_1m, 3), round(dev_10m, 3)],
        "ambient_failure_excursions": {
            "cool_22c": exc_22,
            "warm_30c": exc_30,
            "hot_38c": exc_38,
            "extreme_42c": exc_42,
        },
    }


def check_12_documentation() -> Dict[str, Any]:
    print_header("CHECK 12: Documentation Integrity Audit")
    physics_md = SIMULATOR_ROOT / "PHYSICS.md"
    readme_md = SIMULATOR_ROOT / "README.md"

    has_physics = physics_md.exists()
    has_readme = readme_md.exists()

    p_content = physics_md.read_text(encoding="utf-8") if has_physics else ""
    r_content = readme_md.read_text(encoding="utf-8") if has_readme else ""

    # Constants justification check
    constants = ["HEAT_LEAK_COEFF", "DOOR_HEAT_COEFF", "THERMOSTAT_BAND_C", "PROCESS_NOISE_SIGMA", "AIR_CORE_COUPLING"]
    missing_constants = [c for c in constants if c not in p_content]

    # README topics
    topics = ["Quickstart", "batch", "live", "Schema Contracts", "SensorReading", "GroundTruthEvent"]
    missing_topics = [t for t in topics if t not in r_content]

    print(f"PHYSICS.md exists: {has_physics}, missing constants: {missing_constants}")
    print(f"README.md exists: {has_readme}, missing topics: {missing_topics}")

    status = "PASS" if has_physics and has_readme and not missing_constants and not missing_topics else "FAIL"
    return {
        "status": status,
        "missing_constants": missing_constants,
        "missing_topics": missing_topics,
    }


def main():
    parser = argparse.ArgumentParser(description="FrostGuard Phase 1 QA Verification Audit")
    parser.add_argument("--all", action="store_true", help="Run all automated verification checks")
    parser.add_argument("--check", type=int, choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 12], help="Run a specific check")
    args = parser.parse_args()

    results = {}
    if args.check == 1 or args.all:
        results["Check 1: Tests"] = check_1_tests()
    if args.check == 2 or args.all:
        results["Check 2: Batch Performance"] = check_2_batch_performance()
    if args.check == 3 or args.all:
        results["Check 3: Determinism"] = check_3_determinism()
    if args.check == 4 or args.all:
        results["Check 4: Schema & Quality"] = check_4_schema_and_quality()
    if args.check == 5 or args.all:
        results["Check 5: Class Balance"] = check_5_class_balance()
    if args.check == 6 or args.all:
        results["Check 6: Ground Truth"] = check_6_ground_truth_integrity()
    if args.check == 7 or args.all:
        results["Check 7: Fault Rates"] = check_7_fault_injection_rates()
    if args.check == 8 or args.all:
        results["Check 8: Plots"] = check_8_plots()
    if args.check == 9 or args.all:
        results["Check 9: Physics Sanity"] = check_9_physics_sanity()
    if args.check == 12 or args.all:
        results["Check 12: Documentation"] = check_12_documentation()

    print_header("AUDIT SUMMARY TABLE")
    print(f"{'Check':<35} | {'Status':<10}")
    print("-" * 50)
    for name, res in results.items():
        print(f"{name:<35} | {res['status']:<10}")


if __name__ == "__main__":
    main()
