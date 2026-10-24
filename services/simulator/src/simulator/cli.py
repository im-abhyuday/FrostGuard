"""
Command Line Interface for ColdChainIQ Simulator.
Supports both fast offline batch dataset generation and real-time Kafka streaming.
"""

import json
import logging
import sys
from typing import Optional
from rich.console import Console
from rich.table import Table
import typer

from .config import SimulatorSettings
from .fleet import Fleet
from .sinks import KafkaSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("coldchainiq.cli")
console = Console()

app = typer.Typer(
    name="coldchainiq-simulator",
    help="ColdChainIQ IoT Telemetry Simulator & Historical Dataset Engine",
    add_completion=False,
)


@app.command(name="batch")
def batch(
    trucks: int = typer.Option(50, "--trucks", envvar="SIM_TRUCKS", help="Total trucks in simulated fleet"),
    days: int = typer.Option(30, "--days", envvar="SIM_DAYS", help="Number of simulation days"),
    interval_sec: int = typer.Option(60, "--interval-sec", envvar="SIM_INTERVAL_SEC", help="Physics step dt in seconds"),
    seed: int = typer.Option(42, "--seed", envvar="SIM_SEED", help="Master deterministic RNG seed"),
    excursion_prob: float = typer.Option(
        0.05, "--excursion-prob", envvar="SIM_EXCURSION_PROB", help="Base excursion probability scalar"
    ),
    cargo_mix: str = typer.Option(
        "VACCINE:0.3,DAIRY:0.3,FROZEN_FOOD:0.25,FRESH_PRODUCE:0.15",
        "--cargo-mix",
        envvar="SIM_CARGO_MIX",
        help="Cargo ratio distribution",
    ),
    start_date: str = typer.Option(
        "2026-05-01T00:00:00Z", "--start-date", envvar="SIM_START_DATE", help="Simulation start date (ISO-8601 UTC)"
    ),
    out_dir: str = typer.Option("./data/raw", "--out-dir", envvar="SIM_OUT_DIR", help="Output directory path"),
) -> None:
    """
    Executes fast simulated-time historical dataset generation.
    Emits Parquet datasets, CSV samples, ground-truth events, and validation plots.
    """
    console.print("\n[bold cyan]=== ColdChainIQ: Batch Dataset Generator ===[/bold cyan]")
    settings = SimulatorSettings(
        trucks=trucks,
        days=days,
        interval_sec=interval_sec,
        seed=seed,
        excursion_prob=excursion_prob,
        cargo_mix=cargo_mix,
        start_date=start_date,
        out_dir=out_dir,
    )

    console.print(
        f"Fleet size: [bold]{settings.trucks}[/bold] | "
        f"Timeline: [bold]{settings.days} days[/bold] | "
        f"Step dt: [bold]{settings.interval_sec}s[/bold] | "
        f"Seed: [bold]{settings.seed}[/bold]"
    )

    fleet = Fleet(settings)
    summary = fleet.run_batch(days=days, interval_sec=interval_sec, out_dir=out_dir)

    # Print Pretty Summary Table
    table = Table(title="Dataset Generation & Validation Summary", border_style="green")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="bold white")

    table.add_row("Total Telemetry Rows", f"{summary['total_readings']:,}")
    table.add_row("Completed Shipments", f"{summary['total_shipments']:,}")
    table.add_row("Fleet Size (Devices)", f"{summary['total_devices']}")
    table.add_row("Execution Wall Time", f"{summary['elapsed_wall_seconds']}s")
    table.add_row("Readings Out of Range", f"{summary['excursion_stats']['pct_readings_out_of_range']}%")
    table.add_row(
        "Positive Class Ratio (30m Ahead)",
        f"{summary['excursion_stats']['positive_30min_lookahead_window_pct']}%",
    )
    table.add_row("Ground Truth Events", f"{summary['excursion_stats']['total_ground_truth_events']}")

    console.print(table)
    console.print(f"\n[green]Datasets and validation artifacts saved to [bold]{out_dir}[/bold][/green]\n")


@app.command(name="live")
def live(
    trucks: int = typer.Option(10, "--trucks", envvar="SIM_TRUCKS", help="Total trucks emitting live stream"),
    seed: int = typer.Option(42, "--seed", envvar="SIM_SEED", help="Master deterministic RNG seed"),
    excursion_prob: float = typer.Option(0.05, "--excursion-prob", envvar="SIM_EXCURSION_PROB", help="Excursion probability scalar"),
    cargo_mix: str = typer.Option(
        "VACCINE:0.3,DAIRY:0.3,FROZEN_FOOD:0.25,FRESH_PRODUCE:0.15",
        "--cargo-mix",
        envvar="SIM_CARGO_MIX",
        help="Cargo ratio distribution",
    ),
    start_date: str = typer.Option(
        "2026-05-01T00:00:00Z", "--start-date", envvar="SIM_START_DATE", help="Simulation start date (ISO-8601 UTC)"
    ),
    interval_sec: int = typer.Option(60, "--interval-sec", envvar="SIM_INTERVAL_SEC", help="Physics step dt in seconds"),
    time_scale: float = typer.Option(
        60.0,
        "--time-scale",
        envvar="SIM_TIME_SCALE",
        help="Sim-to-real speed multiplier (60x: 1 real second = 60 sim seconds)",
    ),
    bad_event_rate: float = typer.Option(
        0.002, "--bad-event-rate", envvar="SIM_BAD_EVENT_RATE", help="Rate of corrupt payloads for DLQ testing"
    ),
    force_excursion: Optional[str] = typer.Option(
        None, "--force-excursion", envvar="SIM_FORCE_EXCURSION", help="Device ID to force immediate compressor failure on (e.g. DEV-0001)"
    ),
    kafka_bootstrap: str = typer.Option(
        "localhost:9092", "--kafka-bootstrap", envvar="SIM_KAFKA_BOOTSTRAP", help="Kafka broker bootstrap connection string"
    ),
    kafka_topic: str = typer.Option(
        "sensor.readings", "--kafka-topic", envvar="SIM_KAFKA_TOPIC", help="Destination Kafka topic"
    ),
    duration_sec: Optional[float] = typer.Option(
        None, "--duration", envvar="SIM_DURATION", help="Optional wall-clock duration limit in seconds"
    ),
) -> None:
    """
    Executes live streaming simulation to Kafka/Redpanda topic 'sensor.readings'.
    """
    console.print("\n[bold cyan]=== ColdChainIQ: Live Kafka Telemetry Streamer ===[/bold cyan]")
    settings = SimulatorSettings(
        trucks=trucks,
        seed=seed,
        excursion_prob=excursion_prob,
        cargo_mix=cargo_mix,
        start_date=start_date,
        interval_sec=interval_sec,
        time_scale=time_scale,
        bad_event_rate=bad_event_rate,
        force_excursion=force_excursion,
        kafka_bootstrap=kafka_bootstrap,
        kafka_topic=kafka_topic,
    )

    console.print(
        f"Fleet size: [bold]{settings.trucks}[/bold] | "
        f"Speed: [bold]{settings.time_scale}x[/bold] | "
        f"Broker: [bold]{settings.kafka_bootstrap}[/bold] | "
        f"Topic: [bold]{settings.kafka_topic}[/bold]"
    )
    if force_excursion:
        console.print(f"[bold red]Forcing immediate failure on device: {force_excursion}[/bold red]")

    kafka_sink = KafkaSink(
        bootstrap_servers=settings.kafka_bootstrap,
        topic=settings.kafka_topic,
        num_partitions=settings.kafka_partitions,
    )

    fleet = Fleet(settings)
    fleet.run_live(
        kafka_sink=kafka_sink,
        time_scale=settings.time_scale,
        bad_event_rate=settings.bad_event_rate,
        force_excursion_dev=settings.force_excursion,
        max_duration_seconds=duration_sec,
    )


if __name__ == "__main__":
    app()
