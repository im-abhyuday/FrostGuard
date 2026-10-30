# FrostGuard (ColdChainIQ) ❄️🚛

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Kafka / Redpanda](https://img.shields.io/badge/streaming-Redpanda%20%2F%20Kafka-red.svg)](https://redpanda.com/)
[![PostgreSQL 16](https://img.shields.io/badge/storage-PostgreSQL%2016-blue.svg)](https://www.postgresql.org/)
[![Redis 7](https://img.shields.io/badge/cache-Redis%207-red.svg)](https://redis.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**ColdChainIQ** is an enterprise-grade IoT cold chain telematics and predictive excursion detection platform designed for pharmaceutical and perishable freight logistics.

It pairs a high-fidelity digital twin (modeling 1st-order thermodynamic differential equations, thermostat hysteresis, vehicle dynamics along major Indian highway freight corridors, and stochastic mechanical failures) with real-time distributed stream processing (Kafka/Redpanda -> Consumer -> Redis 7 -> PostgreSQL 16) and supervised early-warning ML pipelines.

---

## Architecture Overview

```mermaid
flowchart LR
    subgraph Phase1 ["Phase 1: Edge Digital Twin & Simulator"]
        A[Fleet Manager<br/>50 Refrigerated Reefers] --> B[Thermodynamic ODE Engine<br/>Euler dt=60s]
        B --> C[Disruptive Events<br/>Door, Traffic, Degradation, Failure]
        C --> D[Sensor Fault Injector<br/>Noise, Latency, Loss, Dupes]
    end

    subgraph Streaming ["Distributed Message Broker (Redpanda / Kafka)"]
        D -->|JSON / Key=device_id| E[Topic: sensor.readings<br/>6 Partitions]
        DLQ[Topic: sensor.dlq<br/>3 Partitions / Dead-Letter Queue]
        E --> F[Redpanda Console<br/>Web UI: 8080]
    end

    subgraph Phase2 ["Phase 2: Streaming Ingestion Pipeline"]
        E --> G[Ingestion Consumer Worker<br/>Manual Offset Commit]
        G -- Malformed Payloads --> DLQ
        G --> H[(PostgreSQL 16)<br/>Authoritative Persistence<br/>ON CONFLICT DO NOTHING]
        G --> I[(Redis 7 Cache)<br/>Atomic Lua Latest State<br/>Rolling ZSET Window 60m]
    end

    subgraph Downstream ["Phases 3-5: Features & Predictive ML"]
        I --> J[Feature Engineering Engine]
        H --> K[Historical Dataset Loader]
        J --> L[Predictive Classifier<br/>XGBoost / LightGBM]
        L --> M[Attribution & Alerting Dashboard]
    end
```

---

## Key Features

1. **High-Fidelity Thermodynamic Digital Twin (Phase 1)**:
   - Explicit first-order Euler integration modeling ambient conductive heat leak ($18.0\ \text{W/K}$), door opening convective displacement ($180.0\ \text{W/K}$), solar highway gain, and compressor cooling curves.
   - Core-to-air thermal mass buffering across 4 specialized cargo profiles: **Vaccines** ($2^\circ\text{C} - 8^\circ\text{C}$), **Frozen Foods** ($-25^\circ\text{C} - -18^\circ\text{C}$), **Dairy** ($1^\circ\text{C} - 4^\circ\text{C}$), and **Fresh Produce** ($4^\circ\text{C} - 10^\circ\text{C}$).
   - Per-truck structural heterogeneity ($\pm 12\%$ insulation/compressor variance) and unobserved latent factors (gasket seal wear, cargo load factor).

2. **Realistic Operational Failure Modes & Hard Negatives**:
   - **Door Infiltration**: Lognormal duration tails at stops calibrated so that short openings (1–5 min) remain true hard negatives (95.1% no-excursion rate), with an overall excursion rate of ~5%.
   - **Compressor Degradation**: Gradual power-law degradation providing learnable precursor patterns before complete failure.
   - **Sudden Compressor Failures**: Calibrated to ~32% of failure events without precursors to cap recall boundaries realistically.

3. **High-Throughput Streaming Ingestion Pipeline (Phase 2)**:
   - **At-Least-Once Delivery to Exactly-Once Storage**: Consumes from topic `sensor.readings` using `confluent-kafka` with manual synchronous offset commits performed **only after** PostgreSQL and Redis writes durably succeed.
   - **Authoritative Database Idempotency**: PostgreSQL `readings` table enforces `UNIQUE (device_id, ts)` via `INSERT ... ON CONFLICT (device_id, ts) DO NOTHING`.
   - **Redis Fast-Path Deduplication**: Redis `SET dedupe:{device_id}:{ts} 1 NX EX 7200` filters retransmitted duplicates in-memory.
   - **Atomic Lua Latest State**: Evaluates timestamp epochs atomically in Redis (`HSET device:{device_id}:latest`) to guarantee late-arriving packets never overwrite fresher state.
   - **60-Minute Rolling Feature Window**: ZSET `device:{device_id}:window` indexed by event epoch seconds and trimmed automatically for Phase 5 real-time ML feature inference.
   - **Dead-Letter Queue (DLQ)**: Validates messages via Pydantic + physical domain bounds; sends corrupted payloads with diagnostic error metadata to `sensor.dlq`.

4. **100% Deterministic Reproducibility**:
   - Master deterministic RNG splitting via `numpy.random.SeedSequence(seed).spawn(N)` guaranteeing identical bit-for-bit telemetry across runs with identical seeds.

---

## Directory Structure

```text
FrostGuard/
├── data/
│   └── raw/                         # Historical datasets (readings.parquet, shipments.csv, etc.)
├── docs/                            # Validation reports and diagnostic figures
│   ├── ambient_vs_excursions.png
│   ├── door_vs_excursion.png
│   ├── truck_timeline.png
│   └── validation_report.md
├── libs/
│   └── coldchain_common/            # Shared canonical models, enums, and database schemas
│       ├── pyproject.toml
│       └── src/coldchain_common/
│           ├── db_models.py         # SQLAlchemy 2.0 ORM entities (Shipment, Device, Reading, Alert)
│           ├── enums.py             # Domain enumerations (CargoType, RootCauseTag, etc.)
│           └── models.py            # Pydantic v2 validation contracts (SensorReading, DLQ)
├── services/
│   ├── api/                         # Backend API and database migrations
│   │   ├── alembic.ini
│   │   ├── migrations/              # Alembic migration scripts
│   │   └── src/api/db/models.py
│   ├── consumer/                    # High-throughput streaming ingestion pipeline (Phase 2)
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── README.md                # In-depth architectural design, Redis keys, failure modes
│   │   ├── scripts/
│   │   │   └── verify_phase2.py     # Automated Phase 2 QA verification suite
│   │   ├── src/consumer/
│   │   │   ├── batching.py          # Micro-batcher (500 events / 1000ms threshold)
│   │   │   ├── config.py            # Pydantic-settings configuration
│   │   │   ├── dedupe.py            # Redis SET NX fast-path deduplicator
│   │   │   ├── kafka_client.py      # Consumer & DLQ producer factories with rebalance hooks
│   │   │   ├── main.py              # Entrypoint, signal handling, HTTP /health server
│   │   │   ├── metrics.py           # In-process counters, throughput rates, consumer lag
│   │   │   ├── pipeline.py          # Main stream loop (Poll -> Validate -> Write -> Commit)
│   │   │   ├── redis_store.py       # Atomic Lua latest state + 60m ZSET window
│   │   │   ├── repository.py        # Sync SQLAlchemy 2.0 + psycopg v3 idempotent inserts
│   │   │   ├── schemas.py           # Re-exported schema contracts
│   │   │   └── validation.py        # Schema and physical domain constraint validator
│   │   └── tests/                   # Pytest unit & integration test suite (21 tests)
│   └── simulator/                   # IoT Sensor Simulator & Telematics Generator (Phase 1)
│       ├── Dockerfile
│       ├── PHYSICS.md               # First-order Euler thermal equations & constant justifications
│       ├── README.md                # Simulator operational documentation
│       ├── requirements.txt
│       ├── scripts/
│       │   └── verify_phase1.py     # Automated Phase 1 QA verification suite
│       ├── src/simulator/           # Core simulator engine
│       └── tests/                   # Pytest test suite (11 unit and integration tests)
├── docker-compose.yml               # Postgres 16, Redis 7, Redpanda, Console, Migrate, Consumer
├── Makefile                         # Unified operations (up, migrate, consumer-up, verify, etc.)
├── .env.example
└── README.md
```

---

## Quickstart Guide

### 1. Prerequisites
- Python 3.11+
- Docker and Docker Compose
- Make (optional, commands can be run directly)

### 2. Environment Setup
```bash
# Clone the repository
git clone https://github.com/im-abhyuday/FrostGuard.git
cd FrostGuard

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install shared package in editable mode
pip install -e libs/coldchain_common

# Install simulator and consumer dependencies
pip install -r services/simulator/requirements.txt
pip install -r services/consumer/requirements.txt
pip install -e services/consumer
```

### 3. Launch Foundational Infrastructure (Docker)
```bash
# Start PostgreSQL, Redis, Redpanda (Kafka), and Web Console
make up
# Or directly: docker compose up -d postgres redis redpanda console
```

### 4. Run Database Schema Migrations
```bash
# Run Alembic migrations against PostgreSQL
make migrate
# Or directly: docker compose run --rm migrate
```

### 5. Start Streaming Telemetry Ingestion Consumer
```bash
# Start containerized consumer
make consumer-up

# Follow live ingestion throughput and metrics
make consumer-logs
```

### 6. Run Automated QA Verification Suites
```bash
# Phase 1 Verification (Physics, Determinism, Batch Quality)
python services/simulator/scripts/verify_phase1.py --all

# Phase 2 Verification (Consumer, Dedupe, Late Data, Redis Window, Tests)
python services/consumer/scripts/verify_phase2.py --all
```

---

## Port Allocation Table

| Service | Container Port | Host Port | Protocol / Listener | Used By |
| :--- | :--- | :--- | :--- | :--- |
| **Redpanda (Kafka)** | `29092` | *None* | `PLAINTEXT` (Internal) | Docker network simulator and consumer (`redpanda:29092`) |
| **Redpanda (Kafka)** | `9092` | `9092` | `OUTSIDE` (External) | Host CLI and local producers (`localhost:9092`) |
| **Redpanda (Admin)** | `9644` | `9645` | `HTTP / Admin API` | Cluster health / `rpk` administration |
| **Redpanda Console** | `8080` | `8080` | `HTTP (Web UI)` | Web browser topic/message inspector (`http://localhost:8080`) |
| **PostgreSQL 16** | `5432` | `5432` | `PostgreSQL Protocol`| Ingestion consumer persistence & API database |
| **Redis 7** | `6379` | `6379` | `RESP Protocol` | In-memory dedupe filter, latest state, and feature window |
| **Consumer Health** | `8081` | `8081` | `HTTP` | Container readiness probe and metrics snapshot (`/health`) |

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.
