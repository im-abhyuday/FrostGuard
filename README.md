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

4. **Authentication, JWT & Role-Based Access Control (Phase 3)**:
   - **Argon2id Password Security**: Modern memory-hard password hashing with automatic transparent rehashing on parameter evolution.
   - **OAuth2 & JWT Architecture**: Compliant with OAuth2 password request form for native Swagger UI Authorize workflows; 15-minute access tokens with `HS256` signature verification.
   - **Refresh Token Rotation & Theft Detection**: 7-day single-use opaque tokens with lineage tracking (`family_id`). Replay of revoked tokens immediately terminates the family and invalidates active JWTs via Redis.
   - **Three-Tier Permission Matrix (RBAC)**: Strict role separation between `driver` (assigned shipments only), `manager` (fleet-wide write/admin), and `auditor` (fleet-wide read-only).
   - **IDOR Defense**: Returns `404 Not Found` (never `403`) for unassigned shipments, eliminating resource enumeration vulnerabilities.
   - **Defense-in-Depth Hardening**: Redis sliding-window rate limiting (fail-open), PostgreSQL account lockout (5 failures = 15m), security headers (HSTS, CSP, XFO), request ID tracing, and log credential redaction.

5. **100% Deterministic Reproducibility**:
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
│           ├── db_models.py         # SQLAlchemy 2.0 ORM entities (User, Shipment, Reading, Alert, etc.)
│           ├── enums.py             # Domain enumerations (UserRole, CargoType, RootCauseTag)
│           └── models.py            # Pydantic v2 validation contracts (SensorReading, DLQ)
├── services/
│   ├── api/                         # Backend REST API, Auth & RBAC (Phase 3)
│   │   ├── Dockerfile               # Multi-stage non-root container image with healthcheck
│   │   ├── README.md                # Sequence diagrams, permission matrix, threat models, design decisions
│   │   ├── requirements.txt         # Pinned production and testing dependencies
│   │   ├── migrations/              # Alembic migration revisions (001_initial, 002_auth_and_rbac)
│   │   ├── scripts/
│   │   │   ├── seed_users.py        # Generates baseline manager, auditor, and assigned drivers
│   │   │   └── verify_phase3.py     # End-to-end 11-step QA verification script
│   │   ├── src/api/
│   │   │   ├── core/                # Config, security (Argon2id/JWT), rate limiter, deps, errors, logging
│   │   │   ├── db/                  # Async sessionmaker and model exports
│   │   │   ├── repositories/        # UserRepository, TokenRepository, ShipmentRepository, AuditRepository
│   │   │   ├── routers/             # auth, users, admin, shipments endpoints
│   │   │   ├── schemas/             # Pydantic v2 auth and user schemas
│   │   │   └── services/            # AuthService orchestrating token rotation and business logic
│   │   └── tests/                   # 43 automated pytest integration and unit tests (88% coverage)
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
├── docker-compose.yml               # Postgres 16, Redis 7, Redpanda, Console, Migrate, Consumer, API
├── Makefile                         # Unified operations (up, migrate, seed-users, verify-phase3, etc.)
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
| **API Gateway / RBAC** | `8002` | `8002` | `HTTP (REST)` | Client apps, Swagger UI (`/docs`), ReDoc (`/redoc`) |
| **Redpanda (Kafka)** | `29092` | *None* | `PLAINTEXT` (Internal) | Docker network simulator and consumer (`redpanda:29092`) |
| **Redpanda (Kafka)** | `9092` | `9092` | `OUTSIDE` (External) | Host CLI and local producers (`localhost:9092`) |
| **Redpanda (Admin)** | `9644` | `9645` | `HTTP / Admin API` | Cluster health / `rpk` administration |
| **Redpanda Console** | `8080` | `8080` | `HTTP (Web UI)` | Web browser topic/message inspector (`http://localhost:8080`) |
| **PostgreSQL 16** | `5432` | `5433` | `PostgreSQL Protocol`| Ingestion consumer persistence & API database (`coldchainiq`) |
| **Redis 7** | `6379` | `6380` | `RESP Protocol` | In-memory dedupe filter, latest state, and token denylist |
| **Consumer Health** | `8081` | `8081` | `HTTP` | Container readiness probe and metrics snapshot (`/health`) |

---

## Security Architecture & Threat Model

| Threat Vector | Attack Scenario | ColdChainIQ Mitigation | Status |
|---|---|---|:---:|
| **Account Enumeration on Register** | Attacker probes `/auth/register` to discover registered emails. | Returns `202 Accepted` with generic body `"If this email can be registered, the account has been created. You can now sign in."` for both new and existing emails. Runs dummy Argon2id hash on existing email to achieve <10% timing parity. Race conditions handled via PostgreSQL `uq_users_email_ci ON (lower(email))`. | **FIXED (FIX 1)** |
| **Single Logout during Redis Outage** | User calls `/auth/logout` while Redis is down. | Revokes refresh token in PostgreSQL. Warns loudly in server logs that current access token remains valid until natural expiry (max 15m). | **Documented (FIX 5)** |
| **Privilege Revocation during Redis Outage** | Role changed or user deactivated while Redis is down. | Access tokens carry a `tv` (token_version) claim. PostgreSQL `token_version` increments on role change, deactivation, password reset, and logout-all. `get_current_user` rejects tokens with `tv < user.token_version` even if Redis is unreachable. | **FIXED (FIX 5)** |
| **Credential Stuffing / Brute Force** | Rapid automated password guessing on `/auth/login`. | Redis sliding window (5/min per combo, 20/min per IP) + PostgreSQL account lockout (5 failures = 15m lock). | **Enforced** |
| **User Enumeration on Login** | Measuring response latency to deduce whether an email exists. | Generic error `"Invalid email or password"` for all failures; executes dummy Argon2id hash verification when user does not exist. | **Enforced** |
| **IDOR Probing** | Driver probes unassigned shipment ID (`/shipments/SHP-002`). | Returns `404 Not Found` (never 403), masking existence from unauthorized drivers. | **Enforced** |
| **Refresh Token Replay** | Attacker replays intercepted refresh token. | Lineage-based theft detection: wipes entire token family in PostgreSQL, increments DB `token_version`, and invalidates sessions. | **Enforced** |

---

## Token Lifecycle & Claims Architecture

```text
Access Token Claims:
{
  "sub":  "<UUID>",                   // User identity
  "role": "driver"|"manager"|"auditor",// PostgreSQL native enum user_role_enum
  "tv":   1,                          // User's token_version counter in PostgreSQL
  "jti":  "<UUID>",                   // Unique token identifier
  "type": "access",                   // Token type discriminant
  "iss":  "coldchainiq-api",          // Configured issuer
  "aud":  "coldchainiq-clients",      // Configured audience
  "iat":  1793369000,                 // Issued at epoch seconds
  "exp":  1793369900                  // Expiry epoch (15 minutes lifetime)
}
```

### Revocation Priority Matrix
1. **Single-Device Logout (Fast Path)**: Adds `jti` to Redis denylist with TTL equal to remaining lifetime (`denylist:{jti}`). If Redis is down, refresh token is revoked in PostgreSQL and a loud warning is emitted; access token remains valid for up to 15 minutes.
2. **Account Termination / Role Modification / Logout-All (Authoritative Backstop)**: Atomically increments `users.token_version` in PostgreSQL. Since `get_current_user` loads the user record on authenticated requests, any token with `tv < user.token_version` or missing `tv` is immediately rejected with `401 TOKEN_REVOKED`—even during a complete Redis outage.

---

## Limitations & Operational Tradeoffs

1. **Redis Outage Residual Window on Single Logout**: When Redis is down, single-device logout cannot write to the JTI denylist. The refresh token is revoked immediately in PostgreSQL, preventing renewal, but the currently held 15-minute access token will remain valid until its expiration. Full invalidation under Redis downtime requires calling `/auth/logout-all`, which increments `token_version` in PostgreSQL.
2. **Rate Limiter Fail-Open**: In the event of a Redis outage, the sliding-window rate limiter fails open to prevent denying legitimate telemetry traffic, with PostgreSQL account lockout (`failed_login_count >= 5`) serving as the authoritative brute-force defense.
3. **Database-Managed Enum**: Role values are strictly constrained by PostgreSQL native enum `user_role_enum ('driver', 'manager', 'auditor')`. Modifying roles requires an Alembic migration (`ALTER TYPE`).

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.
