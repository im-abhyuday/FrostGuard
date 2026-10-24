# ColdChainIQ - Telemetry Simulator & Historical Dataset Engine

A realistic, physics-grounded IoT sensor simulator for cold chain monitoring. Designed to generate training datasets for 30-minute excursion forecasting and stream real-time telemetry to Kafka/Redpanda.

---

## Architecture Overview

```
                      +---------------------------------------+
                      |       Master Seed (SeedSequence)      |
                      +---------------------------------------+
                                          |
                      +-------------------+-------------------+
                      |                                       |
              Child RNG (Truck 1)                    Child RNG (Truck N)
                      |                                       |
        +-------------+-------------+           +-------------+-------------+
        |   Container Physics ODE   |           |   Container Physics ODE   |
        | - Newton's law of cooling |           | - Newton's law of cooling |
        | - Thermostat hysteresis   |           | - Thermostat hysteresis   |
        | - Compressor states       |           | - Compressor states       |
        | - Ingress from door open  |           | - Ingress from door open  |
        +-------------+-------------+           +-------------+-------------+
                      |                                       |
        +-------------+-------------+           +-------------+-------------+
        |  Event & Fault Injection  |           |  Event & Fault Injection  |
        | - Degraded effectiveness  |           | - Degraded effectiveness  |
        | - Sudden fail / recovery  |           | - Sudden fail / recovery  |
        | - Gaussian noise / drift  |           | - Gaussian noise / drift  |
        | - Drops / Delays / Dupes  |           | - Drops / Delays / Dupes  |
        +-------------+-------------+           +-------------+-------------+
                      \                                       /
                       \------------------+------------------/
                                          |
                                [ Telemetry Sinks ]
                                /                 \
                     [Batch Mode]                [Live Mode]
                          |                           |
                 Parquet & CSV Datasets         Kafka Producer
                 - readings.parquet             - topic: sensor.readings
                 - readings_sample.csv          - key: device_id
                 - ground_truth_events.parquet  - 6 partitions, idempotent
                 - shipments.csv
```

---

## Quickstart & CLI Commands

### 1. Install Dependencies
```bash
pip install -r services/simulator/requirements.txt
```

### 2. Run Test Suite
```bash
pytest services/simulator/tests -v
```

### 3. Generate Historical Batch Dataset
```bash
# Simulates 50 trucks across 30 days (2,160,000 readings in ~90 seconds)
export PYTHONPATH="services/simulator/src"
python -m simulator.cli batch --trucks 50 --days 30 --out-dir ./data/raw
```

### 4. Run Real-Time Streaming to Redpanda / Kafka
```bash
# Start Redpanda broker
docker compose up -d redpanda

# Launch simulator streaming at 60x speed (1 real sec = 1 sim min)
python -m simulator.cli live --trucks 10 --time-scale 60.0 --kafka-bootstrap localhost:9092
```

### 5. Inspect Messages with Redpanda CLI (`rpk`)
```bash
# Inspect first 5 messages from sensor.readings topic
docker exec -it coldchain-redpanda rpk topic consume sensor.readings -n 5
```

### 6. Port Allocation Table
| Service | Container Port | Host Port | Protocol / Listener | Used By |
| :--- | :--- | :--- | :--- | :--- |
| **Redpanda (Kafka)** | `29092` | *None* | `PLAINTEXT` (Internal) | Docker network simulator (`redpanda:29092`) |
| **Redpanda (Kafka)** | `9092` | `9092` | `OUTSIDE` (External) | Host CLI and local producers (`localhost:9092`) |
| **Redpanda (Admin)** | `9644` | `9645` | `HTTP / Admin API` | Cluster health / `rpk` administration |
| **Redpanda Console** | `8080` | `8080` | `HTTP (Web UI)` | Web browser topic/message inspector (`http://localhost:8080`) |
| **Simulator** | *N/A* | *N/A* | Producer Client | Containerized streaming agent |

---

## Schema Contracts

### `SensorReading` (Streamed to Kafka)
| Field | Type | Description |
| :--- | :--- | :--- |
| `schema_version` | `str` | `"1.0"` semantic versioning |
| `event_id` | `str` | UUID4 string unique per emission |
| `shipment_id` | `str` | Shipment identifier (e.g. `SHP-0001-0001`) |
| `device_id` | `str` | Hardware sensor device ID (Kafka message key) |
| `timestamp` | `str` | ISO-8601 UTC timestamp |
| `temperature_c` | `float` | Air temperature inside container ($^\circ\text{C}$) |
| `humidity_pct` | `float` | Relative humidity percentage ($0-100\%$) |
| `door_open` | `bool` | Physical door switch sensor |
| `compressor_on` | `bool` | Active cooling relay status |
| `compressor_status`| `enum` | `OK`, `DEGRADED`, or `FAILED` |
| `ambient_temp_c` | `float` | Exterior highway ambient temperature |
| `latitude`, `longitude`| `float` | WGS-84 GPS coordinates along route |
| `speed_kmph` | `float` | Vehicle ground speed |
| `cargo_type` | `enum` | `VACCINE`, `FROZEN_FOOD`, `DAIRY`, `FRESH_PRODUCE` |
| `setpoint_c` | `float` | Thermostat target temperature |
| `battery_pct` | `float` | Onboard sensor telematics battery percentage |

### `GroundTruthEvent` (Offline Audit / ML Evaluation Only)
| Field | Type | Description |
| :--- | :--- | :--- |
| `shipment_id` | `str` | Associated trip ID |
| `device_id` | `str` | Device hardware ID |
| `event_type` | `enum` | `DOOR_OPEN`, `COMPRESSOR_DEGRADATION`, `COMPRESSOR_FAILURE`, `TRAFFIC_STOP`, `PRECOOL_MISSING`, `SENSOR_FAULT` |
| `start_time` / `end_time` | `str` | Event temporal duration |
| `caused_excursion` | `bool` | True if event breached safe cargo band |
| `root_cause_tag` | `enum` | Causal tag for SHAP attribution sanity checks |

---

## Design Decisions & Physics Realism

1. **Deterministic Multi-Stream RNG**:
   Each truck receives an independent, reproducible RNG stream via `numpy.random.SeedSequence(seed).spawn(N)`. Simulations are 100% bit-for-bit identical across identical seeds.

2. **Per-Truck Heterogeneity & Latent Factors**:
   Real fleets exhibit structural variance in wall foam insulation aging ($\pm 12\%$) and compressor capacity ($\pm 12\%$). Unobserved latent factors (e.g. gasket door seal quality $[0.9, 1.25]$ and cargo load mass factor $[0.85, 1.15]$) affect thermal drift rates without being emitted in sensor telemetry, ensuring ML models cannot exploit trivial deterministic linearities.

3. **Door Infiltration Dynamics & Hard Negatives**:
   Door openings are simulated with lognormal durations (heavier tails at warm ambient stops). Convective infiltration coefficient ($180.0\ \text{W/K}$) ensures that brief openings (1–5 min) remain true hard negatives, while extended openings (>10–15 min) in warm ambient trigger excursions at a realistic 3%–12% rate.

4. **Multi-Cause Excursion Balance**:
   Excursion root causes are balanced across operational failure modes:
   - `insufficient_precool`: $\le 40\%$ (softened offsets so many recover without breaching safe limits)
   - `door_left_open`: $15\% - 25\%$
   - `compressor_failure + compressor_degradation`: $20\% - 30\%$
   - `long_stop_hot_ambient`: $10\% - 20\%$ (traffic stops with condenser airflow loss and ground asphalt heat radiation)

5. **Learnable Precursor Signals**:
   Compressor degradation follows a power-law cooling capacity loss down to $0.08$, generating learnable thermal precursors before complete shutdown. Sudden failures account for $35\% - 40\%$ of all failures, capping recall to realistic production boundaries.

