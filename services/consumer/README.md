# ColdChainIQ Streaming Telemetry Ingestion Pipeline (Phase 2)

High-throughput, fault-tolerant streaming consumer bridging Apache Kafka (Redpanda), Redis, and PostgreSQL with Dead-Letter Queue (DLQ) isolation.

---

## Architecture Diagram

```mermaid
flowchart TD
    subgraph Ingestion ["Message Broker (Kafka / Redpanda)"]
        A[Topic: sensor.readings<br/>6 Partitions / Key: device_id]
        DLQ[Topic: sensor.dlq<br/>3 Partitions / Dead-Letter Queue]
    end

    subgraph ConsumerWorker ["Ingestion Consumer Worker"]
        B[Kafka Consumer<br/>Manual Offset Commit] --> C[Event Validator<br/>Pydantic + Domain Checks]
        C -- Invalid Event --> D[DLQ Publisher<br/>Preserve raw bytes + error] --> DLQ
        C -- Valid Event --> E[Micro-Batch Accumulator<br/>500 msgs or 1000ms]
        E --> F[Redis Dedupe Fast-Path<br/>SET dedupe:dev:ts NX EX 2h]
    end

    subgraph Storage ["Durable & Cache Storage"]
        F --> G[(PostgreSQL 16)<br/>Authoritative Persistence<br/>ON CONFLICT DO NOTHING]
        G --> H[(Redis 7)<br/>Atomic Lua Latest State<br/>Rolling ZSET Window 60m]
    end

    H --> I[Manual Offset Commit<br/>consumer.commit asynchronous=False]
```

---

## Delivery Semantics

The consumer implements **at-least-once** delivery from Kafka coupled with **effectively exactly-once** semantics in PostgreSQL and Redis.
1. Messages are pulled in micro-batches without automatic offset commits (`enable.auto.commit=False`).
2. Telemetry records are inserted into PostgreSQL using `INSERT INTO readings ... ON CONFLICT (device_id, ts) DO NOTHING`.
3. Redis sorted-set window members explicitly include the event timestamp, ensuring idempotent `ZADD` writes.
4. Kafka offsets are committed synchronously **only after** PostgreSQL and Redis writes durably succeed.
5. In the event of a worker crash or rebalance, uncommitted records are safely replayed and resolved as no-op conflicts in PostgreSQL.

---

## Redis Key Design

| Key Pattern | Data Structure | Score / Fields | TTL | Description |
| :--- | :--- | :--- | :--- | :--- |
| `device:{id}:latest` | **HASH** | All telemetry fields + `timestamp_epoch`, `updated_at` | 24 hours | Latest known device status. Updated atomically via Lua script only when incoming event timestamp is strictly newer than stored epoch. |
| `device:{id}:window` | **ZSET** | Score: `epoch seconds`<br/>Member: Compact JSON with `ts` | 24 hours | 60-minute rolling feature extraction window. Automatically trimmed via `ZREMRANGEBYSCORE key -inf (latest - 3600)` on each micro-batch. |
| `dedupe:{id}:{ts}` | **STRING** | Value: `"1"` | 2 hours | Fast-path in-memory filter (`SET key 1 NX EX 7200`) to skip redundant Redis window operations on duplicate deliveries. |
| `consumer:heartbeat` | **STRING** | JSON `{consumer_id, timestamp}` | 15 seconds | Health probe heartbeat written periodically to verify consumer liveness. |

---

## Idempotency Strategy

- **Natural Composite Key**: Telemetry has an immutable identity defined by `(device_id, timestamp)`. The simulator-generated `event_id` is an informational UUID and is not used for deduplication.
- **Authoritative Source of Truth**: PostgreSQL enforces a hard `UNIQUE (device_id, ts)` database constraint (`uq_readings_device_ts`).
- **Conflict Resolution**: Writes execute via PostgreSQL dialect `INSERT ... ON CONFLICT (device_id, ts) DO NOTHING`. The database engine returns only the rows genuinely inserted, cleanly reporting `inserted` vs `conflicted` counts.
- **Redis Deduplication Optimization**: Redis `SET key NX` acts purely as a high-speed transient filter to prevent duplicate events from triggering expensive sorted-set writes and lock contention.

---

## Failure Modes & Resilience Matrix

| Component Failure | Pipeline Behavior | Data Loss Risk | Recovery Mechanism |
| :--- | :--- | :--- | :--- |
| **Kafka Broker Offline** | Consumer poll times out and enters exponential backoff retries. Batch accumulator halts. | **Zero** | Resumes consumption from last committed offset once broker re-establishes connectivity. |
| **PostgreSQL Down** | Batch insert retries with exponential backoff and jitter (`tenacity`). If attempts exhaust, **pipeline halts and does NOT commit Kafka offsets**. | **Zero** | Worker pauses without losing data. When PostgreSQL returns, the micro-batch is re-executed and offsets are committed. |
| **Redis Down** | Logs warning and completes PostgreSQL transaction and offset commit. | **Zero** (Cache only) | System remains operational. Redis state can be backfilled using the `rebuild_redis_from_postgres` utility. |
| **Consumer Crash (`kill -9`)** | Container terminates immediately. Uncommitted in-flight batch is dropped from memory. | **Zero** | New consumer instance starts at last committed offset. Replayed events are safely resolved by PostgreSQL `ON CONFLICT DO NOTHING`. |
| **Corrupted Payload** | Rejected by `EventValidator`. Packaged into structured `DeadLetterEnvelope` and dispatched to `sensor.dlq`. | **Zero** | DLQ producer flushes before source offset is acknowledged. Pipeline never crashes on malformed data. |

---

## Architectural Design Decisions (Interview Reference)

1. **Shared Domain Library (`libs/coldchain_common`)**:
   - *Rationale*: Eliminates copy-paste drift across simulator, consumer, and API services. A single canonical source of truth for Pydantic contracts and SQLAlchemy entities provides compile-time type safety across microservices.
2. **Synchronous SQLAlchemy with Psycopg v3**:
   - *Rationale*: The consumer is a dedicated, sequential batch processor rather than an asynchronous web server handling thousands of concurrent requests. Sync batching avoids async event-loop overhead and provides straightforward transaction boundaries.
3. **Atomic Lua Script for Latest State**:
   - *Rationale*: Real-world cellular IoT telemetry arrives out of order due to network handoffs. An atomic Lua script compares `new_timestamp_epoch > existing_timestamp_epoch` in Redis, preventing older delayed packets from overwriting fresher state without distributed lock overhead.
4. **Member-Encoded Timestamps in Sorted Sets**:
   - *Rationale*: Including the event timestamp in the serialized JSON member guarantees that identical duplicate events map to the same ZSET member, ensuring idempotent `ZADD` operations.
5. **Strict Offset Commit Ordering**:
   - *Rationale*: Kafka offsets are committed **only after** PostgreSQL and Redis writes succeed. In the event of any database error, offsets are left uncommitted, upholding at-least-once durability.
6. **Synchronous DLQ Flush**:
   - *Rationale*: When malformed JSON or sentinel values arrive, the DLQ producer publishes the error envelope and calls `flush()` before the batch offset is advanced, preventing corrupted messages from being lost.
7. **Plain Table with B-Tree and BRIN Indexes**:
   - *Rationale*: A plain table with composite indexes on `(shipment_id, ts DESC)`, `(device_id, ts DESC)`, and a block-range index (`BRIN`) on `ts` provides sub-millisecond lookups for operational queries and cheap range scans for historical ingestion without the operational complexity of table partitioning at this initial scale.

---

## Honest Limitations

1. **Single Consumer Group Worker**:
   - Currently configured for a single consumer process. To scale horizontally across all 6 Kafka partitions, multiple container instances should be spawned under consumer group `coldchain-ingest`.
2. **No Distributed 2-Phase Commit (XA)**:
   - There is no global transaction manager spanning Kafka, PostgreSQL, and Redis. However, idempotent database writes make at-least-once replay functionally identical to exactly-once processing.
3. **Plain Table vs Time Partitioning**:
   - The `readings` table is currently an unpartitioned table with BRIN indexing. At scale (>50M rows), monthly declarative range partitioning (`PARTITION BY RANGE (ts)`) with `pg_partman` should be adopted to allow instant partition drops for data retention.
4. **Redis Dedupe TTL Window**:
   - The Redis fast-path dedupe key has a 2-hour TTL. Duplicates arriving after 2 hours will bypass Redis dedupe but will still be caught authoritatively by PostgreSQL's unique constraint.

---

## CLI & Service Management Commands

```bash
# Foundational stack startup (PostgreSQL, Redis, Redpanda)
make up

# Run database migrations
make migrate

# Launch consumer container
make consumer-up

# Follow consumer logs
make consumer-logs

# Run consumer test suite
make test-consumer

# Execute Phase 2 verification script
make verify-phase2
```
