"""
In-process metrics tracking and structured periodic log emitter.
Provides drop-in compatibility for Prometheus client instrumentation in Phase 8.
"""

from collections import defaultdict
from dataclasses import dataclass, field
import json
import logging
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger("coldchainiq.consumer.metrics")


class ConsumerMetrics:
    """
    Thread-safe metrics aggregator tracking ingestion throughput, errors, latency, and lag.
    """

    def __init__(self, log_interval_sec: float = 10.0):
        self.log_interval_sec = log_interval_sec
        self._lock = threading.Lock()

        self.consumed: int = 0
        self.valid: int = 0
        self.invalid: Dict[str, int] = defaultdict(int)
        self.duplicates_skipped: int = 0
        self.inserted: int = 0
        self.db_conflicts: int = 0
        self.late_dropped_from_window: int = 0
        self.batches: int = 0
        self.total_batch_latency_ms: float = 0.0
        self.consumer_lag: Dict[int, int] = {}

        self.start_time: float = time.time()
        self.last_log_time: float = self.start_time
        self.last_consumed_snapshot: int = 0

    def record_consumed(self, count: int = 1) -> None:
        with self._lock:
            self.consumed += count

    def record_valid(self, count: int = 1) -> None:
        with self._lock:
            self.valid += count

    def record_invalid(self, error_type: str, count: int = 1) -> None:
        with self._lock:
            self.invalid[error_type] += count

    def record_duplicates_skipped(self, count: int = 1) -> None:
        with self._lock:
            self.duplicates_skipped += count

    def record_inserted(self, count: int = 1) -> None:
        with self._lock:
            self.inserted += count

    def record_db_conflicts(self, count: int = 1) -> None:
        with self._lock:
            self.db_conflicts += count

    def record_late_dropped(self, count: int = 1) -> None:
        with self._lock:
            self.late_dropped_from_window += count

    def record_batch(self, batch_size: int, latency_ms: float) -> None:
        with self._lock:
            self.batches += 1
            self.total_batch_latency_ms += latency_ms

    def update_lag(self, partition: int, lag: int) -> None:
        with self._lock:
            self.consumer_lag[partition] = max(0, lag)

    def get_summary(self) -> Dict[str, Any]:
        """Returns snapshot of current metrics."""
        with self._lock:
            elapsed = max(0.001, time.time() - self.start_time)
            avg_latency = (
                self.total_batch_latency_ms / self.batches if self.batches > 0 else 0.0
            )
            total_lag = sum(self.consumer_lag.values())

            return {
                "consumed": self.consumed,
                "valid": self.valid,
                "invalid": dict(self.invalid),
                "invalid_total": sum(self.invalid.values()),
                "duplicates_skipped": self.duplicates_skipped,
                "inserted": self.inserted,
                "db_conflicts": self.db_conflicts,
                "late_dropped_from_window": self.late_dropped_from_window,
                "batches": self.batches,
                "avg_batch_latency_ms": round(avg_latency, 2),
                "events_per_sec": round(self.consumed / elapsed, 1),
                "total_consumer_lag": total_lag,
                "partition_lag": dict(self.consumer_lag),
            }

    def maybe_log_periodic(self, force: bool = False) -> Optional[Dict[str, Any]]:
        """
        Emits structured JSON metrics line every log_interval_sec.
        """
        now = time.time()
        with self._lock:
            if not force and (now - self.last_log_time) < self.log_interval_sec:
                return None

            period_elapsed = max(0.001, now - self.last_log_time)
            period_consumed = self.consumed - self.last_consumed_snapshot
            live_rate = round(period_consumed / period_elapsed, 1)

            avg_latency = (
                self.total_batch_latency_ms / self.batches if self.batches > 0 else 0.0
            )
            total_lag = sum(self.consumer_lag.values())

            log_payload = {
                "metric_event": "consumer_throughput_heartbeat",
                "consumed_total": self.consumed,
                "events_per_sec_live": live_rate,
                "valid_total": self.valid,
                "invalid_total": sum(self.invalid.values()),
                "invalid_by_type": dict(self.invalid),
                "duplicates_skipped": self.duplicates_skipped,
                "db_inserted_total": self.inserted,
                "db_conflicts_total": self.db_conflicts,
                "late_dropped_from_window": self.late_dropped_from_window,
                "batches_processed": self.batches,
                "avg_batch_latency_ms": round(avg_latency, 2),
                "total_consumer_lag": total_lag,
            }

            logger.info(f"[METRICS] {json.dumps(log_payload)}")

            self.last_log_time = now
            self.last_consumed_snapshot = self.consumed
            return log_payload
