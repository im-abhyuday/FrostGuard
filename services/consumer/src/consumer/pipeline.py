"""
Main streaming ingestion pipeline for ColdChainIQ.
Orchestrates: Poll -> Validate -> DLQ -> Dedupe -> Postgres -> Redis -> Manual Offset Commit.
Enforces at-least-once delivery with effectively exactly-once database semantics.
"""

from datetime import datetime, timezone
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition

from .batching import MicroBatcher
from .config import ConsumerSettings
from .dedupe import DedupeFilter
from .kafka_client import (
    create_dlq_producer,
    create_kafka_consumer,
    publish_to_dlq,
)
from .metrics import ConsumerMetrics
from .redis_store import RedisStore
from .repository import PostgresRepository
from .schemas import DeadLetterEnvelope, SensorReading
from .validation import EventValidator

logger = logging.getLogger("coldchainiq.consumer.pipeline")


class IngestionPipeline:
    """
    Continuous streaming event consumer and micro-batch writer.
    """

    def __init__(
        self,
        settings: ConsumerSettings,
        consumer: Optional[Consumer] = None,
        dlq_producer: Optional[Producer] = None,
        repository: Optional[PostgresRepository] = None,
        redis_store: Optional[RedisStore] = None,
        validator: Optional[EventValidator] = None,
        dedupe_filter: Optional[DedupeFilter] = None,
        metrics: Optional[ConsumerMetrics] = None,
    ):
        self.settings = settings
        self.running: bool = False

        self.metrics = metrics or ConsumerMetrics(log_interval_sec=settings.METRICS_LOG_INTERVAL_SEC)
        self.validator = validator or EventValidator()
        self.repository = repository or PostgresRepository(settings)
        self.redis_store = redis_store or RedisStore(settings)
        self.dedupe_filter = dedupe_filter or DedupeFilter(
            redis_client=self.redis_store.client if self.redis_store else None,
            ttl_seconds=settings.DEDUPE_TTL_SECONDS,
        )

        self.dlq_producer = dlq_producer or create_dlq_producer(settings)

        # Batch accumulator for (Kafka Message, SensorReading)
        self.batcher: MicroBatcher[Tuple[Message, SensorReading]] = MicroBatcher(
            max_batch_size=settings.BATCH_SIZE,
            max_wait_ms=settings.BATCH_MAX_WAIT_MS,
        )

        # Consumer instance with rebalance callbacks
        if consumer is not None:
            self.consumer = consumer
        else:
            self.consumer = create_kafka_consumer(
                settings=settings,
                on_assign_cb=self._on_assign,
                on_revoke_cb=self._on_revoke,
            )

        self.assigned_partitions: List[TopicPartition] = []

    def _on_assign(self, c: Consumer, partitions: List[TopicPartition]) -> None:
        self.assigned_partitions = partitions

    def _on_revoke(self, c: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info("Partitions being revoked. Flushing pending micro-batch prior to rebalance...")
        self._flush_batch()
        self.assigned_partitions = []

    def run(self) -> None:
        """
        Executes primary ingestion loop until stop() is requested.
        """
        self.running = True
        logger.info("ColdChainIQ streaming ingestion pipeline started successfully.")

        try:
            while self.running:
                # 1. Poll next Kafka message
                msg = self.consumer.poll(timeout=0.1)

                if msg is not None:
                    err = msg.error()
                    if err is not None:
                        if err.code() == KafkaError._PARTITION_EOF:
                            pass
                        else:
                            logger.error(f"Kafka consumer error: {err}")
                    else:
                        self._handle_message(msg)

                # 2. Check if micro-batch threshold or timeout reached
                if self.batcher.should_flush():
                    self._flush_batch()

                # 3. Periodic metrics logging and heartbeat
                self._heartbeat_and_metrics()

        except Exception as e:
            logger.error(f"Fatal exception in ingestion loop: {e}", exc_info=True)
            raise
        finally:
            self._shutdown()

    def _handle_message(self, msg: Message) -> None:
        """Processes an individual polled Kafka message."""
        self.metrics.record_consumed(1)

        partition = msg.partition()
        offset = msg.offset()
        raw_payload = msg.value()
        raw_key = msg.key()

        # Validate message
        reading, dead_letter = self.validator.validate(
            raw_payload=raw_payload,
            partition=partition,
            offset=offset,
            source_topic=msg.topic() or self.settings.KAFKA_TOPIC,
        )

        if dead_letter is not None:
            # Publish invalid event to DLQ synchronously before offset commit
            self.metrics.record_invalid(dead_letter.error_type, 1)
            publish_to_dlq(
                producer=self.dlq_producer,
                dlq_topic=self.settings.KAFKA_DLQ_TOPIC,
                dead_letter=dead_letter,
                key=raw_key,
            )
            # Commit offset immediately for single DLQ event if batcher empty,
            # or allow it to be committed with current batch offset
            return

        # Valid reading: add to micro-batch
        self.metrics.record_valid(1)
        self.batcher.add((msg, reading))

    def _flush_batch(self) -> None:
        """
        Executes atomic micro-batch write pipeline and commits Kafka offsets.
        """
        batch = self.batcher.flush()
        if not batch:
            return

        batch_start = time.time()
        readings = [r for (_, r) in batch]

        # Step 1: Redis Deduplication Fast-Path
        unique_readings, duplicates = self.dedupe_filter.filter_batch(readings)
        if duplicates:
            self.metrics.record_duplicates_skipped(len(duplicates))

        # Step 2: PostgreSQL Batch Insert (Authoritative Idempotency)
        # If Postgres fails, retries with backoff. If exhaustion occurs, raises and halts
        # so Kafka offsets are NOT committed (at-least-once preservation).
        try:
            inserted, conflicted = self.repository.insert_batch(unique_readings)
            self.metrics.record_inserted(inserted)
            self.metrics.record_db_conflicts(conflicted)
        except Exception as e:
            logger.critical(
                f"PostgreSQL batch insert failed after retries: {e}. "
                "Halting consumption to prevent data loss (offsets NOT committed)."
            )
            raise

        # Step 3: Redis Latest State and Rolling Window Update
        # If Redis fails, log warning and continue (Redis can be rebuilt from Postgres).
        try:
            latest_upd, late_drop = self.redis_store.write_batch(unique_readings)
            self.metrics.record_late_dropped(late_drop)
        except Exception as e:
            logger.warning(
                f"Redis write failed for batch of {len(unique_readings)} items: {e}. "
                "Postgres write was durable; continuing pipeline."
            )

        # Step 4: Synchronous Kafka Offset Commit
        # Guaranteed only AFTER durable storage writes succeed
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Kafka offset commit error: {e}")

        latency_ms = (time.time() - batch_start) * 1000.0
        self.metrics.record_batch(batch_size=len(batch), latency_ms=latency_ms)

        # Update partition lag metrics
        self._update_consumer_lag(batch[-1][0])

    def _update_consumer_lag(self, last_msg: Message) -> None:
        """Calculates current consumer lag on the active partition."""
        try:
            tp = TopicPartition(last_msg.topic(), last_msg.partition())
            low, high = self.consumer.get_watermark_offsets(tp, timeout=0.5)
            lag = max(0, high - (last_msg.offset() + 1))
            self.metrics.update_lag(last_msg.partition(), lag)
        except Exception:
            pass

    def _heartbeat_and_metrics(self) -> None:
        """Updates health heartbeat and periodically emits metrics."""
        try:
            self.redis_store.write_heartbeat(consumer_id="consumer-1")
        except Exception:
            pass

        self.metrics.maybe_log_periodic()

    def stop(self) -> None:
        """Signals pipeline to finish current batch and exit gracefully."""
        logger.info("Stop signal received. Initiating graceful shutdown...")
        self.running = False

    def _shutdown(self) -> None:
        """Flushes remaining buffers and closes clients cleanly."""
        logger.info("Executing final flush and closing connections...")
        try:
            self._flush_batch()
        except Exception as e:
            logger.warning(f"Error during final batch flush: {e}")

        try:
            self.dlq_producer.flush(timeout=5.0)
        except Exception:
            pass

        try:
            self.consumer.close()
            logger.info("Kafka consumer cleanly closed.")
        except Exception as e:
            logger.warning(f"Error closing Kafka consumer: {e}")
