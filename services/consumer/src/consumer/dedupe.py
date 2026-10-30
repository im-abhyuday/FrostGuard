"""
Idempotency and deduplication engine.
Provides Redis SET NX fast-path optimization while designating PostgreSQL as single source of truth.
"""

import logging
from typing import List, Optional, Tuple
import redis

from .schemas import SensorReading

logger = logging.getLogger("coldchainiq.consumer.dedupe")


class DedupeFilter:
    """
    In-memory fast-path deduplication filter using Redis SET key NX EX.

    Architecture Note:
    PostgreSQL's unique constraint on (device_id, ts) is the authoritative source of truth
    guaranteeing strict exactly-once database semantics.
    Redis dedupe is a high-speed optimization: duplicate events identified here skip
    redundant Redis sorted-set window writes and reduce write amplification.
    """

    def __init__(self, redis_client: Optional[redis.Redis] = None, ttl_seconds: int = 7200):
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds

    def is_duplicate(self, reading: SensorReading) -> bool:
        """
        Checks whether (device_id, timestamp) has already been processed.
        Returns True if duplicate (key already existed), False if first sight.
        """
        if self.redis is None:
            return False

        key = f"dedupe:{reading.device_id}:{reading.timestamp}"
        try:
            # SET key 1 NX EX ttl
            # Returns True if key was set (new record), None/False if key already existed
            was_set = self.redis.set(key, "1", nx=True, ex=self.ttl_seconds)
            return not bool(was_set)
        except Exception as e:
            logger.warning(f"Redis dedupe check failed: {e}. Falling back to Postgres authoritative dedupe.")
            return False

    def filter_batch(
        self, readings: List[SensorReading]
    ) -> Tuple[List[SensorReading], List[SensorReading]]:
        """
        Pipelined deduplication check for an entire micro-batch.
        Returns (unique_readings, duplicate_readings).
        """
        if not readings:
            return [], []

        # In-batch deduplication first (in case the micro-batch itself contains identical events)
        seen_in_batch = set()
        deduped_in_memory: List[SensorReading] = []
        batch_duplicates: List[SensorReading] = []

        for r in readings:
            natural_key = (r.device_id, r.timestamp)
            if natural_key in seen_in_batch:
                batch_duplicates.append(r)
            else:
                seen_in_batch.add(natural_key)
                deduped_in_memory.append(r)

        if self.redis is None:
            return deduped_in_memory, batch_duplicates

        try:
            pipe = self.redis.pipeline()
            for r in deduped_in_memory:
                key = f"dedupe:{r.device_id}:{r.timestamp}"
                pipe.set(key, "1", nx=True, ex=self.ttl_seconds)

            results = pipe.execute()

            unique_readings: List[SensorReading] = []
            redis_duplicates: List[SensorReading] = []

            for r, was_set in zip(deduped_in_memory, results):
                if was_set:
                    unique_readings.append(r)
                else:
                    redis_duplicates.append(r)

            all_duplicates = batch_duplicates + redis_duplicates
            return unique_readings, all_duplicates

        except Exception as e:
            logger.warning(f"Redis pipeline dedupe failed: {e}. Falling back to Postgres.")
            return deduped_in_memory, batch_duplicates
