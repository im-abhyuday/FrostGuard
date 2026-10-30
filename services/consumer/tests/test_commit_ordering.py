"""
Unit tests verifying strict offset commit ordering and failure isolation.
Asserts that Kafka commit is invoked ONLY AFTER storage writes succeed,
and NEVER invoked if repository write raises an exception.
"""

from unittest.mock import MagicMock
import pytest

from consumer.config import ConsumerSettings
from consumer.pipeline import IngestionPipeline


def test_commit_called_after_successful_writes(valid_sensor_reading):
    mock_consumer = MagicMock()
    mock_repo = MagicMock()
    mock_redis = MagicMock()
    mock_dlq = MagicMock()

    mock_repo.insert_batch.return_value = (1, 0)
    mock_redis.write_batch.return_value = (1, 0)

    settings = ConsumerSettings()
    pipeline = IngestionPipeline(
        settings=settings,
        consumer=mock_consumer,
        dlq_producer=mock_dlq,
        repository=mock_repo,
        redis_store=mock_redis,
    )

    mock_msg = MagicMock()
    mock_msg.topic.return_value = "sensor.readings"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 100
    mock_consumer.get_watermark_offsets.return_value = (0, 101)

    # Add item to batcher and flush
    pipeline.batcher.add((mock_msg, valid_sensor_reading))
    pipeline._flush_batch()

    # Assert repo and redis were called
    mock_repo.insert_batch.assert_called_once()
    mock_redis.write_batch.assert_called_once()

    # Assert synchronous offset commit was executed
    mock_consumer.commit.assert_called_once_with(asynchronous=False)


def test_commit_not_called_when_postgres_raises(valid_sensor_reading):
    mock_consumer = MagicMock()
    mock_repo = MagicMock()
    mock_redis = MagicMock()
    mock_dlq = MagicMock()

    # Simulate database outage
    mock_repo.insert_batch.side_effect = RuntimeError("FATAL: PostgreSQL Connection Dropped")

    settings = ConsumerSettings()
    pipeline = IngestionPipeline(
        settings=settings,
        consumer=mock_consumer,
        dlq_producer=mock_dlq,
        repository=mock_repo,
        redis_store=mock_redis,
    )

    mock_msg = MagicMock()
    pipeline.batcher.add((mock_msg, valid_sensor_reading))

    # Flushing must raise the exception and abort before commit
    with pytest.raises(RuntimeError, match="FATAL: PostgreSQL Connection Dropped"):
        pipeline._flush_batch()

    # Assert consumer.commit was NEVER called
    mock_consumer.commit.assert_not_called()
    mock_redis.write_batch.assert_not_called()
