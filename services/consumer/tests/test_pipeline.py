"""
Unit tests for full IngestionPipeline message handling and DLQ routing.
"""

import json
from unittest.mock import MagicMock
import pytest

from consumer.config import ConsumerSettings
from consumer.pipeline import IngestionPipeline
from consumer.schemas import DeadLetterErrorType


def test_pipeline_dispatches_bad_messages_to_dlq(valid_reading_payload):
    mock_consumer = MagicMock()
    mock_dlq_producer = MagicMock()
    mock_repo = MagicMock()
    mock_redis = MagicMock()

    mock_repo.insert_batch.return_value = (1, 0)
    mock_redis.write_batch.return_value = (1, 0)

    settings = ConsumerSettings(BATCH_SIZE=10, BATCH_MAX_WAIT_MS=500)
    pipeline = IngestionPipeline(
        settings=settings,
        consumer=mock_consumer,
        dlq_producer=mock_dlq_producer,
        repository=mock_repo,
        redis_store=mock_redis,
    )

    # 1. Corrupt message
    bad_msg = MagicMock()
    bad_msg.error.return_value = None
    bad_msg.value.return_value = b"{corrupt json syntax"
    bad_msg.key.return_value = b"DEV-0001"
    bad_msg.topic.return_value = "sensor.readings"
    bad_msg.partition.return_value = 0
    bad_msg.offset.return_value = 10

    pipeline._handle_message(bad_msg)

    # Verify DLQ producer called and metrics updated
    mock_dlq_producer.produce.assert_called_once()
    assert pipeline.metrics.invalid[DeadLetterErrorType.CORRUPT_JSON.value] == 1
    assert pipeline.batcher.size == 0  # Bad message did not enter valid batcher

    # 2. Valid message
    valid_msg = MagicMock()
    valid_msg.error.return_value = None
    valid_msg.value.return_value = json.dumps(valid_reading_payload).encode("utf-8")
    valid_msg.key.return_value = b"DEV-0001"
    valid_msg.topic.return_value = "sensor.readings"
    valid_msg.partition.return_value = 0
    valid_msg.offset.return_value = 11

    pipeline._handle_message(valid_msg)

    assert pipeline.metrics.valid == 1
    assert pipeline.batcher.size == 1

    # Flush batcher
    pipeline._flush_batch()
    mock_repo.insert_batch.assert_called_once()
    mock_consumer.commit.assert_called_once_with(asynchronous=False)
