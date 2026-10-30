"""
Unit tests for MicroBatcher threshold and timeout mechanics.
"""

import time
import pytest
from consumer.batching import MicroBatcher


def test_batcher_flushes_on_size():
    batcher = MicroBatcher(max_batch_size=3, max_wait_ms=5000)

    assert batcher.is_empty is True
    assert batcher.should_flush() is False

    # Add item 1
    ready = batcher.add("event_1")
    assert ready is False
    assert batcher.should_flush() is False

    # Add item 2
    ready = batcher.add("event_2")
    assert ready is False
    assert batcher.should_flush() is False

    # Add item 3 (reaches size 3)
    ready = batcher.add("event_3")
    assert ready is True
    assert batcher.should_flush() is True

    # Flush
    items = batcher.flush()
    assert items == ["event_1", "event_2", "event_3"]
    assert batcher.is_empty is True
    assert batcher.should_flush() is False


def test_batcher_flushes_on_timeout():
    # 50ms wait timeout
    batcher = MicroBatcher(max_batch_size=100, max_wait_ms=50)

    batcher.add("event_slow")
    assert batcher.should_flush() is False

    # Sleep past max_wait_ms
    time.sleep(0.06)
    assert batcher.should_flush() is True

    items = batcher.flush()
    assert items == ["event_slow"]
    assert batcher.is_empty is True


def test_empty_batcher_does_not_flush():
    batcher = MicroBatcher(max_batch_size=10, max_wait_ms=10)
    time.sleep(0.02)
    assert batcher.should_flush() is False
    assert batcher.flush() == []
