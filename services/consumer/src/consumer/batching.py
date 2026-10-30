"""
Micro-batching accumulator for streaming telemetry.
Triggers batch dispatch on size boundary or timeout elapsed.
"""

import time
from typing import Generic, List, Optional, TypeVar

T = TypeVar("T")


class MicroBatcher(Generic[T]):
    """
    Buffers consumed Kafka events until either:
    1. Buffer size >= max_batch_size
    2. Elapsed time since first buffered event >= max_wait_ms
    """

    def __init__(self, max_batch_size: int = 500, max_wait_ms: int = 1000):
        self.max_batch_size = max_batch_size
        self.max_wait_ms = max_wait_ms
        self._buffer: List[T] = []
        self._first_item_time: Optional[float] = None
        self._last_flush_time: float = time.time()

    def add(self, item: T) -> bool:
        """
        Appends an item to the buffer.
        Returns True if batch threshold reached and ready for immediate flush.
        """
        if not self._buffer:
            self._first_item_time = time.time()

        self._buffer.append(item)
        return len(self._buffer) >= self.max_batch_size

    def should_flush(self) -> bool:
        """Determines if the batch is ready to be processed."""
        if not self._buffer:
            return False

        if len(self._buffer) >= self.max_batch_size:
            return True

        if self._first_item_time is not None:
            elapsed_ms = (time.time() - self._first_item_time) * 1000.0
            if elapsed_ms >= self.max_wait_ms:
                return True

        return False

    def flush(self) -> List[T]:
        """Flushes and returns all accumulated items in the buffer."""
        items = self._buffer
        self._buffer = []
        self._first_item_time = None
        self._last_flush_time = time.time()
        return items

    @property
    def size(self) -> int:
        return len(self._buffer)

    @property
    def is_empty(self) -> bool:
        return len(self._buffer) == 0
