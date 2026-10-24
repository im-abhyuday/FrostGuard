"""
Output sinks for ColdChainIQ telemetry: CsvSink, ParquetSink, and KafkaSink.
Provides a unified interface for streaming and batch storage.
"""

import abc
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .models import SensorReading

logger = logging.getLogger("coldchainiq.simulator.sinks")


class BaseSink(abc.ABC):
    """Abstract interface for all telemetry sinks."""

    @abc.abstractmethod
    def write(self, reading: Union[SensorReading, Dict[str, Any]]) -> None:
        pass

    @abc.abstractmethod
    def write_batch(self, readings: List[Union[SensorReading, Dict[str, Any]]]) -> None:
        pass

    @abc.abstractmethod
    def flush(self) -> None:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass


class CsvSink(BaseSink):
    """Writes telemetry records directly to CSV format."""

    def __init__(self, file_path: Union[str, Path], max_rows: Optional[int] = None):
        self.file_path = Path(file_path)
        self.max_rows = max_rows
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer: List[Dict[str, Any]] = []
        self._total_written = 0

    def write(self, reading: Union[SensorReading, Dict[str, Any]]) -> None:
        if self.max_rows and self._total_written >= self.max_rows:
            return
        row = reading.model_dump() if isinstance(reading, SensorReading) else reading
        self._buffer.append(row)
        self._total_written += 1
        if len(self._buffer) >= 5000:
            self.flush()

    def write_batch(self, readings: List[Union[SensorReading, Dict[str, Any]]]) -> None:
        for r in readings:
            self.write(r)

    def flush(self) -> None:
        if not self._buffer:
            return
        df = pd.DataFrame(self._buffer)
        header = not self.file_path.exists()
        df.to_csv(self.file_path, mode="a", index=False, header=header)
        self._buffer.clear()

    def close(self) -> None:
        self.flush()


class ParquetSink(BaseSink):
    """
    Streaming Parquet writer that writes Arrow record batches in fixed chunks
    to ensure minimal memory footprint and high I/O throughput.
    """

    def __init__(self, file_path: Union[str, Path], chunk_size: int = 50000):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        self._buffer: List[Dict[str, Any]] = []
        self._writer: Optional[pq.ParquetWriter] = None
        self._schema: Optional[pa.Schema] = None

    def write(self, reading: Union[SensorReading, Dict[str, Any]]) -> None:
        row = reading.model_dump() if isinstance(reading, SensorReading) else reading
        self._buffer.append(row)
        if len(self._buffer) >= self.chunk_size:
            self.flush()

    def write_batch(self, readings: List[Union[SensorReading, Dict[str, Any]]]) -> None:
        for r in readings:
            row = r.model_dump() if isinstance(r, SensorReading) else r
            self._buffer.append(row)
        if len(self._buffer) >= self.chunk_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        table = pa.Table.from_pylist(self._buffer)
        if self._writer is None:
            self._schema = table.schema
            self._writer = pq.ParquetWriter(
                self.file_path, self._schema, compression="snappy"
            )
        self._writer.write_table(table)
        self._buffer.clear()

    def close(self) -> None:
        self.flush()
        if self._writer:
            self._writer.close()
            self._writer = None


class KafkaSink(BaseSink):
    """
    High-performance, idempotent Kafka producer publishing telemetry partitioned by device_id.
    Ensures in-order delivery per hardware device.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "sensor.readings",
        num_partitions: int = 6,
        replication_factor: int = 1,
        producer_instance: Any = None,  # For unit test mocking
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

        if producer_instance is not None:
            self.producer = producer_instance
        else:
            from confluent_kafka import Producer
            from confluent_kafka.admin import AdminClient, NewTopic

            # 1. Ensure Kafka topic exists with 6 partitions
            try:
                admin = AdminClient({"bootstrap.servers": bootstrap_servers})
                existing_topics = admin.list_topics(timeout=5.0).topics
                if topic not in existing_topics:
                    logger.info(f"Creating topic '{topic}' with {num_partitions} partitions...")
                    new_topic = NewTopic(
                        topic=topic,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                    )
                    futures = admin.create_topics([new_topic])
                    for t, f in futures.items():
                        try:
                            f.result()
                            logger.info(f"Topic '{t}' created successfully.")
                        except Exception as e:
                            logger.warning(f"Topic creation warning: {e}")
            except Exception as e:
                logger.warning(f"Could not connect to Kafka AdminClient: {e}. Will attempt direct publish.")

            # 2. Configure enterprise-grade producer
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "coldchainiq-simulator-producer",
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "linger.ms": 10,
                "batch.num.messages": 1000,
                "compression.type": "snappy",
            }
            self.producer = Producer(conf)

        self._messages_sent = 0

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error(f"Kafka delivery failed: {err}")

    def write(self, reading: Union[SensorReading, Dict[str, Any]]) -> None:
        if isinstance(reading, SensorReading):
            device_id = reading.device_id
            payload_str = reading.model_dump_json()
        elif isinstance(reading, dict):
            device_id = str(reading.get("device_id", "UNKNOWN"))
            payload_str = json.dumps(reading)
        else:
            device_id = "UNKNOWN"
            payload_str = json.dumps(reading)

        # Message key = device_id guarantees strict partition affinity and ordered processing
        self.producer.produce(
            topic=self.topic,
            key=device_id.encode("utf-8"),
            value=payload_str.encode("utf-8"),
            on_delivery=self._delivery_callback,
        )
        self.producer.poll(0)
        self._messages_sent += 1

    def write_batch(self, readings: List[Union[SensorReading, Dict[str, Any]]]) -> None:
        for r in readings:
            self.write(r)

    def flush(self) -> None:
        self.producer.flush(timeout=10.0)

    def close(self) -> None:
        self.flush()

    @property
    def messages_sent(self) -> int:
        return self._messages_sent
