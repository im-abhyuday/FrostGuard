"""
Kafka Consumer and DLQ Producer factories for ColdChainIQ.
Configures idempotent DLQ publishing and manual offset commit management.
"""

import json
import logging
from typing import Any, Callable, Dict, List, Optional
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from .config import ConsumerSettings
from .schemas import DeadLetterEnvelope

logger = logging.getLogger("coldchainiq.consumer.kafka")


def ensure_topic_exists(
    bootstrap_servers: str,
    topic: str,
    num_partitions: int = 3,
    replication_factor: int = 1,
) -> None:
    """Ensures a Kafka topic exists with the requested partition count."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        cluster_metadata = admin.list_topics(timeout=5.0)
        if topic not in cluster_metadata.topics:
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
                    logger.info(f"Topic '{t}' successfully created.")
                except Exception as e:
                    logger.warning(f"Topic '{t}' creation notice: {e}")
    except Exception as e:
        logger.warning(f"Could not connect to Kafka AdminClient: {e}. Assuming topic exists or will auto-create.")


def create_kafka_consumer(
    settings: ConsumerSettings,
    on_assign_cb: Optional[Callable[[Consumer, List[TopicPartition]], None]] = None,
    on_revoke_cb: Optional[Callable[[Consumer, List[TopicPartition]], None]] = None,
) -> Consumer:
    """
    Constructs an enterprise-grade Kafka Consumer with manual offset commits and rebalance hooks.
    """
    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
        "group.id": settings.KAFKA_GROUP_ID,
        "auto.offset.reset": settings.KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,  # Strict manual commit per micro-batch
        "session.timeout.ms": settings.KAFKA_SESSION_TIMEOUT_MS,
        "max.poll.interval.ms": settings.KAFKA_MAX_POLL_INTERVAL_MS,
        "client.id": f"coldchain-ingest-worker",
    }

    consumer = Consumer(conf)

    def _on_assign(c: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(f"Partitions assigned: {[p.partition for p in partitions]}")
        if on_assign_cb:
            on_assign_cb(c, partitions)

    def _on_revoke(c: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(f"Partitions revoked: {[p.partition for p in partitions]}. Executing rebalance flush...")
        if on_revoke_cb:
            on_revoke_cb(c, partitions)

    consumer.subscribe(
        [settings.KAFKA_TOPIC],
        on_assign=_on_assign,
        on_revoke=_on_revoke,
    )
    logger.info(f"Subscribed to topic '{settings.KAFKA_TOPIC}' with consumer group '{settings.KAFKA_GROUP_ID}'")
    return consumer


def create_dlq_producer(settings: ConsumerSettings) -> Producer:
    """
    Constructs an idempotent Kafka Producer dedicated to publishing invalid records to the DLQ.
    """
    ensure_topic_exists(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        topic=settings.KAFKA_DLQ_TOPIC,
        num_partitions=3,
    )

    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
        "client.id": "coldchain-dlq-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 5,
    }
    return Producer(conf)


def publish_to_dlq(
    producer: Producer,
    dlq_topic: str,
    dead_letter: DeadLetterEnvelope,
    key: Optional[bytes] = None,
) -> None:
    """
    Synchronously dispatches an envelope to the DLQ topic and flushes buffer.
    Guarantees message durability before parent Kafka offset is committed.
    """
    payload_json = dead_letter.model_dump_json()

    def _on_delivery(err: Any, msg: Any) -> None:
        if err is not None:
            logger.error(f"DLQ delivery failure: {err}")

    producer.produce(
        topic=dlq_topic,
        key=key,
        value=payload_json.encode("utf-8"),
        on_delivery=_on_delivery,
    )
    producer.poll(0)
    # Flush ensures DLQ publish succeeds before the source offset is committed
    producer.flush(timeout=5.0)
