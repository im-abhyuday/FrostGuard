"""
Environment-driven configuration for ColdChainIQ Ingestion Consumer.
Uses pydantic-settings for type-safe validation and sensible production defaults.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka Broker Configuration
    KAFKA_BOOTSTRAP: str = "localhost:9092"
    KAFKA_TOPIC: str = "sensor.readings"
    KAFKA_DLQ_TOPIC: str = "sensor.dlq"
    KAFKA_GROUP_ID: str = "coldchain-ingest"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_SESSION_TIMEOUT_MS: int = 30000
    KAFKA_MAX_POLL_INTERVAL_MS: int = 300000

    # Micro-batching Configuration
    BATCH_SIZE: int = 500
    BATCH_MAX_WAIT_MS: int = 1000

    # Redis Cache & Window Configuration
    REDIS_URL: str = "redis://localhost:6379/0"
    WINDOW_MINUTES: int = 60
    LATEST_TTL_SECONDS: int = 86400    # 24 hours
    DEDUPE_TTL_SECONDS: int = 7200     # 2 hours

    # PostgreSQL Configuration
    POSTGRES_DSN: str = "postgresql+psycopg://postgres:postgres@localhost:5432/coldchainiq"
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10

    # Reliability & Retries
    MAX_RETRIES: int = 5
    RETRY_MIN_WAIT_SECONDS: float = 0.5
    RETRY_MAX_WAIT_SECONDS: float = 8.0

    # Observability & Health
    HEALTH_PORT: int = 8081
    LOG_LEVEL: str = "INFO"
    METRICS_LOG_INTERVAL_SEC: float = 10.0
