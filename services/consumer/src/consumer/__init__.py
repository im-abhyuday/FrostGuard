"""
ColdChainIQ Streaming Telemetry Ingestion Consumer Service.
"""

from .config import ConsumerSettings
from .pipeline import IngestionPipeline

__all__ = ["ConsumerSettings", "IngestionPipeline"]
