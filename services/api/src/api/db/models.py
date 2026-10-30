"""
Database models for API service.
Re-exported directly from canonical coldchain_common library.
"""

from coldchain_common.db_models import (
    Alert,
    Base,
    Device,
    Reading,
    Shipment,
)

__all__ = [
    "Alert",
    "Base",
    "Device",
    "Reading",
    "Shipment",
]
