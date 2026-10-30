"""
Database models for API service.
Re-exported directly from canonical coldchain_common library.
"""

from sqlalchemy import Column, Enum as SAEnum, Integer
from coldchain_common.db_models import (
    Alert,
    AuditLog,
    Base,
    Device,
    Reading,
    RefreshToken,
    Shipment,
    ShipmentAssignment,
    User,
)
from coldchain_common.enums import UserRole

# Augment User model with native user_role_enum and token_version
if "token_version" not in User.__table__.columns:
    token_ver_col = Column(
        "token_version",
        Integer,
        server_default="1",
        default=1,
        nullable=False,
    )
    User.__table__.append_column(token_ver_col)
    setattr(User, "token_version", token_ver_col)

User.__table__.columns["role"].type = SAEnum(
    UserRole,
    name="user_role_enum",
    create_type=False,
    values_callable=lambda x: [e.value for e in x],
)

__all__ = [
    "Alert",
    "AuditLog",
    "Base",
    "Device",
    "Reading",
    "RefreshToken",
    "Shipment",
    "ShipmentAssignment",
    "User",
]

