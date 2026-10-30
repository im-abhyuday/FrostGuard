"""
Pydantic v2 schemas for user management, profile updates, and admin assignments.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, ConfigDict, EmailStr, field_validator
from ..core.security import validate_password_policy


class UserRead(BaseModel):
    """User profile response representation."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    email: str
    full_name: str
    role: str
    is_active: bool
    token_version: int = 1
    created_at: datetime
    last_login_at: Optional[datetime] = None


class UserUpdate(BaseModel):
    """Profile update payload restricted to non-security attributes."""
    model_config = ConfigDict(str_strip_whitespace=True)
    full_name: str

    @field_validator("full_name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or len(v.strip()) < 2:
            raise ValueError("Full name must contain at least 2 characters.")
        return v.strip()


class PasswordChangeRequest(BaseModel):
    """Password modification payload requiring current password verification."""
    current_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        validate_password_policy(v)
        return v


class UserRoleUpdate(BaseModel):
    """Admin payload for modifying role assignments."""
    role: str

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        valid_roles = {"driver", "manager", "auditor"}
        if v.lower() not in valid_roles:
            raise ValueError(f"Invalid role '{v}'. Allowed roles: {sorted(valid_roles)}")
        return v.lower()


class UserActiveUpdate(BaseModel):
    """Admin payload for enabling or disabling user accounts."""
    is_active: bool


class AssignmentCreate(BaseModel):
    """Admin payload linking a driver to a shipment."""
    user_id: uuid.UUID
    shipment_id: str


class AssignmentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: uuid.UUID
    shipment_id: str
    assigned_at: datetime


class ShipmentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    cargo_type: str
    origin: Optional[str] = None
    destination: Optional[str] = None
    route_name: Optional[str] = None
    setpoint_c: float
    safe_min_c: float
    safe_max_c: float
    status: str
    created_at: datetime


class ReadingRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    device_id: str
    shipment_id: str
    ts: datetime
    temperature_c: float
    humidity_pct: float
    door_open: bool
    compressor_on: bool
    compressor_status: str
    ambient_temp_c: float
    speed_kmph: float
    setpoint_c: float


class AlertAckResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    shipment_id: Optional[str]
    device_id: Optional[str]
    severity: str
    status: str
    acknowledged_by: Optional[str]
    acknowledged_at: Optional[datetime]
