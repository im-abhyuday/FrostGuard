"""
Pydantic v2 schemas for authentication requests and responses.
"""

from typing import Optional
from pydantic import BaseModel, ConfigDict, EmailStr, field_validator
from ..core.security import validate_password_policy


class RegisterRequest(BaseModel):
    """Self-registration request payload for new drivers."""
    model_config = ConfigDict(str_strip_whitespace=True)

    email: EmailStr
    password: str
    full_name: str
    role: Optional[str] = "driver"

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.lower().strip()

    @field_validator("role")
    @classmethod
    def validate_role_attempt(cls, v: Optional[str]) -> str:
        if v and v.lower() in ("manager", "auditor"):
            raise ValueError("Privilege escalation rejected: Self-registration is strictly restricted to role 'driver'.")
        return "driver"

    @field_validator("password")
    @classmethod
    def validate_password_strength(cls, v: str, info) -> str:
        email = info.data.get("email") if hasattr(info, "data") else None
        validate_password_policy(v, email)
        return v


class TokenResponse(BaseModel):
    """OAuth2 compatible token response."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class RefreshRequest(BaseModel):
    """Payload for rotating an opaque refresh token."""
    refresh_token: str


class LogoutRequest(BaseModel):
    """Optional payload specifying refresh token to revoke on logout."""
    refresh_token: Optional[str] = None


class RegisterResponse(BaseModel):
    """Generic registration response to mitigate account enumeration."""
    message: str = "If this email can be registered, the account has been created. You can now sign in."

