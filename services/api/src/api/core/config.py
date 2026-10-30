"""
Application configuration for ColdChainIQ API.
Driven by pydantic-settings and environment variables.
"""

from typing import Any, List, Union

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Enterprise API settings with strict validation for secrets and security parameters.
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    PROJECT_NAME: str = "ColdChainIQ API"
    VERSION: str = "1.0.0"
    API_PREFIX: str = ""
    HOST: str = "0.0.0.0"
    PORT: int = 8002

    # Database
    POSTGRES_DSN: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/coldchainiq"
    DB_ECHO: bool = False
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20

    # Redis Cache & Denylist
    REDIS_URL: str = "redis://localhost:6379/0"

    # Authentication & JWT
    JWT_SECRET: str = "coldchainiq-super-secure-production-jwt-secret-key-32bytes-min"
    JWT_ALGORITHM: str = "HS256"
    JWT_ISSUER: str = "coldchainiq-api"
    JWT_AUDIENCE: str = "coldchainiq-client"
    ACCESS_TOKEN_MINUTES: int = 15
    REFRESH_TOKEN_DAYS: int = 7

    # Registration Policy
    ALLOW_SELF_REGISTRATION: bool = True
    REQUIRE_APPROVAL: bool = False

    # Account Lockout & Security
    MAX_FAILED_LOGINS: int = 5
    ACCOUNT_LOCKOUT_MINUTES: int = 15

    # Rate Limiting
    LOGIN_RATE_LIMIT_PER_MIN: int = 5
    LOGIN_IP_RATE_LIMIT_PER_MIN: int = 20
    REGISTER_RATE_LIMIT_PER_HOUR: int = 5
    REFRESH_RATE_LIMIT_PER_MIN: int = 30

    # CORS & Network Hardening
    CORS_ORIGINS: Union[List[str], str] = [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost:8002",
        "http://127.0.0.1:8002",
    ]
    HSTS_ENABLED: bool = False
    LOG_LEVEL: str = "INFO"

    @field_validator("CORS_ORIGINS")
    @classmethod
    def assemble_cors_origins(cls, v: Any) -> List[str]:
        if isinstance(v, str):
            if v.strip().startswith("[") and v.strip().endswith("]"):
                import json

                return json.loads(v)
            return [i.strip() for i in v.split(",") if i.strip()]
        return v

    @field_validator("JWT_SECRET")
    @classmethod
    def validate_jwt_secret_length(cls, v: str) -> str:
        if not v or len(v.encode("utf-8")) < 32:
            raise ValueError(
                f"JWT_SECRET must be at least 32 bytes for cryptographic safety (got {len(v.encode('utf-8')) if v else 0} bytes)"
            )
        return v


settings = Settings()
