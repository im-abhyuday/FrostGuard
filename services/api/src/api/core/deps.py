"""
FastAPI dependency injection providers for ColdChainIQ API.
Includes database sessions, Redis client, current user extraction, RBAC role guard,
and object-level IDOR-safe shipment authorization.
"""

from typing import AsyncGenerator, Callable, List, Optional
import uuid
from fastapi import Depends, Header, Request, status
from fastapi.security import OAuth2PasswordBearer
import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .errors import (
    InvalidCredentialsError,
    PermissionDeniedError,
    ResourceNotFoundError,
    TokenRevokedError,
    UserInactiveError,
)
from .security import decode_access_token
from ..db.models import Shipment, ShipmentAssignment, User
from ..db.session import get_db_session

# OAuth2 scheme enabling Swagger UI "Authorize" modal
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

# Shared Redis connection pool
_redis_client: Optional[aioredis.Redis] = None


def get_redis_client() -> aioredis.Redis:
    """Returns singleton async Redis client."""
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_timeout=2.0,
        )
    return _redis_client


async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    """Dependency yielding async Redis client."""
    client = get_redis_client()
    yield client


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
) -> User:
    """
    Validates JWT access token, checks Redis JTI denylist and user invalidation timestamps,
    and returns active User entity.
    """
    # 1. Decode & cryptographically verify JWT claims
    payload = decode_access_token(token)
    user_id_str = payload.get("sub")
    jti = payload.get("jti")
    token_role = payload.get("role")
    token_iat = payload.get("iat", 0)

    try:
        user_uuid = uuid.UUID(user_id_str)
    except (ValueError, TypeError):
        raise InvalidCredentialsError("Invalid user identity in token.")

    # 2. Check JTI Revocation Denylist in Redis
    try:
        is_denylisted = await redis.get(f"denylist:{jti}")
        if is_denylisted:
            raise TokenRevokedError("Access token has been revoked (logged out).")

        # 3. Check User Min-IAT Invalidation (set on password change, role change, deactivation)
        min_iat_str = await redis.get(f"user:{user_id_str}:min_iat")
        if min_iat_str and token_iat <= int(min_iat_str):
            raise TokenRevokedError("Session invalidated due to security/role changes. Please log in again.")
    except (ConnectionError, aioredis.RedisError) as e:
        # If Redis is temporarily unreachable, proceed to database check
        pass

    # 4. Fetch User from authoritative database
    result = await db.execute(select(User).where(User.id == user_uuid))
    user = result.scalar_one_or_none()
    if not user:
        raise InvalidCredentialsError("User account no longer exists.")

    # 5. Check Active Status
    if not user.is_active:
        raise UserInactiveError("User account is inactive or disabled.")

    # 6. Check Role Drift (e.g. if user was demoted while holding a manager token)
    if user.role != token_role:
        raise TokenRevokedError("User role has changed. Please re-authenticate to obtain updated privileges.")

    # 7. Check Token Version (PostgreSQL-backed backstop against Redis outages)
    token_tv = payload.get("tv")
    user_tv = getattr(user, "token_version", 1)
    if token_tv is None or not isinstance(token_tv, int) or token_tv < user_tv:
        raise TokenRevokedError("Access token version is obsolete. Please re-authenticate.")

    return user


def require_role(*allowed_roles: str) -> Callable[[User], User]:
    """
    Role-Based Access Control (RBAC) dependency factory.
    Enforces that current user belongs to one of the authorized roles.
    """
    def _role_checker(user: User = Depends(get_current_user)) -> User:
        if user.role not in allowed_roles:
            raise PermissionDeniedError(
                f"Role '{user.role}' is not authorized. Required: {list(allowed_roles)}"
            )
        return user

    return _role_checker


async def get_shipment_for_user(
    shipment_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
) -> Shipment:
    """
    Object-level authorization dependency preventing Insecure Direct Object References (IDOR).
    - Managers & Auditors: Can view any valid shipment.
    - Drivers: Can only view shipments explicitly assigned to them.
    Returns 404 (NOT_FOUND) instead of 403 to prevent leaking shipment existence.
    """
    if user.role in ("manager", "auditor"):
        res = await db.execute(select(Shipment).where(Shipment.id == shipment_id))
        shipment = res.scalar_one_or_none()
        if not shipment:
            raise ResourceNotFoundError(f"Shipment '{shipment_id}' not found.")
        return shipment

    # Driver role: must check shipment_assignments link
    query = (
        select(Shipment)
        .join(ShipmentAssignment, ShipmentAssignment.shipment_id == Shipment.id)
        .where(
            Shipment.id == shipment_id,
            ShipmentAssignment.user_id == user.id,
        )
    )
    res = await db.execute(query)
    shipment = res.scalar_one_or_none()
    if not shipment:
        # Return 404 (not 403) to prevent IDOR existence leakage
        raise ResourceNotFoundError(f"Shipment '{shipment_id}' not found.")

    return shipment


def get_client_ip(request: Request, x_forwarded_for: Optional[str] = Header(None)) -> str:
    """Extracts client IP address respecting X-Forwarded-For reverse proxy header."""
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "127.0.0.1"
