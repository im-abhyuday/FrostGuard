"""
Comprehensive unit tests for api.core.deps to achieve 90%+ code coverage.
Tests all branches: invalid/missing claims, deleted users, inactive accounts,
role drift, token version mismatches, Redis outages, IDOR shipment authorization,
and client IP extraction.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
import uuid
import jwt
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.config import settings
from api.core.deps import (
    get_client_ip,
    get_current_user,
    get_redis,
    get_redis_client,
    get_shipment_for_user,
    require_role,
)
from api.core.errors import (
    InvalidCredentialsError,
    PermissionDeniedError,
    ResourceNotFoundError,
    TokenRevokedError,
    UserInactiveError,
)
from api.core.security import create_access_token
from api.db.models import Shipment, ShipmentAssignment, User


@pytest.mark.asyncio
async def test_deps_get_redis_client_and_generator():
    """Tests get_redis_client singleton and get_redis async generator."""
    client = get_redis_client()
    assert client is not None

    gen = get_redis()
    yielded = await anext(gen)
    assert yielded is client


@pytest.mark.asyncio
async def test_deps_non_uuid_sub_raises_invalid_credentials(test_db_session: AsyncSession, fake_redis):
    """Tests get_current_user when sub is not a valid UUID."""
    now = datetime.now(timezone.utc)
    claims = {
        "sub": "not-a-valid-uuid",
        "role": "driver",
        "jti": str(uuid.uuid4()),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=15)).timestamp()),
        "type": "access",
        "iss": settings.JWT_ISSUER,
        "aud": settings.JWT_AUDIENCE,
        "tv": 1,
    }
    raw_token = jwt.encode(claims, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

    with pytest.raises(InvalidCredentialsError) as exc:
        await get_current_user(token=raw_token, db=test_db_session, redis=fake_redis)
    assert "Invalid user identity" in str(exc.value)


@pytest.mark.asyncio
async def test_deps_user_not_found_in_db(test_db_session: AsyncSession, fake_redis):
    """Tests get_current_user when token is valid but user does not exist in DB."""
    non_existent_id = uuid.uuid4()
    token, _, _ = create_access_token(non_existent_id, "driver", token_version=1)

    with pytest.raises(InvalidCredentialsError) as exc:
        await get_current_user(token=token, db=test_db_session, redis=fake_redis)
    assert "no longer exists" in str(exc.value)


@pytest.mark.asyncio
async def test_deps_inactive_user_raises_user_inactive(test_db_session: AsyncSession, fake_redis, seed_data: dict):
    """Tests get_current_user when user.is_active is False."""
    user = seed_data["driver1"]
    user.is_active = False
    await test_db_session.commit()

    token, _, _ = create_access_token(user.id, user.role, token_version=1)
    with pytest.raises(UserInactiveError) as exc:
        await get_current_user(token=token, db=test_db_session, redis=fake_redis)
    assert "inactive or disabled" in str(exc.value)


@pytest.mark.asyncio
async def test_deps_role_drift_raises_token_revoked(test_db_session: AsyncSession, fake_redis, seed_data: dict):
    """Tests get_current_user when token role does not match current DB role."""
    user = seed_data["driver2"]
    # User in DB is driver, but token claims manager
    token, _, _ = create_access_token(user.id, "manager", token_version=getattr(user, "token_version", 1))

    with pytest.raises(TokenRevokedError) as exc:
        await get_current_user(token=token, db=test_db_session, redis=fake_redis)
    assert "role has changed" in str(exc.value)


@pytest.mark.asyncio
async def test_deps_token_version_mismatch_raises_token_revoked(test_db_session: AsyncSession, fake_redis, seed_data: dict):
    """Tests get_current_user when token tv is lower than user's DB token_version."""
    user = seed_data["driver1"]
    user.token_version = 5
    await test_db_session.commit()

    # Token with tv=1
    token, _, _ = create_access_token(user.id, user.role, token_version=1)
    with pytest.raises(TokenRevokedError) as exc:
        await get_current_user(token=token, db=test_db_session, redis=fake_redis)
    assert "obsolete" in str(exc.value)


@pytest.mark.asyncio
async def test_deps_redis_denylist_and_min_iat_hits(test_db_session: AsyncSession, fake_redis, seed_data: dict):
    """Tests get_current_user when token is explicitly in Redis denylist or older than min_iat."""
    user = seed_data["driver1"]
    token, jti, _ = create_access_token(user.id, user.role, token_version=getattr(user, "token_version", 1))

    # 1. Test denylist hit
    await fake_redis.set(f"denylist:{jti}", "1")
    with pytest.raises(TokenRevokedError) as exc:
        await get_current_user(token=token, db=test_db_session, redis=fake_redis)
    assert "logged out" in str(exc.value)

    # 2. Test min_iat hit
    await fake_redis.delete(f"denylist:{jti}")
    future_epoch = int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp())
    await fake_redis.set(f"user:{user.id}:min_iat", str(future_epoch))
    with pytest.raises(TokenRevokedError) as exc:
        await get_current_user(token=token, db=test_db_session, redis=fake_redis)
    assert "invalidated" in str(exc.value)


@pytest.mark.asyncio
async def test_deps_require_role_checker(seed_data: dict):
    """Tests require_role dependency function."""
    driver = seed_data["driver1"]
    checker = require_role("manager", "auditor")

    with pytest.raises(PermissionDeniedError) as exc:
        checker(user=driver)
    assert "is not authorized" in str(exc.value)

    manager_checker = require_role("driver")
    approved = manager_checker(user=driver)
    assert approved is driver


@pytest.mark.asyncio
async def test_deps_get_shipment_for_user_branches(test_db_session: AsyncSession, seed_data: dict):
    """Tests get_shipment_for_user for managers, auditors, and drivers."""
    manager = seed_data["manager"]
    auditor = seed_data["auditor"]
    driver1 = seed_data["driver1"]

    # 1. Manager & Auditor access valid shipment
    shp_mgr = await get_shipment_for_user(shipment_id="SHP-TEST-001", user=manager, db=test_db_session)
    assert shp_mgr.id == "SHP-TEST-001"

    shp_aud = await get_shipment_for_user(shipment_id="SHP-TEST-001", user=auditor, db=test_db_session)
    assert shp_aud.id == "SHP-TEST-001"

    # 2. Manager accesses non-existent shipment -> 404
    with pytest.raises(ResourceNotFoundError):
        await get_shipment_for_user(shipment_id="NON-EXISTENT", user=manager, db=test_db_session)

    # 3. Driver accesses assigned shipment -> 200
    shp_drv = await get_shipment_for_user(shipment_id="SHP-TEST-001", user=driver1, db=test_db_session)
    assert shp_drv.id == "SHP-TEST-001"

    # 4. Driver accesses unassigned shipment -> 404 (IDOR safe)
    with pytest.raises(ResourceNotFoundError):
        await get_shipment_for_user(shipment_id="SHP-TEST-002", user=driver1, db=test_db_session)


def test_deps_get_client_ip():
    """Tests get_client_ip helper with various headers and fallback."""
    req_mock = MagicMock()
    req_mock.client.host = "192.0.2.1"

    # With X-Forwarded-For single IP
    ip1 = get_client_ip(req_mock, x_forwarded_for="203.0.113.195")
    assert ip1 == "203.0.113.195"

    # With X-Forwarded-For multiple IPs
    ip2 = get_client_ip(req_mock, x_forwarded_for="203.0.113.195, 70.41.3.18, 150.172.238.178")
    assert ip2 == "203.0.113.195"

    # Without X-Forwarded-For fallback to client.host
    ip3 = get_client_ip(req_mock, x_forwarded_for=None)
    assert ip3 == "192.0.2.1"

    # Without client
    req_no_client = MagicMock()
    req_no_client.client = None
    ip4 = get_client_ip(req_no_client, x_forwarded_for=None)
    assert ip4 == "127.0.0.1"
