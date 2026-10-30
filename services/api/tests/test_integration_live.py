"""
Integration test suite for ColdChainIQ API running against REAL PostgreSQL 16 (port 5433)
and REAL Redis 7 (port 6380).

Marked with @pytest.mark.integration so it can be run independently:
- Fast unit tests (SQLite + FakeRedis): pytest -m "not integration"
- Live integration tests: pytest -m "integration"
"""

import asyncio
from datetime import datetime, timezone
import os
import uuid
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
import redis.asyncio as aioredis
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from api.core.config import settings
from api.core.deps import get_db_session, get_redis
from api.core.security import create_access_token, hash_password
from api.db.models import Base, RefreshToken, User
from api.main import create_app

# Live service endpoints
LIVE_POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN_LIVE",
    "postgresql+asyncpg://postgres:postgres@localhost:5433/coldchainiq",
)
LIVE_REDIS_URL = os.getenv("REDIS_URL_LIVE", "redis://localhost:6380/0")


@pytest_asyncio.fixture(scope="function")
async def live_engine():
    """Function-scoped async engine connected to PostgreSQL 16 on port 5433."""
    engine = create_async_engine(
        LIVE_POSTGRES_DSN,
        echo=False,
        pool_pre_ping=True,
    )
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def live_redis():
    """Function-scoped async Redis client connected to Redis 7 on port 6380."""
    client = aioredis.from_url(
        LIVE_REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        socket_timeout=3.0,
    )
    yield client
    await client.aclose()


@pytest_asyncio.fixture(scope="function")
async def live_session(live_engine) -> AsyncSession:
    """Yields clean session against live PostgreSQL."""
    session_maker = async_sessionmaker(
        bind=live_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    async with session_maker() as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def live_app(live_engine, live_redis):
    """Instantiates FastAPI app wired directly to live PostgreSQL and Redis."""
    app = create_app()

    session_maker = async_sessionmaker(
        bind=live_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async def _live_db():
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def _live_redis():
        yield live_redis

    app.dependency_overrides[get_db_session] = _live_db
    app.dependency_overrides[get_redis] = _live_redis

    return app


@pytest_asyncio.fixture(scope="function")
async def live_client(live_app) -> AsyncClient:
    """HTTP client bound to live FastAPI app."""
    transport = ASGITransport(app=live_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_login_and_auth(live_client: AsyncClient, live_session: AsyncSession):
    """Tests registration, login, and authenticated user extraction against real Postgres and Redis."""
    email = f"live_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    pwd = "SecureColdChain2026!"

    # 1. Register
    ip = f"10.88.{uuid.uuid4().hex[:4]}.1"
    r_reg = await live_client.post(
        "/auth/register",
        json={"email": email, "full_name": "Live Tester", "password": pwd},
        headers={"x-forwarded-for": ip},
    )
    assert r_reg.status_code == 202

    # 2. Login
    r_login = await live_client.post(
        "/auth/login",
        data={"username": email, "password": pwd},
    )
    assert r_login.status_code == 200
    token_data = r_login.json()
    assert "access_token" in token_data
    assert "refresh_token" in token_data

    # 3. Authenticated endpoint
    r_me = await live_client.get(
        "/users/me",
        headers={"Authorization": f"Bearer {token_data['access_token']}"},
    )
    assert r_me.status_code == 200
    user_me = r_me.json()
    assert user_me["email"] == email.lower()
    assert user_me["role"] == "driver"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_refresh_rotation_and_concurrent_reuse(
    live_client: AsyncClient, live_session: AsyncSession
):
    """
    Tests refresh token rotation and token family theft detection against live PostgreSQL:
    1. Login -> Token 1.
    2. Rotate Token 1 -> Token 2.
    3. Replay revoked Token 1 -> triggers REUSE DETECTION.
    4. Confirm all family tokens are revoked in PostgreSQL.
    """
    email = f"reuse_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    pwd = "SecureColdChain2026!"

    ip = f"10.88.{uuid.uuid4().hex[:4]}.1"
    await live_client.post(
        "/auth/register",
        json={"email": email, "full_name": "Reuse Tester", "password": pwd},
        headers={"x-forwarded-for": ip},
    )
    login_res = await live_client.post("/auth/login", data={"username": email, "password": pwd})
    token1 = login_res.json()["refresh_token"]

    # Benign rotation
    rot_res = await live_client.post("/auth/refresh", json={"refresh_token": token1})
    assert rot_res.status_code == 200
    token2 = rot_res.json()["refresh_token"]
    assert token2 != token1

    # Theft replay of token 1
    replay_res = await live_client.post("/auth/refresh", json={"refresh_token": token1})
    assert replay_res.status_code == 401
    assert replay_res.json()["error"]["code"] == "TOKEN_THEFT_DETECTED"

    # Legitimate token 2 is now dead because family was wiped in Postgres
    t2_res = await live_client.post("/auth/refresh", json={"refresh_token": token2})
    assert t2_res.status_code == 401


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_role_change_invalidation(
    live_client: AsyncClient, live_session: AsyncSession
):
    """
    Tests that a manager changing a user's role immediately invalidates active tokens
    via token_version (tv) and Redis min_iat in live services.
    """
    # Create driver and manager in Postgres
    driver_email = f"drv_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    mgr_email = f"mgr_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    pwd = "SecureColdChain2026!"

    ip1 = f"10.88.{uuid.uuid4().hex[:4]}.1"
    ip2 = f"10.88.{uuid.uuid4().hex[:4]}.2"
    await live_client.post(
        "/auth/register",
        json={"email": driver_email, "full_name": "Driver", "password": pwd},
        headers={"x-forwarded-for": ip1},
    )
    await live_client.post(
        "/auth/register",
        json={"email": mgr_email, "full_name": "Manager", "password": pwd},
        headers={"x-forwarded-for": ip2},
    )

    # Elevate manager directly in DB
    await live_session.execute(
        text("UPDATE users SET role = 'manager'::user_role_enum WHERE email = :email"),
        {"email": mgr_email},
    )
    await live_session.commit()

    # Login both
    r_drv_login = await live_client.post("/auth/login", data={"username": driver_email, "password": pwd})
    token_drv = r_drv_login.json()["access_token"]

    r_mgr_login = await live_client.post("/auth/login", data={"username": mgr_email, "password": pwd})
    token_mgr = r_mgr_login.json()["access_token"]

    # Driver token works
    assert (await live_client.get("/users/me", headers={"Authorization": f"Bearer {token_drv}"})).status_code == 200

    # Get driver user id
    driver_row = (await live_session.execute(select(User).where(User.email == driver_email))).scalar_one()

    # Manager promotes driver to auditor
    patch_res = await live_client.patch(
        f"/admin/users/{driver_row.id}/role",
        json={"role": "auditor"},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert patch_res.status_code == 200

    # Old driver token is dead immediately (token_version incremented)
    res_after = await live_client.get("/users/me", headers={"Authorization": f"Bearer {token_drv}"})
    assert res_after.status_code == 401
    assert res_after.json()["error"]["code"] == "TOKEN_REVOKED"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_case_insensitive_email_uniqueness(live_session: AsyncSession):
    """
    Tests that PostgreSQL uq_users_email_ci unique index strictly prevents
    case-insensitive duplicate email insertions.
    """
    email_lower = f"case_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    email_mixed = email_lower.upper()

    user1 = User(
        id=uuid.uuid4(),
        email=email_lower,
        full_name="User Lower",
        hashed_password="hash",
        role="driver",
        is_active=True,
        token_version=1,
    )
    live_session.add(user1)
    await live_session.commit()

    # Attempt to insert uppercase variant of the same email
    user2 = User(
        id=uuid.uuid4(),
        email=email_mixed,
        full_name="User Mixed",
        hashed_password="hash",
        role="driver",
        is_active=True,
        token_version=1,
    )
    live_session.add(user2)

    with pytest.raises(IntegrityError) as exc:
        await live_session.commit()
    assert "uq_users_email_ci" in str(exc.value)
    await live_session.rollback()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_enum_constraint(live_session: AsyncSession):
    """
    Tests that PostgreSQL user_role_enum rejects invalid roles.
    """
    with pytest.raises((DBAPIError, IntegrityError)) as exc:
        await live_session.execute(
            text("""
                INSERT INTO users (id, email, full_name, hashed_password, role, is_active, created_at, updated_at)
                VALUES (gen_random_uuid(), :email, 'Hacker', 'hash', 'superadmin'::user_role_enum, true, now(), now())
            """),
            {"email": f"hacker_{uuid.uuid4().hex[:6]}@coldchainiq.com"},
        )
        await live_session.commit()

    assert "invalid input value for enum user_role_enum" in str(exc.value)
    await live_session.rollback()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_concurrent_registrations_race(live_client: AsyncClient, live_session: AsyncSession):
    """
    Tests FIX 1 requirement (c) against REAL PostgreSQL:
    10 concurrent parallel requests attempting to register the exact same email.
    Produces exactly one user row in PostgreSQL and all 10 return HTTP 202 Accepted.
    """
    race_email = f"live_race_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    pwd = "ValidColdChain123!"
    body = {
        "email": race_email,
        "full_name": "Parallel Candidate",
        "password": pwd,
    }

    # Burst 10 parallel HTTP requests simultaneously with distinct client IPs
    tasks = [
        live_client.post("/auth/register", json=body, headers={"x-forwarded-for": f"10.50.0.{i+1}"})
        for i in range(10)
    ]
    responses = await asyncio.gather(*tasks)

    # All 10 responses must be 202 Accepted with identical generic anti-enumeration body
    for resp in responses:
        assert resp.status_code == 202
        assert resp.json()["message"] == "If this email can be registered, the account has been created. You can now sign in."

    # Exactly 1 row in real PostgreSQL
    users = (await live_session.execute(select(User).where(User.email == race_email))).scalars().all()
    assert len(users) == 1
    assert users[0].email == race_email
