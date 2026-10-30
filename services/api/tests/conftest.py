"""
Pytest configuration and async fixtures for ColdChainIQ API testing.
Provides in-memory SQLite database, mock Redis client, and httpx AsyncClient.
"""

import asyncio
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List
import uuid
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from api.db.models import Alert, Base, Device, Reading, Shipment, ShipmentAssignment, User
from api.core.config import settings
from api.core.deps import get_db_session, get_redis
from api.core.security import hash_password
from api.main import create_app


class FakeAsyncRedis:
    """Async in-memory Redis simulator supporting strings, sorted sets, and TTLs."""

    def __init__(self):
        self.strings: Dict[str, str] = {}
        self.zsets: Dict[str, Dict[str, float]] = {}
        self.ttls: Dict[str, int] = {}
        self.is_connected = True

    async def get(self, key: str) -> str:
        if not self.is_connected:
            raise ConnectionError("Simulated Redis connection failure")
        return self.strings.get(key)

    async def set(self, key: str, value: str, ex: int = None) -> bool:
        if not self.is_connected:
            raise ConnectionError("Simulated Redis connection failure")
        self.strings[key] = str(value)
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def delete(self, *keys: str) -> int:
        if not self.is_connected:
            raise ConnectionError("Simulated Redis connection failure")
        count = 0
        for k in keys:
            if k in self.strings:
                del self.strings[k]
                count += 1
            if k in self.zsets:
                del self.zsets[k]
                count += 1
        return count

    async def ping(self) -> bool:
        if not self.is_connected:
            raise ConnectionError("Simulated Redis down")
        return True

    async def close(self) -> None:
        pass

    def pipeline(self, transaction: bool = True):
        return FakeAsyncPipeline(self)


class FakeAsyncPipeline:
    def __init__(self, redis: FakeAsyncRedis):
        self.redis = redis
        self.commands = []

    def zremrangebyscore(self, key: str, min_val, max_val):
        self.commands.append(("zremrangebyscore", key, float(min_val), float(max_val)))
        return self

    def zcard(self, key: str):
        self.commands.append(("zcard", key))
        return self

    def zrange(self, key: str, start: int, stop: int, withscores: bool = False):
        self.commands.append(("zrange", key, start, stop, withscores))
        return self

    def zadd(self, key: str, mapping: Dict[str, float]):
        self.commands.append(("zadd", key, mapping))
        return self

    def expire(self, key: str, seconds: int):
        self.commands.append(("expire", key, seconds))
        return self

    async def execute(self) -> List:
        if not self.redis.is_connected:
            raise ConnectionError("Simulated Redis connection failure during pipeline")

        results = []
        for cmd in self.commands:
            action = cmd[0]
            if action == "zremrangebyscore":
                _, key, min_s, max_s = cmd
                z = self.redis.zsets.get(key, {})
                to_remove = [m for m, s in z.items() if min_s <= s <= max_s]
                for m in to_remove:
                    del z[m]
                results.append(len(to_remove))

            elif action == "zcard":
                _, key = cmd
                z = self.redis.zsets.get(key, {})
                results.append(len(z))

            elif action == "zrange":
                _, key, start, stop, withscores = cmd
                z = self.redis.zsets.get(key, {})
                sorted_items = sorted(z.items(), key=lambda x: x[1])
                sliced = sorted_items[start : stop + 1 if stop != -1 else None]
                if withscores:
                    results.append(sliced)
                else:
                    results.append([m for m, s in sliced])

            elif action == "zadd":
                _, key, mapping = cmd
                z = self.redis.zsets.setdefault(key, {})
                for m, s in mapping.items():
                    z[m] = float(s)
                results.append(len(mapping))

            elif action == "expire":
                _, key, sec = cmd
                self.redis.ttls[key] = sec
                results.append(True)

        self.commands.clear()
        return results


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


from sqlalchemy.pool import StaticPool


@pytest_asyncio.fixture(scope="function")
async def test_db_engine():
    """Creates isolated in-memory SQLite database engine per test."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def test_db_session(test_db_engine) -> AsyncGenerator[AsyncSession, None]:
    """Yields clean async session connected to the test database."""
    session_maker = async_sessionmaker(
        bind=test_db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    async with session_maker() as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def fake_redis() -> FakeAsyncRedis:
    """Yields fresh in-memory mock Redis client."""
    return FakeAsyncRedis()


@pytest_asyncio.fixture(scope="function")
async def app_instance(test_db_session: AsyncSession, fake_redis: FakeAsyncRedis):
    """Instantiates FastAPI application with overridden test dependencies."""
    app = create_app()

    async def _override_db():
        yield test_db_session

    async def _override_redis():
        yield fake_redis

    app.dependency_overrides[get_db_session] = _override_db
    app.dependency_overrides[get_redis] = _override_redis

    return app


@pytest_asyncio.fixture(scope="function")
async def client(app_instance) -> AsyncGenerator[AsyncClient, None]:
    """Yields httpx.AsyncClient bound to the test FastAPI app."""
    transport = ASGITransport(app=app_instance)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture(scope="function")
async def seed_data(test_db_session: AsyncSession):
    """Seeds baseline Manager, Auditor, and Driver accounts with shipments."""
    pwd_hash = hash_password("ColdChain2026!Secure")

    manager = User(
        id=uuid.uuid4(),
        email="manager@test.com",
        full_name="Manager User",
        hashed_password=pwd_hash,
        role="manager",
        is_active=True,
    )
    auditor = User(
        id=uuid.uuid4(),
        email="auditor@test.com",
        full_name="Auditor User",
        hashed_password=pwd_hash,
        role="auditor",
        is_active=True,
    )
    driver1 = User(
        id=uuid.uuid4(),
        email="driver1@test.com",
        full_name="Driver One",
        hashed_password=pwd_hash,
        role="driver",
        is_active=True,
    )
    driver2 = User(
        id=uuid.uuid4(),
        email="driver2@test.com",
        full_name="Driver Two",
        hashed_password=pwd_hash,
        role="driver",
        is_active=True,
    )

    shp1 = Shipment(
        id="SHP-TEST-001",
        cargo_type="VACCINE",
        origin="Pune",
        destination="Mumbai",
        setpoint_c=5.0,
        safe_min_c=2.0,
        safe_max_c=8.0,
        status="IN_TRANSIT",
    )
    shp2 = Shipment(
        id="SHP-TEST-002",
        cargo_type="DAIRY",
        origin="Delhi",
        destination="Jaipur",
        setpoint_c=3.0,
        safe_min_c=1.0,
        safe_max_c=4.0,
        status="IN_TRANSIT",
    )

    assign1 = ShipmentAssignment(
        user_id=driver1.id,
        shipment_id=shp1.id,
    )

    alert1 = Alert(
        id=1,
        shipment_id=shp1.id,
        severity="WARNING",
        risk_score=0.75,
        status="OPEN",
        reasons={"violation": "temperature_spike"},
    )

    test_db_session.add_all([manager, auditor, driver1, driver2, shp1, shp2, assign1, alert1])
    await test_db_session.commit()

    return {
        "manager": manager,
        "auditor": auditor,
        "driver1": driver1,
        "driver2": driver2,
        "shp1": shp1,
        "shp2": shp2,
        "alert1": alert1,
        "password": "ColdChain2026!Secure",
    }
