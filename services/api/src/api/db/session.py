"""
Asynchronous database engine and sessionmaker configuration for ColdChainIQ API.
Uses SQLAlchemy 2.0 async engine with asyncpg / aiosqlite.
"""

from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy import text

from ..core.config import settings

# Engine instantiation
def create_engine_instance(dsn: str) -> AsyncEngine:
    connect_args = {}
    if "sqlite" in dsn:
        connect_args["check_same_thread"] = False

    return create_async_engine(
        dsn,
        echo=settings.DB_ECHO,
        pool_pre_ping=True,
        pool_size=settings.DB_POOL_SIZE if "sqlite" not in dsn else None,
        max_overflow=settings.DB_MAX_OVERFLOW if "sqlite" not in dsn else None,
        connect_args=connect_args,
    )


engine: AsyncEngine = create_engine_instance(settings.POSTGRES_DSN)
async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency yielding an isolated async database session with automatic rollback on error."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def ping_database() -> bool:
    """Readiness probe checking database connectivity."""
    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1"))
            return True
    except Exception:
        return False
