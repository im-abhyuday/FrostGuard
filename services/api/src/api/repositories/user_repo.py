"""
Repository for User database operations.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
import uuid
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.models import User


class UserRepository:
    """Async database operations for User identity and account state."""

    @staticmethod
    async def get_by_id(db: AsyncSession, user_id: uuid.UUID) -> Optional[User]:
        result = await db.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_by_email(db: AsyncSession, email: str) -> Optional[User]:
        normalized = email.lower().strip()
        result = await db.execute(select(User).where(User.email == normalized))
        return result.scalar_one_or_none()

    @staticmethod
    async def create(
        db: AsyncSession,
        email: str,
        full_name: str,
        hashed_password: str,
        role: str = "driver",
        is_active: bool = True,
    ) -> User:
        user = User(
            id=uuid.uuid4(),
            email=email.lower().strip(),
            full_name=full_name.strip(),
            hashed_password=hashed_password,
            role=role.lower(),
            is_active=is_active,
            failed_login_count=0,
            locked_until=None,
        )
        db.add(user)
        await db.flush()
        return user

    @staticmethod
    async def record_failed_login(
        db: AsyncSession,
        user: User,
        max_failed: int = 5,
        lockout_minutes: int = 15,
    ) -> Tuple[int, Optional[datetime]]:
        """
        Increments failed login counter and conditionally triggers account lockout.
        Returns: (failed_login_count, locked_until)
        """
        user.failed_login_count += 1
        now = datetime.now(timezone.utc)

        if user.failed_login_count >= max_failed:
            user.locked_until = now + timedelta(minutes=lockout_minutes)

        await db.commit()
        return user.failed_login_count, user.locked_until

    @staticmethod
    async def record_successful_login(db: AsyncSession, user: User) -> None:
        """Resets failed login counter and updates last_login_at timestamp."""
        user.failed_login_count = 0
        user.locked_until = None
        user.last_login_at = datetime.now(timezone.utc)
        await db.flush()

    @staticmethod
    async def increment_token_version(db: AsyncSession, user: User) -> int:
        """Increments user's token version for instantaneous multi-session invalidation."""
        user.token_version = getattr(user, "token_version", 1) + 1
        user.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return user.token_version

    @staticmethod
    async def update_password(db: AsyncSession, user: User, new_hashed_password: str) -> None:
        user.hashed_password = new_hashed_password
        user.token_version = getattr(user, "token_version", 1) + 1
        user.updated_at = datetime.now(timezone.utc)
        await db.commit()

    @staticmethod
    async def update_full_name(db: AsyncSession, user: User, full_name: str) -> User:
        user.full_name = full_name.strip()
        user.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return user

    @staticmethod
    async def update_role(db: AsyncSession, user: User, new_role: str) -> User:
        user.role = new_role.lower()
        user.token_version = getattr(user, "token_version", 1) + 1
        user.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return user

    @staticmethod
    async def update_active_status(db: AsyncSession, user: User, is_active: bool) -> User:
        user.is_active = is_active
        user.token_version = getattr(user, "token_version", 1) + 1
        user.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return user

    @staticmethod
    async def list_all(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[User]:
        result = await db.execute(
            select(User).order_by(User.created_at.desc()).offset(skip).limit(limit)
        )
        return list(result.scalars().all())
