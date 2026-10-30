"""
Repository for RefreshToken management, rotation, and reuse revocation.
"""

from datetime import datetime, timezone
from typing import Optional
import uuid
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.models import RefreshToken


class TokenRepository:
    """Async database operations for RefreshToken lineage and rotation."""

    @staticmethod
    async def create(
        db: AsyncSession,
        user_id: uuid.UUID,
        token_hash: str,
        family_id: uuid.UUID,
        expires_at: datetime,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> RefreshToken:
        token = RefreshToken(
            id=uuid.uuid4(),
            user_id=user_id,
            token_hash=token_hash,
            family_id=family_id,
            issued_at=datetime.now(timezone.utc),
            expires_at=expires_at,
            revoked_at=None,
            replaced_by=None,
            user_agent=user_agent,
            ip_address=ip_address,
        )
        db.add(token)
        await db.flush()
        return token

    @staticmethod
    async def get_by_hash(db: AsyncSession, token_hash: str) -> Optional[RefreshToken]:
        result = await db.execute(
            select(RefreshToken).where(RefreshToken.token_hash == token_hash)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def revoke_token(
        db: AsyncSession,
        token: RefreshToken,
        replaced_by: Optional[str] = None,
    ) -> None:
        """Revokes a single refresh token during benign rotation."""
        token.revoked_at = datetime.now(timezone.utc)
        token.replaced_by = replaced_by
        await db.flush()

    @staticmethod
    async def revoke_family(db: AsyncSession, family_id: uuid.UUID) -> int:
        """
        Revokes all tokens belonging to a family.
        Triggered when reuse of an already-revoked token is detected (theft mitigation).
        """
        now = datetime.now(timezone.utc)
        stmt = (
            update(RefreshToken)
            .where(
                RefreshToken.family_id == family_id,
                RefreshToken.revoked_at.is_(None),
            )
            .values(revoked_at=now)
        )
        res = await db.execute(stmt)
        await db.commit()
        return res.rowcount

    @staticmethod
    async def revoke_all_for_user(db: AsyncSession, user_id: uuid.UUID) -> int:
        """Revokes all active refresh tokens for user (logout-all or password change)."""
        now = datetime.now(timezone.utc)
        stmt = (
            update(RefreshToken)
            .where(
                RefreshToken.user_id == user_id,
                RefreshToken.revoked_at.is_(None),
            )
            .values(revoked_at=now)
        )
        res = await db.execute(stmt)
        await db.commit()
        return res.rowcount
