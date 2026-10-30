"""
Repository for recording immutable security and administrative audit events.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional
import uuid
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.models import AuditLog


class AuditRepository:
    """Async operations for inserting audit trail records."""

    @staticmethod
    async def create(
        db: AsyncSession,
        action: str,
        target: str,
        actor_id: Optional[uuid.UUID] = None,
        metadata_json: Optional[Dict[str, Any]] = None,
        ip: Optional[str] = None,
    ) -> AuditLog:
        entry = AuditLog(
            actor_id=actor_id,
            action=action,
            target=target,
            metadata_json=metadata_json,
            created_at=datetime.now(timezone.utc),
            ip=ip,
        )
        db.add(entry)
        await db.flush()
        return entry
