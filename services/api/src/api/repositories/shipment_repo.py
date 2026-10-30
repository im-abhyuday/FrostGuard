"""
Repository for Shipment queries, readings retrieval, and driver assignments.
"""

from datetime import datetime, timezone
from typing import List, Optional
import uuid
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.models import Alert, Reading, Shipment, ShipmentAssignment, User


class ShipmentRepository:
    """Async database operations for shipments and driver assignment links."""

    @staticmethod
    async def get_by_id(db: AsyncSession, shipment_id: str) -> Optional[Shipment]:
        result = await db.execute(select(Shipment).where(Shipment.id == shipment_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def list_for_user(
        db: AsyncSession,
        user: User,
        skip: int = 0,
        limit: int = 50,
    ) -> List[Shipment]:
        """
        Lists shipments based on caller's role:
        - Manager / Auditor: All shipments across the fleet.
        - Driver: Strictly shipments assigned to this driver.
        """
        if user.role in ("manager", "auditor"):
            stmt = select(Shipment).order_by(Shipment.created_at.desc()).offset(skip).limit(limit)
            res = await db.execute(stmt)
            return list(res.scalars().all())

        # Driver role: Join with shipment_assignments
        stmt = (
            select(Shipment)
            .join(ShipmentAssignment, ShipmentAssignment.shipment_id == Shipment.id)
            .where(ShipmentAssignment.user_id == user.id)
            .order_by(Shipment.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        res = await db.execute(stmt)
        return list(res.scalars().all())

    @staticmethod
    async def get_readings_for_shipment(
        db: AsyncSession,
        shipment_id: str,
        limit: int = 100,
    ) -> List[Reading]:
        """Fetches latest readings for a shipment in chronological descending order."""
        stmt = (
            select(Reading)
            .where(Reading.shipment_id == shipment_id)
            .order_by(Reading.ts.desc())
            .limit(limit)
        )
        res = await db.execute(stmt)
        return list(res.scalars().all())

    @staticmethod
    async def assign_driver(
        db: AsyncSession,
        user_id: uuid.UUID,
        shipment_id: str,
    ) -> ShipmentAssignment:
        assignment = ShipmentAssignment(
            user_id=user_id,
            shipment_id=shipment_id,
            assigned_at=datetime.now(timezone.utc),
        )
        db.add(assignment)
        await db.flush()
        return assignment

    @staticmethod
    async def unassign_driver(
        db: AsyncSession,
        user_id: uuid.UUID,
        shipment_id: str,
    ) -> bool:
        stmt = delete(ShipmentAssignment).where(
            ShipmentAssignment.user_id == user_id,
            ShipmentAssignment.shipment_id == shipment_id,
        )
        res = await db.execute(stmt)
        await db.flush()
        return res.rowcount > 0

    @staticmethod
    async def is_assigned(
        db: AsyncSession,
        user_id: uuid.UUID,
        shipment_id: str,
    ) -> bool:
        stmt = select(ShipmentAssignment).where(
            ShipmentAssignment.user_id == user_id,
            ShipmentAssignment.shipment_id == shipment_id,
        )
        res = await db.execute(stmt)
        return res.scalar_one_or_none() is not None

    @staticmethod
    async def acknowledge_alert(
        db: AsyncSession,
        alert_id: int,
        user: User,
    ) -> Optional[Alert]:
        """Acknowledges an alert if the caller is authorized."""
        res = await db.execute(select(Alert).where(Alert.id == alert_id))
        alert = res.scalar_one_or_none()
        if not alert:
            return None

        alert.status = "ACKNOWLEDGED"
        alert.acknowledged_by = user.email
        alert.acknowledged_at = datetime.now(timezone.utc)
        await db.flush()
        return alert
