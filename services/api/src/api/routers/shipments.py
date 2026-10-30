"""
Shipments and Alerts router providing protected placeholder endpoints to verify RBAC and IDOR protection.
"""

from typing import List
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..core.deps import get_current_user, get_shipment_for_user, require_role
from ..core.errors import PermissionDeniedError, ResourceNotFoundError
from ..db.models import Alert, Reading, Shipment, User
from ..db.session import get_db_session
from ..repositories.shipment_repo import ShipmentRepository
from ..schemas.user import AlertAckResponse, ReadingRead, ShipmentRead

router = APIRouter(tags=["Shipments & Telemetry"])


@router.get(
    "/shipments",
    response_model=List[ShipmentRead],
    summary="List shipments (Driver: assigned only; Manager/Auditor: all)",
)
async def list_shipments(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
) -> List[ShipmentRead]:
    shipments = await ShipmentRepository.list_for_user(db, user, skip=skip, limit=limit)
    return [ShipmentRead.model_validate(s) for s in shipments]


@router.get(
    "/shipments/{shipment_id}",
    response_model=ShipmentRead,
    summary="Get shipment details with IDOR protection",
)
async def get_shipment(
    shipment: Shipment = Depends(get_shipment_for_user),
) -> ShipmentRead:
    return ShipmentRead.model_validate(shipment)


@router.get(
    "/shipments/{shipment_id}/readings",
    response_model=List[ReadingRead],
    summary="Get latest telemetry readings for an authorized shipment",
)
async def get_shipment_readings(
    limit: int = Query(100, ge=1, le=500),
    shipment: Shipment = Depends(get_shipment_for_user),
    db: AsyncSession = Depends(get_db_session),
) -> List[ReadingRead]:
    readings = await ShipmentRepository.get_readings_for_shipment(db, shipment.id, limit=limit)
    return [ReadingRead.model_validate(r) for r in readings]


@router.post(
    "/alerts/{alert_id}/ack",
    response_model=AlertAckResponse,
    summary="Acknowledge alert (Driver on own shipment, Manager on any; Auditor forbidden)",
)
async def acknowledge_alert(
    alert_id: int,
    user: User = Depends(require_role("driver", "manager")),
    db: AsyncSession = Depends(get_db_session),
) -> AlertAckResponse:
    # 1. Fetch Alert
    res = await db.execute(select(Alert).where(Alert.id == alert_id))
    alert = res.scalar_one_or_none()
    if not alert:
        raise ResourceNotFoundError(f"Alert '{alert_id}' not found.")

    # 2. Object-level check: Drivers can only acknowledge alerts on their own shipments
    if user.role == "driver":
        if not alert.shipment_id or not await ShipmentRepository.is_assigned(db, user.id, alert.shipment_id):
            raise ResourceNotFoundError(f"Alert '{alert_id}' not found.")

    # 3. Acknowledge
    updated = await ShipmentRepository.acknowledge_alert(db, alert_id, user)
    return AlertAckResponse.model_validate(updated)
