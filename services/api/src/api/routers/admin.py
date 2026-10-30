"""
Administrative router for user management, role adjustments, and driver-shipment assignments.
Restricted exclusively to callers with role='manager'.
"""

from datetime import datetime, timezone
from typing import List
import uuid
from fastapi import APIRouter, Depends, Query, Request, status
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.config import settings
from ..core.deps import get_client_ip, get_redis, require_role
from ..core.errors import PermissionDeniedError, ResourceNotFoundError
from ..db.models import User
from ..db.session import get_db_session
from ..repositories.audit_repo import AuditRepository
from ..repositories.shipment_repo import ShipmentRepository
from ..repositories.user_repo import UserRepository
from ..schemas.user import (
    AssignmentCreate,
    AssignmentRead,
    UserActiveUpdate,
    UserRead,
    UserRoleUpdate,
)

router = APIRouter(prefix="/admin", tags=["Admin Management"])


@router.get(
    "/users",
    response_model=List[UserRead],
    summary="List platform users (Manager only)",
)
async def list_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    manager: User = Depends(require_role("manager")),
    db: AsyncSession = Depends(get_db_session),
) -> List[UserRead]:
    users = await UserRepository.list_all(db, skip=skip, limit=limit)
    return [UserRead.model_validate(u) for u in users]


@router.patch(
    "/users/{user_id}/role",
    response_model=UserRead,
    summary="Change user role (Manager only)",
)
async def update_user_role(
    user_id: uuid.UUID,
    payload: UserRoleUpdate,
    request: Request,
    manager: User = Depends(require_role("manager")),
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
    client_ip: str = Depends(get_client_ip),
) -> UserRead:
    # 1. Anti-lockout self-protection
    if manager.id == user_id:
        raise PermissionDeniedError("Managers cannot modify their own role. Prevent accidental lockout.")

    target_user = await UserRepository.get_by_id(db, user_id)
    if not target_user:
        raise ResourceNotFoundError(f"User '{user_id}' not found.")

    old_role = target_user.role
    updated = await UserRepository.update_role(db, target_user, payload.role)

    # 2. Immediately invalidate user's active access tokens in Redis
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    try:
        await redis.set(
            f"user:{user_id}:min_iat",
            str(now_epoch),
            ex=settings.ACCESS_TOKEN_MINUTES * 60,
        )
    except Exception:
        pass

    # 3. Write immutable audit log
    await AuditRepository.create(
        db=db,
        actor_id=manager.id,
        action="UPDATE_USER_ROLE",
        target=str(user_id),
        metadata_json={"old_role": old_role, "new_role": payload.role},
        ip=client_ip,
    )

    return UserRead.model_validate(updated)


@router.patch(
    "/users/{user_id}/active",
    response_model=UserRead,
    summary="Activate or deactivate user account (Manager only)",
)
async def update_user_active_status(
    user_id: uuid.UUID,
    payload: UserActiveUpdate,
    request: Request,
    manager: User = Depends(require_role("manager")),
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
    client_ip: str = Depends(get_client_ip),
) -> UserRead:
    # 1. Anti-lockout self-protection
    if manager.id == user_id:
        raise PermissionDeniedError("Managers cannot deactivate their own account.")

    target_user = await UserRepository.get_by_id(db, user_id)
    if not target_user:
        raise ResourceNotFoundError(f"User '{user_id}' not found.")

    old_status = target_user.is_active
    updated = await UserRepository.update_active_status(db, target_user, payload.is_active)

    # 2. If deactivated, immediately invalidate active sessions
    if not payload.is_active:
        now_epoch = int(datetime.now(timezone.utc).timestamp())
        try:
            await redis.set(
                f"user:{user_id}:min_iat",
                str(now_epoch),
                ex=settings.ACCESS_TOKEN_MINUTES * 60,
            )
        except Exception:
            pass

    # 3. Audit trail
    await AuditRepository.create(
        db=db,
        actor_id=manager.id,
        action="UPDATE_USER_ACTIVE",
        target=str(user_id),
        metadata_json={"old_active": old_status, "new_active": payload.is_active},
        ip=client_ip,
    )

    return UserRead.model_validate(updated)


@router.post(
    "/assignments",
    response_model=AssignmentRead,
    status_code=status.HTTP_201_CREATED,
    summary="Assign a shipment to a driver (Manager only)",
)
async def assign_shipment(
    payload: AssignmentCreate,
    manager: User = Depends(require_role("manager")),
    db: AsyncSession = Depends(get_db_session),
    client_ip: str = Depends(get_client_ip),
) -> AssignmentRead:
    driver = await UserRepository.get_by_id(db, payload.user_id)
    if not driver:
        raise ResourceNotFoundError(f"Driver '{payload.user_id}' not found.")
    if driver.role != "driver":
        raise PermissionDeniedError(f"User '{driver.email}' is a '{driver.role}', not a driver.")

    shipment = await ShipmentRepository.get_by_id(db, payload.shipment_id)
    if not shipment:
        raise ResourceNotFoundError(f"Shipment '{payload.shipment_id}' not found.")

    # Idempotent assignment check
    if await ShipmentRepository.is_assigned(db, payload.user_id, payload.shipment_id):
        # Fetch existing assignment
        from ..db.models import ShipmentAssignment
        from sqlalchemy import select
        res = await db.execute(
            select(ShipmentAssignment).where(
                ShipmentAssignment.user_id == payload.user_id,
                ShipmentAssignment.shipment_id == payload.shipment_id,
            )
        )
        existing = res.scalar_one()
        return AssignmentRead.model_validate(existing)

    assignment = await ShipmentRepository.assign_driver(db, payload.user_id, payload.shipment_id)

    await AuditRepository.create(
        db=db,
        actor_id=manager.id,
        action="ASSIGN_SHIPMENT",
        target=payload.shipment_id,
        metadata_json={"driver_id": str(payload.user_id), "shipment_id": payload.shipment_id},
        ip=client_ip,
    )

    return AssignmentRead.model_validate(assignment)


@router.delete(
    "/assignments",
    status_code=status.HTTP_200_OK,
    summary="Unassign a shipment from a driver (Manager only)",
)
async def unassign_shipment(
    user_id: uuid.UUID = Query(...),
    shipment_id: str = Query(...),
    manager: User = Depends(require_role("manager")),
    db: AsyncSession = Depends(get_db_session),
    client_ip: str = Depends(get_client_ip),
) -> dict:
    removed = await ShipmentRepository.unassign_driver(db, user_id, shipment_id)
    if not removed:
        raise ResourceNotFoundError("Assignment does not exist.")

    await AuditRepository.create(
        db=db,
        actor_id=manager.id,
        action="UNASSIGN_SHIPMENT",
        target=shipment_id,
        metadata_json={"driver_id": str(user_id), "shipment_id": shipment_id},
        ip=client_ip,
    )
    return {"message": "Shipment assignment removed successfully."}
