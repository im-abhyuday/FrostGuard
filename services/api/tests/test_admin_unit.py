"""
Direct unit tests for admin router functions:
update_user_role, update_user_active_status, assign_shipment, unassign_shipment.
"""

from unittest.mock import AsyncMock, MagicMock
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from coldchain_common.db_models import Shipment, User
from api.core.errors import PermissionDeniedError, ResourceNotFoundError
from api.routers.admin import (
    assign_shipment,
    list_users,
    unassign_shipment,
    update_user_active_status,
    update_user_role,
)
from api.schemas.user import AssignmentCreate, UserActiveUpdate, UserRoleUpdate


@pytest.mark.asyncio
async def test_admin_functions_direct(test_db_session: AsyncSession, seed_data: dict, fake_redis):
    """Directly invokes admin handler functions to test business logic and audit logging."""
    manager = seed_data["manager"]
    driver1 = seed_data["driver1"]
    driver2 = seed_data["driver2"]
    shp1 = seed_data["shp1"]
    mock_request = MagicMock()

    # 1. list_users
    users_list = await list_users(skip=0, limit=10, manager=manager, db=test_db_session)
    assert len(users_list) >= 4

    # 2. update_user_role
    # Prevent self-demotion
    with pytest.raises(PermissionDeniedError):
        await update_user_role(
            user_id=manager.id,
            payload=UserRoleUpdate(role="auditor"),
            request=mock_request,
            manager=manager,
            db=test_db_session,
            redis=fake_redis,
            client_ip="127.0.0.1",
        )

    # Valid role update
    res_role = await update_user_role(
        user_id=driver2.id,
        payload=UserRoleUpdate(role="auditor"),
        request=mock_request,
        manager=manager,
        db=test_db_session,
        redis=fake_redis,
        client_ip="127.0.0.1",
    )
    assert res_role.role == "auditor"

    # Nonexistent user
    import uuid
    with pytest.raises(ResourceNotFoundError):
        await update_user_role(
            user_id=uuid.uuid4(),
            payload=UserRoleUpdate(role="auditor"),
            request=mock_request,
            manager=manager,
            db=test_db_session,
            redis=fake_redis,
            client_ip="127.0.0.1",
        )

    # 3. update_user_active_status
    # Prevent self-deactivation
    with pytest.raises(PermissionDeniedError):
        await update_user_active_status(
            user_id=manager.id,
            payload=UserActiveUpdate(is_active=False),
            request=mock_request,
            manager=manager,
            db=test_db_session,
            redis=fake_redis,
            client_ip="127.0.0.1",
        )

    # Valid deactivation
    res_deact = await update_user_active_status(
        user_id=driver2.id,
        payload=UserActiveUpdate(is_active=False),
        request=mock_request,
        manager=manager,
        db=test_db_session,
        redis=fake_redis,
        client_ip="127.0.0.1",
    )
    assert res_deact.is_active is False

    # 4. assign_shipment and unassign_shipment
    # Valid assignment
    res_assign = await assign_shipment(
        payload=AssignmentCreate(user_id=driver1.id, shipment_id="SHP-TEST-002"),
        manager=manager,
        db=test_db_session,
        client_ip="127.0.0.1",
    )
    assert res_assign.shipment_id == "SHP-TEST-002"

    # Idempotent re-assignment
    res_assign2 = await assign_shipment(
        payload=AssignmentCreate(user_id=driver1.id, shipment_id="SHP-TEST-002"),
        manager=manager,
        db=test_db_session,
        client_ip="127.0.0.1",
    )
    assert res_assign2.shipment_id == "SHP-TEST-002"

    # Valid unassignment
    res_unassign = await unassign_shipment(
        user_id=driver1.id,
        shipment_id="SHP-TEST-002",
        manager=manager,
        db=test_db_session,
        client_ip="127.0.0.1",
    )
    assert "successfully" in res_unassign["message"]

    # Nonexistent unassignment -> 404
    with pytest.raises(ResourceNotFoundError):
        await unassign_shipment(
            user_id=driver1.id,
            shipment_id="SHP-NONEXISTENT",
            manager=manager,
            db=test_db_session,
            client_ip="127.0.0.1",
        )
