"""
Comprehensive integration tests for User, Admin, and Shipment endpoints,
ensuring complete RBAC coverage and audit logging.
"""

import uuid
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from coldchain_common.db_models import AuditLog, Reading, ShipmentAssignment
from api.core.security import create_access_token


@pytest.mark.asyncio
async def test_users_me_and_profile_update(client: AsyncClient, seed_data: dict):
    """Tests GET /users/me and PATCH /users/me (full_name update)."""
    token_d1, _, _ = create_access_token(seed_data["driver1"].id, "driver")

    # 1. GET /users/me
    res = await client.get("/users/me", headers={"Authorization": f"Bearer {token_d1}"})
    assert res.status_code == 200
    data = res.json()
    assert data["email"] == seed_data["driver1"].email
    assert data["role"] == "driver"

    # 2. PATCH /users/me
    res_patch = await client.patch(
        "/users/me",
        json={"full_name": "Updated Driver Name"},
        headers={"Authorization": f"Bearer {token_d1}"},
    )
    assert res_patch.status_code == 200
    assert res_patch.json()["full_name"] == "Updated Driver Name"


@pytest.mark.asyncio
async def test_users_change_password(client: AsyncClient, seed_data: dict, test_db_session: AsyncSession):
    """Tests POST /users/me/change-password with validation, bad password, and successful update."""
    token_d1, _, _ = create_access_token(seed_data["driver1"].id, "driver")

    # 1. Wrong current password -> 401
    res_wrong = await client.post(
        "/users/me/change-password",
        json={"current_password": "WrongPassword123!", "new_password": "BrandNewSecretPassword123!"},
        headers={"Authorization": f"Bearer {token_d1}"},
    )
    assert res_wrong.status_code == 401

    # 2. Weak new password -> 422
    res_weak = await client.post(
        "/users/me/change-password",
        json={"current_password": seed_data["password"], "new_password": "short"},
        headers={"Authorization": f"Bearer {token_d1}"},
    )
    assert res_weak.status_code == 422

    # 3. Successful password change
    res_ok = await client.post(
        "/users/me/change-password",
        json={"current_password": seed_data["password"], "new_password": "BrandNewSecretPassword123!"},
        headers={"Authorization": f"Bearer {token_d1}"},
    )
    assert res_ok.status_code == 200

    # 4. Old password login now fails
    res_login_old = await client.post("/auth/login", data={"username": seed_data["driver1"].email, "password": seed_data["password"]})
    assert res_login_old.status_code == 401

    # 5. New password login succeeds
    res_login_new = await client.post("/auth/login", data={"username": seed_data["driver1"].email, "password": "BrandNewSecretPassword123!"})
    assert res_login_new.status_code == 200


@pytest.mark.asyncio
async def test_admin_list_users(client: AsyncClient, seed_data: dict):
    """Tests GET /admin/users listing with manager role."""
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")
    res = await client.get("/admin/users", headers={"Authorization": f"Bearer {token_mgr}"})
    assert res.status_code == 200
    users = res.json()
    assert len(users) >= 4


@pytest.mark.asyncio
async def test_admin_update_role_and_audit(client: AsyncClient, seed_data: dict, test_db_session: AsyncSession):
    """Tests PATCH /admin/users/{id}/role and verifies audit log entry."""
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")
    target_id = seed_data["driver2"].id

    res = await client.patch(
        f"/admin/users/{target_id}/role",
        json={"role": "auditor"},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert res.status_code == 200
    assert res.json()["role"] == "auditor"

    # Verify audit log entry
    logs = (await test_db_session.execute(
        select(AuditLog).where(AuditLog.action == "UPDATE_USER_ROLE")
    )).scalars().all()
    assert len(logs) >= 1
    assert logs[0].actor_id == seed_data["manager"].id
    assert logs[0].target == str(target_id)


@pytest.mark.asyncio
async def test_admin_activate_deactivate(client: AsyncClient, seed_data: dict, test_db_session: AsyncSession):
    """Tests PATCH /admin/users/{id}/active deactivating and reactivating a user."""
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")
    target_id = seed_data["driver2"].id

    # Deactivate
    res_deact = await client.patch(
        f"/admin/users/{target_id}/active",
        json={"is_active": False},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert res_deact.status_code == 200
    assert res_deact.json()["is_active"] is False

    # Reactivate
    res_act = await client.patch(
        f"/admin/users/{target_id}/active",
        json={"is_active": True},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert res_act.status_code == 200
    assert res_act.json()["is_active"] is True


@pytest.mark.asyncio
async def test_admin_shipment_assignments(client: AsyncClient, seed_data: dict, test_db_session: AsyncSession):
    """Tests POST /admin/assignments and DELETE /admin/assignments."""
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")
    driver1_id = str(seed_data["driver1"].id)

    # 1. Assign SHP-TEST-002 to Driver 1 (who only had SHP-TEST-001)
    res_assign = await client.post(
        "/admin/assignments",
        json={"user_id": driver1_id, "shipment_id": "SHP-TEST-002"},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert res_assign.status_code == 201
    assert res_assign.json()["shipment_id"] == "SHP-TEST-002"

    # 2. Driver 1 can now access SHP-TEST-002
    token_d1, _, _ = create_access_token(seed_data["driver1"].id, "driver")
    res_shp2 = await client.get("/shipments/SHP-TEST-002", headers={"Authorization": f"Bearer {token_d1}"})
    assert res_shp2.status_code == 200

    # 3. Unassign SHP-TEST-002
    res_unassign = await client.delete(
        f"/admin/assignments?user_id={driver1_id}&shipment_id=SHP-TEST-002",
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert res_unassign.status_code == 200

    # 4. Driver 1 now gets 404 again on SHP-TEST-002
    res_shp2_after = await client.get("/shipments/SHP-TEST-002", headers={"Authorization": f"Bearer {token_d1}"})
    assert res_shp2_after.status_code == 404


@pytest.mark.asyncio
async def test_shipment_readings_endpoint(client: AsyncClient, seed_data: dict, test_db_session: AsyncSession):
    """Tests GET /shipments/{id}/readings object-level auth and reading retrieval."""
    from datetime import datetime, timezone
    # Add a mock reading
    reading = Reading(
        device_id="DEV-TEST-001",
        shipment_id="SHP-TEST-001",
        ts=datetime.now(timezone.utc),
        temperature_c=-18.5,
        humidity_pct=85.0,
        door_open=False,
        compressor_on=True,
        compressor_status="NORMAL",
        ambient_temp_c=25.0,
        latitude=40.7128,
        longitude=-74.0060,
        speed_kmph=65.0,
        setpoint_c=-20.0,
        battery_pct=95.0,
        cargo_type="FROZEN_FOODS",
    )
    test_db_session.add(reading)
    await test_db_session.commit()

    token_d1, _, _ = create_access_token(seed_data["driver1"].id, "driver")
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")

    # Assigned driver -> 200
    res_d1 = await client.get("/shipments/SHP-TEST-001/readings?limit=10", headers={"Authorization": f"Bearer {token_d1}"})
    assert res_d1.status_code == 200
    data = res_d1.json()
    assert len(data) >= 1
    assert data[0]["shipment_id"] == "SHP-TEST-001"

    # Unassigned driver -> 404 (IDOR safe)
    res_unassigned = await client.get("/shipments/SHP-TEST-002/readings?limit=10", headers={"Authorization": f"Bearer {token_d1}"})
    assert res_unassigned.status_code == 404


@pytest.mark.asyncio
async def test_logout_all(client: AsyncClient, seed_data: dict):
    """Tests POST /auth/logout-all revokes all sessions."""
    # 1. Login to get tokens
    res_login = await client.post("/auth/login", data={"username": seed_data["driver1"].email, "password": seed_data["password"]})
    assert res_login.status_code == 200
    access_token = res_login.json()["access_token"]
    refresh_token = res_login.json()["refresh_token"]

    # 2. Call /auth/logout-all
    res_logout = await client.post("/auth/logout-all", headers={"Authorization": f"Bearer {access_token}"})
    assert res_logout.status_code == 200

    # 3. Refresh token now fails
    res_refresh = await client.post("/auth/refresh", json={"refresh_token": refresh_token})
    assert res_refresh.status_code == 401
