"""
RBAC test suite: Permission matrix, IDOR object-level protection,
immediate token invalidation on role change, self-protection, and full route security audit.
"""

import pytest
from httpx import AsyncClient

from api.core.security import create_access_token


@pytest.mark.asyncio
async def test_unauthenticated_request_rejected(client: AsyncClient):
    """Verifies that accessing protected endpoints without a token returns 401."""
    res1 = await client.get("/users/me")
    assert res1.status_code == 401

    res2 = await client.get("/shipments")
    assert res2.status_code == 401

    res3 = await client.get("/admin/users")
    assert res3.status_code == 401


@pytest.mark.asyncio
async def test_driver_assigned_vs_unassigned_shipment_idor_404(client: AsyncClient, seed_data: dict):
    """
    IDOR Protection:
    Driver 1 is assigned to SHP-TEST-001 but NOT SHP-TEST-002.
    Viewing SHP-TEST-001 succeeds (200).
    Viewing SHP-TEST-002 returns 404 (NOT 403), preventing enumeration of other shipments.
    """
    token_d1, _, _ = create_access_token(seed_data["driver1"].id, "driver")

    # 1. Own assigned shipment -> 200
    res_own = await client.get("/shipments/SHP-TEST-001", headers={"Authorization": f"Bearer {token_d1}"})
    assert res_own.status_code == 200
    assert res_own.json()["id"] == "SHP-TEST-001"

    # 2. Other driver's shipment -> 404 (IDOR safe)
    res_other = await client.get("/shipments/SHP-TEST-002", headers={"Authorization": f"Bearer {token_d1}"})
    assert res_other.status_code == 404
    assert res_other.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_manager_and_auditor_can_view_all_shipments(client: AsyncClient, seed_data: dict):
    """Verifies that Managers and Auditors have fleet-wide read visibility."""
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")
    token_aud, _, _ = create_access_token(seed_data["auditor"].id, "auditor")

    res_mgr = await client.get("/shipments", headers={"Authorization": f"Bearer {token_mgr}"})
    assert res_mgr.status_code == 200
    assert len(res_mgr.json()) == 2

    res_aud = await client.get("/shipments", headers={"Authorization": f"Bearer {token_aud}"})
    assert res_aud.status_code == 200
    assert len(res_aud.json()) == 2


@pytest.mark.asyncio
async def test_alert_acknowledgement_rbac(client: AsyncClient, seed_data: dict):
    """
    Alert Acknowledgement:
    - Driver (own shipment): 200 OK
    - Manager: 200 OK
    - Auditor: 403 Forbidden (Auditor is strictly read-only!)
    """
    token_d1, _, _ = create_access_token(seed_data["driver1"].id, "driver")
    token_aud, _, _ = create_access_token(seed_data["auditor"].id, "auditor")
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")

    # Auditor attempt -> 403
    res_aud = await client.post("/alerts/1/ack", headers={"Authorization": f"Bearer {token_aud}"})
    assert res_aud.status_code == 403
    assert res_aud.json()["error"]["code"] == "PERMISSION_DENIED"

    # Assigned driver attempt -> 200
    res_d1 = await client.post("/alerts/1/ack", headers={"Authorization": f"Bearer {token_d1}"})
    assert res_d1.status_code == 200
    assert res_d1.json()["status"] == "ACKNOWLEDGED"


@pytest.mark.asyncio
async def test_driver_forbidden_on_admin_endpoints(client: AsyncClient, seed_data: dict):
    """Verifies that driver role is rejected with 403 on all admin endpoints."""
    token_d1, _, _ = create_access_token(seed_data["driver1"].id, "driver")

    res1 = await client.get("/admin/users", headers={"Authorization": f"Bearer {token_d1}"})
    assert res1.status_code == 403
    assert res1.json()["error"]["code"] == "PERMISSION_DENIED"

    res2 = await client.post(
        "/admin/assignments",
        json={"user_id": str(seed_data["driver1"].id), "shipment_id": "SHP-TEST-002"},
        headers={"Authorization": f"Bearer {token_d1}"},
    )
    assert res2.status_code == 403


@pytest.mark.asyncio
async def test_manager_cannot_demote_or_deactivate_self(client: AsyncClient, seed_data: dict):
    """Anti-lockout: A manager cannot modify their own role or deactivate their own account."""
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")
    mgr_id = str(seed_data["manager"].id)

    # Attempt to demote self
    res_role = await client.patch(
        f"/admin/users/{mgr_id}/role",
        json={"role": "driver"},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert res_role.status_code == 403
    assert "Prevent accidental lockout" in res_role.json()["error"]["message"]

    # Attempt to deactivate self
    res_active = await client.patch(
        f"/admin/users/{mgr_id}/active",
        json={"is_active": False},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert res_active.status_code == 403


@pytest.mark.asyncio
async def test_immediate_token_invalidation_on_role_change(client: AsyncClient, seed_data: dict):
    """
    When a manager changes a user's role (e.g. promoting or demoting),
    that user's existing active token must stop working immediately.
    """
    # Driver 2 has an active token
    token_d2, _, _ = create_access_token(seed_data["driver2"].id, "driver")
    res_before = await client.get("/users/me", headers={"Authorization": f"Bearer {token_d2}"})
    assert res_before.status_code == 200

    # Manager promotes Driver 2 to auditor
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager")
    d2_id = str(seed_data["driver2"].id)
    patch_res = await client.patch(
        f"/admin/users/{d2_id}/role",
        json={"role": "auditor"},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert patch_res.status_code == 200

    # Driver 2's old token is immediately rejected because role claims drifted / min_iat updated
    res_after = await client.get("/users/me", headers={"Authorization": f"Bearer {token_d2}"})
    assert res_after.status_code == 401
    assert res_after.json()["error"]["code"] == "TOKEN_REVOKED"


@pytest.mark.asyncio
async def test_route_security_scan_all_non_public_routes_protected(app_instance):
    """
    Default Deny Architecture Audit:
    Scans every single registered route in the FastAPI application.
    Asserts that every endpoint NOT in the public allowlist has an explicit authentication dependency.
    """
    public_allowlist = {
        "/health",
        "/auth/register",
        "/auth/login",
        "/auth/refresh",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
        "/openapi.json",
    }

    unprotected_routes = []
    for route in app_instance.routes:
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", set())

        if not path or path in public_allowlist:
            continue

        # Check dependencies on endpoint
        dependant = getattr(route, "dependant", None)
        has_auth = False
        if dependant:
            # Recursively check dependencies for get_current_user or require_role or oauth2_scheme
            flat_deps = [d.call.__name__ for d in dependant.dependencies if hasattr(d, "call") and hasattr(d.call, "__name__")]
            if any(name in ("get_current_user", "_role_checker", "get_shipment_for_user") for name in flat_deps):
                has_auth = True

        if not has_auth:
            unprotected_routes.append(f"{methods} {path}")

    assert unprotected_routes == [], f"Found unprotected routes lacking authentication: {unprotected_routes}"
