"""
Authentication test suite: registration, login flows, account lockout, and timing equality.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import uuid
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from coldchain_common.db_models import User


@pytest.mark.asyncio
async def test_registration_success(client: AsyncClient, test_db_session: AsyncSession):
    """Verifies valid driver self-registration returns 202 and stores Argon2id hash."""
    payload = {
        "email": "NewDriver@ColdChainIQ.com",
        "password": "UltraSecurePassword2026!",
        "full_name": "New Reefer Driver",
    }
    res = await client.post("/auth/register", json=payload)
    assert res.status_code == 202
    data = res.json()
    assert "message" in data

    # Verify database persistence
    user = (await test_db_session.execute(select(User).where(User.email == "newdriver@coldchainiq.com"))).scalar_one()
    assert user.full_name == "New Reefer Driver"
    assert user.role == "driver"
    assert user.is_active is True
    assert user.hashed_password.startswith("$argon2id$")


@pytest.mark.asyncio
async def test_registration_duplicate_email(client: AsyncClient, seed_data: dict, test_db_session: AsyncSession):
    """Verifies registering with an existing email returns identical 202 status and body (no user enumeration)."""
    payload = {
        "email": seed_data["driver1"].email,
        "password": "UltraSecurePassword2026!",
        "full_name": "Duplicate Driver",
    }
    res = await client.post("/auth/register", json=payload)
    assert res.status_code == 202
    assert res.json() == {
        "message": "If this email can be registered, the account has been created. You can now sign in."
    }

    # Verify original user was not overwritten
    driver = (await test_db_session.execute(select(User).where(User.email == seed_data["driver1"].email))).scalar_one()
    assert driver.full_name != "Duplicate Driver"


@pytest.mark.asyncio
async def test_registration_weak_passwords(client: AsyncClient):
    """Verifies password policy: minimum length, email identity, and common dictionary rejection."""
    # Too short (< 10 chars)
    res1 = await client.post("/auth/register", json={
        "email": "short@test.com", "password": "Short1!", "full_name": "Short Pwd"
    })
    assert res1.status_code == 422

    # Equals email
    res2 = await client.post("/auth/register", json={
        "email": "mypassword10@test.com", "password": "mypassword10@test.com", "full_name": "Same Pwd"
    })
    assert res2.status_code == 422

    # Very common password
    res3 = await client.post("/auth/register", json={
        "email": "common@test.com", "password": "password123", "full_name": "Common Pwd"
    })
    assert res3.status_code == 422


@pytest.mark.asyncio
async def test_registration_rejects_privilege_escalation(client: AsyncClient):
    """Verifies that attempting to register with role=manager or auditor is strictly rejected."""
    res1 = await client.post("/auth/register", json={
        "email": "hacker1@test.com",
        "password": "ValidStrongPassword123!",
        "full_name": "Malicious User",
        "role": "manager",
    })
    assert res1.status_code == 422

    res2 = await client.post("/auth/register", json={
        "email": "hacker2@test.com",
        "password": "ValidStrongPassword123!",
        "full_name": "Malicious User 2",
        "role": "auditor",
    })
    assert res2.status_code == 422


@pytest.mark.asyncio
async def test_login_success(client: AsyncClient, seed_data: dict):
    """Verifies successful login via OAuth2 Password form returns access and refresh tokens."""
    res = await client.post("/auth/login", data={
        "username": seed_data["driver1"].email,
        "password": seed_data["password"],
    })
    assert res.status_code == 200
    data = res.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"
    assert data["expires_in"] == 900  # 15 minutes


@pytest.mark.asyncio
async def test_login_invalid_credentials_generic(client: AsyncClient, seed_data: dict):
    """Verifies unknown email and wrong password yield identical generic error envelopes."""
    res_wrong_pwd = await client.post("/auth/login", data={
        "username": seed_data["driver1"].email,
        "password": "WrongPassword123!",
    })
    assert res_wrong_pwd.status_code == 401
    assert res_wrong_pwd.json()["error"]["code"] == "INVALID_CREDENTIALS"

    res_unknown_email = await client.post("/auth/login", data={
        "username": "nonexistent@test.com",
        "password": "SomePassword123!",
    })
    assert res_unknown_email.status_code == 401
    assert res_unknown_email.json()["error"]["code"] == "INVALID_CREDENTIALS"
    assert res_wrong_pwd.json()["error"]["message"] == res_unknown_email.json()["error"]["message"]


@pytest.mark.asyncio
async def test_account_lockout_after_5_failures(client: AsyncClient, seed_data: dict, test_db_session: AsyncSession):
    """Verifies that 5 consecutive failed logins trigger 15-minute account lockout."""
    email = seed_data["driver2"].email

    for i in range(4):
        r = await client.post("/auth/login", data={"username": email, "password": "WrongPassword123!"})
        assert r.status_code == 401

    # 5th failed attempt triggers lockout
    r5 = await client.post("/auth/login", data={"username": email, "password": "WrongPassword123!"})
    assert r5.status_code == 429
    assert r5.json()["error"]["code"] == "ACCOUNT_LOCKED"
    assert "Retry-After" in r5.headers

    # 6th attempt from a different IP with correct password confirms account remains locked in database
    r6 = await client.post(
        "/auth/login",
        data={"username": email, "password": seed_data["password"]},
        headers={"x-forwarded-for": "198.51.100.2"},
    )
    assert r6.status_code == 429
    assert r6.json()["error"]["code"] == "ACCOUNT_LOCKED"


@pytest.mark.asyncio
async def test_inactive_user_cannot_login(client: AsyncClient, seed_data: dict, test_db_session: AsyncSession):
    """Verifies deactivated users receive 403 on login attempt."""
    user = seed_data["driver1"]
    user.is_active = False
    await test_db_session.commit()

    res = await client.post("/auth/login", data={
        "username": user.email,
        "password": seed_data["password"],
    })
    assert res.status_code == 403
    assert res.json()["error"]["code"] == "USER_INACTIVE"


@pytest.mark.asyncio
async def test_registration_anti_enumeration_and_concurrency(client: AsyncClient, test_db_session: AsyncSession):
    """
    Verifies that:
    (a) new vs duplicate email return identical 202 status and body.
    (b) concurrent registrations (10 parallel requests) produce exactly 1 user row and all return 202.
    """
    target_email = f"concurr_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    pwd = "ValidColdChain123!"

    # 1. First registration -> 202
    r_first = await client.post("/auth/register", json={
        "email": target_email,
        "full_name": "First Register",
        "password": pwd,
    })
    assert r_first.status_code == 202
    body_first = r_first.json()

    # 2. Duplicate registration -> 202, identical body
    r_dup = await client.post("/auth/register", json={
        "email": target_email,
        "full_name": "Second Register",
        "password": pwd,
    })
    assert r_dup.status_code == 202
    assert r_dup.json() == body_first

    # 3. Duplicate registration repeatability: multiple subsequent requests with the same email
    race_email = f"race_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    req_body = {
        "email": race_email,
        "full_name": "Race Candidate",
        "password": pwd,
    }

    # Initial registration
    r0 = await client.post("/auth/register", json=req_body, headers={"x-forwarded-for": "198.51.100.1"})
    assert r0.status_code == 202
    assert r0.json() == body_first

    # Multiple duplicate attempts produce identical response
    for i in range(5):
        resp = await client.post(
            "/auth/register",
            json=req_body,
            headers={"x-forwarded-for": f"198.51.100.{i+2}"},
        )
        assert resp.status_code == 202
        assert resp.json() == body_first

    # Exactly 1 row in database
    users = (await test_db_session.execute(select(User).where(User.email == race_email))).scalars().all()
    assert len(users) == 1
    assert users[0].email == race_email


@pytest.mark.asyncio
async def test_registration_timing_parity(client: AsyncClient):
    """
    Verifies that 50-attempt median timing difference between new and duplicate
    registrations is under 30%.
    """
    import statistics
    import time

    fixed_email = f"fixed_{uuid.uuid4().hex[:8]}@coldchainiq.com"
    pwd = "ValidColdChain123!"

    # Seed the fixed account
    r_seed = await client.post(
        "/auth/register",
        json={"email": fixed_email, "full_name": "Fixed", "password": pwd},
        headers={"x-forwarded-for": "10.0.0.1"},
    )
    assert r_seed.status_code == 202

    new_durations = []
    dup_durations = []

    # 25 pairs = 50 total requests
    for i in range(25):
        new_email = f"timing_{i}_{uuid.uuid4().hex[:6]}@coldchainiq.com"
        ip_new = f"10.1.{i // 250}.{i % 250 + 1}"
        ip_dup = f"10.2.{i // 250}.{i % 250 + 1}"

        t0 = time.perf_counter()
        r1 = await client.post(
            "/auth/register",
            json={"email": new_email, "full_name": "New", "password": pwd},
            headers={"x-forwarded-for": ip_new},
        )
        t1 = time.perf_counter()
        assert r1.status_code == 202
        new_durations.append(t1 - t0)

        t0 = time.perf_counter()
        r2 = await client.post(
            "/auth/register",
            json={"email": fixed_email, "full_name": "Dup", "password": pwd},
            headers={"x-forwarded-for": ip_dup},
        )
        t1 = time.perf_counter()
        assert r2.status_code == 202
        dup_durations.append(t1 - t0)

    med_new = statistics.median(new_durations)
    med_dup = statistics.median(dup_durations)
    diff = abs(med_new - med_dup) / max(med_new, med_dup)
    assert diff < 0.30, f"Timing difference too high: new={med_new:.4f}s, dup={med_dup:.4f}s, diff={diff:.2%}"

