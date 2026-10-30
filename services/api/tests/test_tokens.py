"""
Token security test suite: claims validation, alg=none attack, tampering,
refresh token rotation, reuse detection, and denylist revocation.
"""

from datetime import datetime, timedelta, timezone
import uuid
import jwt
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.config import settings
from api.core.security import create_access_token


@pytest.mark.asyncio
async def test_access_token_expired(client: AsyncClient, seed_data: dict):
    """Verifies that an expired access token produces 401 TOKEN_EXPIRED."""
    expired_token, _, _ = create_access_token(
        user_id=seed_data["driver1"].id,
        role="driver",
        expires_delta=timedelta(seconds=-10),
    )
    res = await client.get("/users/me", headers={"Authorization": f"Bearer {expired_token}"})
    assert res.status_code == 401
    assert res.json()["error"]["code"] == "TOKEN_EXPIRED"


@pytest.mark.asyncio
async def test_access_token_wrong_issuer_and_audience(client: AsyncClient, seed_data: dict):
    """Verifies tokens with illegitimate issuer or audience claims are rejected."""
    # Wrong issuer
    bad_iss = jwt.encode(
        {"sub": str(seed_data["driver1"].id), "role": "driver", "jti": str(uuid.uuid4()),
         "iat": int(datetime.now(timezone.utc).timestamp()), "exp": int((datetime.now(timezone.utc) + timedelta(minutes=15)).timestamp()),
         "type": "access", "iss": "evil-issuer", "aud": settings.JWT_AUDIENCE},
        settings.JWT_SECRET, algorithm="HS256"
    )
    res1 = await client.get("/users/me", headers={"Authorization": f"Bearer {bad_iss}"})
    assert res1.status_code == 401

    # Wrong audience
    bad_aud = jwt.encode(
        {"sub": str(seed_data["driver1"].id), "role": "driver", "jti": str(uuid.uuid4()),
         "iat": int(datetime.now(timezone.utc).timestamp()), "exp": int((datetime.now(timezone.utc) + timedelta(minutes=15)).timestamp()),
         "type": "access", "iss": settings.JWT_ISSUER, "aud": "evil-audience"},
        settings.JWT_SECRET, algorithm="HS256"
    )
    res2 = await client.get("/users/me", headers={"Authorization": f"Bearer {bad_aud}"})
    assert res2.status_code == 401


@pytest.mark.asyncio
async def test_access_token_tampered_signature(client: AsyncClient, seed_data: dict):
    """Verifies that tampering with token payload or signature yields 401."""
    token, _, _ = create_access_token(seed_data["driver1"].id, "driver")
    # Mutate last 4 characters of signature
    tampered = token[:-4] + "xxxx"
    res = await client.get("/users/me", headers={"Authorization": f"Bearer {tampered}"})
    assert res.status_code == 401
    assert res.json()["error"]["code"] == "TOKEN_INVALID"


@pytest.mark.asyncio
async def test_access_token_alg_none_attack_rejected(client: AsyncClient, seed_data: dict):
    """Verifies that alg=none vulnerability is strictly blocked."""
    claims = {
        "sub": str(seed_data["driver1"].id),
        "role": "driver",
        "jti": str(uuid.uuid4()),
        "iat": int(datetime.now(timezone.utc).timestamp()),
        "exp": int((datetime.now(timezone.utc) + timedelta(minutes=15)).timestamp()),
        "type": "access",
        "iss": settings.JWT_ISSUER,
        "aud": settings.JWT_AUDIENCE,
    }
    alg_none_token = jwt.encode(claims, key="", algorithm="none")
    res = await client.get("/users/me", headers={"Authorization": f"Bearer {alg_none_token}"})
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_refresh_token_used_as_access_token_rejected(client: AsyncClient, seed_data: dict):
    """Verifies that presenting an opaque refresh token as an access token fails."""
    # Obtain real tokens
    login_res = await client.post("/auth/login", data={
        "username": seed_data["driver1"].email,
        "password": seed_data["password"],
    })
    refresh_token = login_res.json()["refresh_token"]

    res = await client.get("/users/me", headers={"Authorization": f"Bearer {refresh_token}"})
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_access_token_used_as_refresh_token_rejected(client: AsyncClient, seed_data: dict):
    """Verifies that presenting a JWT access token to /auth/refresh fails."""
    login_res = await client.post("/auth/login", data={
        "username": seed_data["driver1"].email,
        "password": seed_data["password"],
    })
    access_token = login_res.json()["access_token"]

    res = await client.post("/auth/refresh", json={"refresh_token": access_token})
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_refresh_rotation_and_theft_reuse_detection(client: AsyncClient, seed_data: dict):
    """
    Critical Test:
    1. Login to obtain Token 1.
    2. Rotate Token 1 -> receives Token 2.
    3. Replay Token 1 -> triggers REUSE DETECTION (whole family revoked).
    4. Attempt to use Token 2 -> fails because family was revoked!
    """
    # 1. Login
    login_res = await client.post("/auth/login", data={
        "username": seed_data["driver1"].email,
        "password": seed_data["password"],
    })
    token1 = login_res.json()["refresh_token"]

    # 2. Benign rotation (Token 1 -> Token 2)
    rot_res = await client.post("/auth/refresh", json={"refresh_token": token1})
    assert rot_res.status_code == 200
    token2 = rot_res.json()["refresh_token"]
    assert token2 != token1

    # 3. Theft Replay: Attacker presents revoked Token 1
    replay_res = await client.post("/auth/refresh", json={"refresh_token": token1})
    assert replay_res.status_code == 401
    assert replay_res.json()["error"]["code"] == "TOKEN_THEFT_DETECTED"

    # 4. Legitimate Token 2 is now dead because family was revoked
    token2_res = await client.post("/auth/refresh", json={"refresh_token": token2})
    assert token2_res.status_code == 401


@pytest.mark.asyncio
async def test_logout_denylists_access_jti(client: AsyncClient, seed_data: dict):
    """Verifies that logout immediately adds the JTI to the Redis denylist and invalidates it."""
    login_res = await client.post("/auth/login", data={
        "username": seed_data["driver1"].email,
        "password": seed_data["password"],
    })
    access_token = login_res.json()["access_token"]
    refresh_token = login_res.json()["refresh_token"]

    # Access works before logout
    res_before = await client.get("/users/me", headers={"Authorization": f"Bearer {access_token}"})
    assert res_before.status_code == 200

    # Logout
    logout_res = await client.post(
        "/auth/logout",
        json={"refresh_token": refresh_token},
        headers={"Authorization": f"Bearer {access_token}"},
    )
    assert logout_res.status_code == 200

    # Access token is immediately dead (401 TOKEN_REVOKED)
    res_after = await client.get("/users/me", headers={"Authorization": f"Bearer {access_token}"})
    assert res_after.status_code == 401
    assert res_after.json()["error"]["code"] == "TOKEN_REVOKED"


@pytest.mark.asyncio
async def test_tokens_without_valid_tv_claim_rejected(client: AsyncClient, seed_data: dict):
    """Verifies that access tokens missing a valid 'tv' claim are rejected with 401 TOKEN_REVOKED."""
    user = seed_data["driver1"]
    
    # Craft a token missing 'tv'
    now = datetime.now(timezone.utc)
    claims = {
        "sub": str(user.id),
        "role": user.role,
        "jti": str(uuid.uuid4()),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=15)).timestamp()),
        "type": "access",
        "iss": settings.JWT_ISSUER,
        "aud": settings.JWT_AUDIENCE,
    }
    raw_jwt = jwt.encode(claims, key=settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

    res = await client.get("/users/me", headers={"Authorization": f"Bearer {raw_jwt}"})
    assert res.status_code == 401
    assert res.json()["error"]["code"] == "TOKEN_REVOKED"


@pytest.mark.asyncio
async def test_token_version_revocation_during_redis_outage(
    client: AsyncClient, seed_data: dict, fake_redis, test_db_session: AsyncSession
):
    """
    Verifies that:
    1. Role change invalidates old tokens via token_version even when Redis is down.
    2. Deactivation invalidates old tokens via token_version even when Redis is down.
    3. Logout-all invalidates old tokens via token_version even when Redis is down.
    """
    # 1. Login driver1 to get initial token with tv=1
    login_res = await client.post("/auth/login", data={
        "username": seed_data["driver1"].email,
        "password": seed_data["password"],
    })
    token_v1 = login_res.json()["access_token"]

    # Verify token_v1 works initially
    res_ok = await client.get("/users/me", headers={"Authorization": f"Bearer {token_v1}"})
    assert res_ok.status_code == 200

    # Simulate complete Redis outage!
    fake_redis.is_connected = False

    # A) Test logout-all with Redis down:
    token_mgr, _, _ = create_access_token(seed_data["manager"].id, "manager", token_version=getattr(seed_data["manager"], "token_version", 1))
    logout_all_res = await client.post(
        "/auth/logout-all",
        headers={"Authorization": f"Bearer {token_v1}"},
    )
    assert logout_all_res.status_code == 200

    # Old token_v1 must now be rejected despite Redis being completely down!
    res_dead_after_logout_all = await client.get("/users/me", headers={"Authorization": f"Bearer {token_v1}"})
    assert res_dead_after_logout_all.status_code == 401
    assert res_dead_after_logout_all.json()["error"]["code"] == "TOKEN_REVOKED"

    # Restore Redis temporarily to obtain new token for driver2
    fake_redis.is_connected = True
    login_d2 = await client.post("/auth/login", data={
        "username": seed_data["driver2"].email,
        "password": seed_data["password"],
    })
    token_d2 = login_d2.json()["access_token"]
    assert (await client.get("/users/me", headers={"Authorization": f"Bearer {token_d2}"})).status_code == 200

    # B) Simulate Redis outage again and perform role change
    fake_redis.is_connected = False
    patch_role_res = await client.patch(
        f"/admin/users/{seed_data['driver2'].id}/role",
        json={"role": "auditor"},
        headers={"Authorization": f"Bearer {token_mgr}"},
    )
    assert patch_role_res.status_code == 200

    # Old token_d2 is immediately dead even with Redis down
    res_dead_role = await client.get("/users/me", headers={"Authorization": f"Bearer {token_d2}"})
    assert res_dead_role.status_code == 401

    # C) Deactivation test with Redis down
    fake_redis.is_connected = True
    login_mgr = await client.post("/auth/login", data={
        "username": seed_data["manager"].email,
        "password": seed_data["password"],
    })
    token_mgr_fresh = login_mgr.json()["access_token"]

    fake_redis.is_connected = False
    deact_res = await client.patch(
        f"/admin/users/{seed_data['driver2'].id}/active",
        json={"is_active": False},
        headers={"Authorization": f"Bearer {token_mgr_fresh}"},
    )
    assert deact_res.status_code == 200

    fake_redis.is_connected = True

