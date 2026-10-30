"""
Direct unit tests for AuthService methods, exercising registration, authentication,
token rotation, theft detection, password changes, and multi-session revocation.
"""

from datetime import datetime, timezone
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.errors import (
    APIException,
    InvalidCredentialsError,
    TokenInvalidError,
    TokenReuseError,
)
from api.core.security import decode_access_token
from api.schemas.auth import RegisterRequest
from api.services.auth_service import AuthService
from api.repositories.user_repo import UserRepository
from coldchain_common.db_models import User


@pytest.mark.asyncio
async def test_auth_service_register(test_db_session: AsyncSession):
    """Direct test of AuthService.register."""
    req = RegisterRequest(
        email="unit_driver@frostguard.io",
        full_name="Unit Driver",
        password="ValidPassword123!",
    )
    res = await AuthService.register(db=test_db_session, data=req)
    assert res.message == "If this email can be registered, the account has been created. You can now sign in."

    # Verify user was created in database
    user = (await test_db_session.execute(select(User).where(User.email == "unit_driver@frostguard.io"))).scalar_one()
    assert user.role == "driver"

    # Duplicate registration returns identical message without raising USER_ALREADY_EXISTS (anti-enumeration)
    dup_res = await AuthService.register(db=test_db_session, data=req)
    assert dup_res.message == res.message


@pytest.mark.asyncio
async def test_auth_service_register_concurrent_integrity_error(test_db_session: AsyncSession, monkeypatch):
    """Verifies that race condition IntegrityError is caught and returns identical RegisterResponse."""
    from sqlalchemy.exc import IntegrityError

    req = RegisterRequest(
        email="race_unit@frostguard.io",
        full_name="Race Unit",
        password="ValidPassword123!",
    )

    async def mock_create(*args, **kwargs):
        raise IntegrityError("mock unique violation", orig=Exception(), params={})

    monkeypatch.setattr(UserRepository, "create", mock_create)
    res = await AuthService.register(db=test_db_session, data=req)
    assert res.message == "If this email can be registered, the account has been created. You can now sign in."



@pytest.mark.asyncio
async def test_auth_service_login_and_logout(test_db_session: AsyncSession, fake_redis):
    """Direct test of AuthService.login, logout, and logout_all."""
    pwd = "ValidPassword123!"
    req = RegisterRequest(
        email="login_unit@frostguard.io",
        full_name="Login Unit",
        password=pwd,
    )
    await AuthService.register(db=test_db_session, data=req)
    user = await UserRepository.get_by_email(test_db_session, "login_unit@frostguard.io")

    # 1. Invalid password
    with pytest.raises(InvalidCredentialsError):
        await AuthService.login(
            db=test_db_session,
            redis=fake_redis,
            email="login_unit@frostguard.io",
            password="WrongPassword123!",
        )

    # 2. Non-existent email (timing equalization dummy check)
    with pytest.raises(InvalidCredentialsError):
        await AuthService.login(
            db=test_db_session,
            redis=fake_redis,
            email="nonexistent_unit@frostguard.io",
            password="SomePassword123!",
        )

    # 3. Successful login
    tok_res = await AuthService.login(
        db=test_db_session,
        redis=fake_redis,
        email="login_unit@frostguard.io",
        password=pwd,
        user_agent="UnitRunner/1.0",
        ip_address="127.0.0.1",
    )
    assert tok_res.access_token is not None
    assert tok_res.refresh_token is not None

    claims = decode_access_token(tok_res.access_token)
    jti = claims["jti"]
    exp = claims["exp"]

    # 4. Single session logout
    await AuthService.logout(
        db=test_db_session,
        redis=fake_redis,
        current_token=tok_res.access_token,
        raw_refresh_token=tok_res.refresh_token,
    )
    assert await fake_redis.get(f"denylist:{jti}") == "1"

    # 5. Logout all
    tok_res2 = await AuthService.login(
        db=test_db_session,
        redis=fake_redis,
        email="login_unit@frostguard.io",
        password=pwd,
    )
    await AuthService.logout_all(
        db=test_db_session,
        redis=fake_redis,
        user=user,
        current_token=tok_res2.access_token,
    )


@pytest.mark.asyncio
async def test_auth_service_refresh_and_theft(test_db_session: AsyncSession, fake_redis):
    """Direct test of AuthService.refresh_tokens rotation and theft reuse detection."""
    pwd = "ValidPassword123!"
    req = RegisterRequest(
        email="refresh_unit@frostguard.io",
        full_name="Refresh Unit",
        password=pwd,
    )
    await AuthService.register(db=test_db_session, data=req)

    tok1 = await AuthService.login(
        db=test_db_session,
        redis=fake_redis,
        email="refresh_unit@frostguard.io",
        password=pwd,
    )

    # 1. Invalid refresh token string
    with pytest.raises(TokenInvalidError):
        await AuthService.refresh_tokens(
            db=test_db_session,
            redis=fake_redis,
            raw_refresh_token="invalid_raw_token_xyz",
        )

    # 2. Benign rotation (tok1 -> tok2)
    tok2 = await AuthService.refresh_tokens(
        db=test_db_session,
        redis=fake_redis,
        raw_refresh_token=tok1.refresh_token,
    )
    assert tok2.refresh_token != tok1.refresh_token

    # 3. Theft replay of tok1 -> raises TokenReuseError and revokes family
    with pytest.raises(TokenReuseError):
        await AuthService.refresh_tokens(
            db=test_db_session,
            redis=fake_redis,
            raw_refresh_token=tok1.refresh_token,
        )


@pytest.mark.asyncio
async def test_auth_service_change_password(test_db_session: AsyncSession, fake_redis):
    """Direct test of AuthService.change_password."""
    pwd = "ValidPassword123!"
    req = RegisterRequest(
        email="change_pwd_unit@frostguard.io",
        full_name="Change Pwd Unit",
        password=pwd,
    )
    await AuthService.register(db=test_db_session, data=req)
    user = await UserRepository.get_by_email(test_db_session, "change_pwd_unit@frostguard.io")

    tok = await AuthService.login(
        db=test_db_session,
        redis=fake_redis,
        email="change_pwd_unit@frostguard.io",
        password=pwd,
    )

    # Wrong old password
    with pytest.raises(InvalidCredentialsError):
        await AuthService.change_password(
            db=test_db_session,
            redis=fake_redis,
            user=user,
            current_password="WrongOldPassword123!",
            new_password="NewSecurePassword123!",
            current_token=tok.access_token,
        )

    # Successful change
    await AuthService.change_password(
        db=test_db_session,
        redis=fake_redis,
        user=user,
        current_password=pwd,
        new_password="NewSecurePassword123!",
        current_token=tok.access_token,
    )
