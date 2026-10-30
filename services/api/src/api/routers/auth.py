"""
Authentication router providing registration, login, refresh rotation, and logout endpoints.
"""

from typing import Optional
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.security import OAuth2PasswordRequestForm
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.config import settings
from ..core.deps import get_client_ip, get_current_user, get_redis, oauth2_scheme
from ..core.rate_limit import SlidingWindowRateLimiter
from ..db.models import User
from ..db.session import get_db_session
from ..schemas.auth import LogoutRequest, RefreshRequest, RegisterRequest, RegisterResponse, TokenResponse
from ..schemas.user import UserRead
from ..services.auth_service import AuthService

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post(
    "/register",
    response_model=RegisterResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Self-register as a new driver",
)
async def register(
    data: RegisterRequest,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
    client_ip: str = Depends(get_client_ip),
) -> RegisterResponse:
    # Rate limit: 5 registration attempts per hour per IP
    limiter = SlidingWindowRateLimiter(redis)
    await limiter.enforce(
        key=f"register:ip:{client_ip}",
        max_requests=settings.REGISTER_RATE_LIMIT_PER_HOUR,
        window_seconds=3600,
        error_message="Too many registration attempts. Please try again in an hour.",
    )

    return await AuthService.register(db, data)


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="OAuth2 Password Login for all roles",
)
async def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
    client_ip: str = Depends(get_client_ip),
) -> TokenResponse:
    # 1. Rate Limiting: 5 per minute per IP+email combo AND 20 per minute per IP
    limiter = SlidingWindowRateLimiter(redis)
    email_clean = form_data.username.lower().strip()

    await limiter.enforce(
        key=f"login:combo:{client_ip}:{email_clean}",
        max_requests=settings.LOGIN_RATE_LIMIT_PER_MIN,
        window_seconds=60,
        error_message="Too many login attempts for this account. Please wait 1 minute before retrying.",
    )
    await limiter.enforce(
        key=f"login:ip:{client_ip}",
        max_requests=settings.LOGIN_IP_RATE_LIMIT_PER_MIN,
        window_seconds=60,
        error_message="Too many login attempts from this network. Please wait 1 minute.",
    )

    user_agent = request.headers.get("user-agent")
    token_response = await AuthService.login(
        db=db,
        redis=redis,
        email=email_clean,
        password=form_data.password,
        user_agent=user_agent,
        ip_address=client_ip,
    )
    return token_response


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Rotate refresh token with theft/reuse detection",
)
async def refresh_token(
    payload: RefreshRequest,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
    client_ip: str = Depends(get_client_ip),
) -> TokenResponse:
    # Rate limit: 30 refresh requests per minute per IP
    limiter = SlidingWindowRateLimiter(redis)
    await limiter.enforce(
        key=f"refresh:ip:{client_ip}",
        max_requests=settings.REFRESH_RATE_LIMIT_PER_MIN,
        window_seconds=60,
    )

    user_agent = request.headers.get("user-agent")
    token_response = await AuthService.refresh_tokens(
        db=db,
        redis=redis,
        raw_refresh_token=payload.refresh_token,
        user_agent=user_agent,
        ip_address=client_ip,
    )
    return token_response


@router.post(
    "/logout",
    status_code=status.HTTP_200_OK,
    summary="Log out active session and denylist access token",
)
async def logout(
    payload: Optional[LogoutRequest] = None,
    current_token: str = Depends(oauth2_scheme),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
) -> dict:
    raw_refresh = payload.refresh_token if payload else None
    await AuthService.logout(
        db=db,
        redis=redis,
        current_token=current_token,
        raw_refresh_token=raw_refresh,
    )
    return {"message": "Successfully logged out."}


@router.post(
    "/logout-all",
    status_code=status.HTTP_200_OK,
    summary="Revoke all active refresh and access tokens for the user",
)
async def logout_all(
    current_token: str = Depends(oauth2_scheme),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
) -> dict:
    await AuthService.logout_all(
        db=db,
        redis=redis,
        user=user,
        current_token=current_token,
    )
    return {"message": "Successfully logged out of all devices."}
