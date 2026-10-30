"""
Users router providing profile inspection, name updates, and password rotation.
"""

from fastapi import APIRouter, Depends, status
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.deps import get_current_user, get_redis, oauth2_scheme
from ..db.models import User
from ..db.session import get_db_session
from ..repositories.user_repo import UserRepository
from ..schemas.user import PasswordChangeRequest, UserRead, UserUpdate
from ..services.auth_service import AuthService

router = APIRouter(prefix="/users", tags=["Users"])


@router.get(
    "/me",
    response_model=UserRead,
    summary="Get authenticated caller's profile",
)
async def get_me(user: User = Depends(get_current_user)) -> UserRead:
    return UserRead.model_validate(user)


@router.patch(
    "/me",
    response_model=UserRead,
    summary="Update authenticated user's full name",
)
async def update_me(
    payload: UserUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
) -> UserRead:
    updated = await UserRepository.update_full_name(db, user, payload.full_name)
    return UserRead.model_validate(updated)


@router.post(
    "/me/change-password",
    status_code=status.HTTP_200_OK,
    summary="Change password and invalidate all active sessions",
)
async def change_password(
    payload: PasswordChangeRequest,
    current_token: str = Depends(oauth2_scheme),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
    redis: aioredis.Redis = Depends(get_redis),
) -> dict:
    await AuthService.change_password(
        db=db,
        redis=redis,
        user=user,
        current_password=payload.current_password,
        new_password=payload.new_password,
        current_token=current_token,
    )
    return {"message": "Password changed successfully. All active sessions have been revoked."}
