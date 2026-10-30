"""
Business logic service for Authentication, Token Lifecycle, Rotation, and Theft Mitigation.
"""

from datetime import datetime, timedelta, timezone
import logging
from typing import Optional, Tuple
import uuid
import hashlib
import redis.asyncio as aioredis
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.config import settings
from ..core.errors import (
    APIException,
    AccountLockedError,
    InvalidCredentialsError,
    TokenExpiredError,
    TokenInvalidError,
    TokenReuseError,
    UserInactiveError,
)
from ..core.security import (
    create_access_token,
    decode_access_token,
    generate_refresh_token,
    hash_password,
    hash_token,
    password_needs_rehash,
    validate_password_policy,
    verify_dummy_password,
    verify_password,
)
from ..db.models import User
from ..repositories.token_repo import TokenRepository
from ..repositories.user_repo import UserRepository
from ..schemas.auth import RegisterRequest, RegisterResponse, TokenResponse

logger = logging.getLogger("coldchainiq.api.auth")


class AuthService:
    """Orchestrates authentication, token lifecycle, rate limiting, and password policies."""

    @staticmethod
    async def register(db: AsyncSession, data: RegisterRequest) -> RegisterResponse:
        """
        Self-registers a new driver.
        Mitigates account enumeration: duplicate email returns identical status & body.
        """
        if not settings.ALLOW_SELF_REGISTRATION:
            raise APIException(
                status_code=403,
                code="REGISTRATION_DISABLED",
                message="Self-registration is currently disabled.",
            )

        generic_response = RegisterResponse(
            message="If this email can be registered, the account has been created. You can now sign in."
        )

        existing = await UserRepository.get_by_email(db, data.email)
        if existing:
            email_hash = hashlib.sha256(data.email.encode("utf-8")).hexdigest()[:12]
            logger.info(f"Duplicate registration attempt suppressed for email_hash={email_hash}")
            # Timing mitigation: run password hashing so timing matches new user path
            hash_password(data.password)
            return generic_response

        hashed = hash_password(data.password)
        is_active = not settings.REQUIRE_APPROVAL

        try:
            user = await UserRepository.create(
                db=db,
                email=data.email,
                full_name=data.full_name,
                hashed_password=hashed,
                role="driver",  # Self-registration is strictly role=driver
                is_active=is_active,
            )
            await db.commit()
            logger.info(f"User registered successfully (active={is_active})")
            return generic_response
        except IntegrityError:
            await db.rollback()
            logger.info("Concurrent duplicate registration race condition suppressed")
            return generic_response

    @staticmethod
    async def login(
        db: AsyncSession,
        redis: aioredis.Redis,
        email: str,
        password: str,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> TokenResponse:
        """
        Authenticates user credentials, handles account lockout, transparent password rehashing,
        and generates an initial token family.
        """
        user = await UserRepository.get_by_email(db, email)
        now = datetime.now(timezone.utc)

        # 1. Mitigation against timing attacks for nonexistent accounts
        if not user:
            verify_dummy_password(password)
            raise InvalidCredentialsError("Invalid email or password.")

        # 2. Account Lockout Check
        if user.locked_until is not None:
            locked_until = user.locked_until
            if locked_until.tzinfo is None:
                locked_until = locked_until.replace(tzinfo=timezone.utc)
            if locked_until > now:
                remaining_sec = max(1, int((locked_until - now).total_seconds()))
                raise AccountLockedError(retry_after_sec=remaining_sec)
            else:
                # Lockout period elapsed, unlock
                user.locked_until = None
                user.failed_login_count = 0

        # 3. Verify Password with Argon2id
        if not verify_password(password, user.hashed_password):
            failed_count, locked_until = await UserRepository.record_failed_login(
                db=db,
                user=user,
                max_failed=settings.MAX_FAILED_LOGINS,
                lockout_minutes=settings.ACCOUNT_LOCKOUT_MINUTES,
            )
            if locked_until is not None:
                logger.warning(f"Account locked due to 5 consecutive failed logins: {user.email}")
                raise AccountLockedError(retry_after_sec=settings.ACCOUNT_LOCKOUT_MINUTES * 60)

            raise InvalidCredentialsError("Invalid email or password.")

        # 4. Active Account Guard
        if not user.is_active:
            raise UserInactiveError("Your account has been deactivated. Contact an administrator.")

        # 5. Transparent Argon2id Rehash on Parameter Changes
        if password_needs_rehash(user.hashed_password):
            new_hash = hash_password(password)
            await UserRepository.update_password(db, user, new_hash)
            logger.info(f"Transparently rehashed password for user: {user.email}")

        # 6. Reset Failures on Successful Login
        await UserRepository.record_successful_login(db, user)

        # 7. Issue Tokens
        access_token, _, expires_in = create_access_token(
            user.id, user.role, token_version=getattr(user, "token_version", 1)
        )
        raw_refresh, refresh_hash = generate_refresh_token()
        family_id = uuid.uuid4()
        refresh_expires = now + timedelta(days=settings.REFRESH_TOKEN_DAYS)

        await TokenRepository.create(
            db=db,
            user_id=user.id,
            token_hash=refresh_hash,
            family_id=family_id,
            expires_at=refresh_expires,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        return TokenResponse(
            access_token=access_token,
            refresh_token=raw_refresh,
            token_type="bearer",
            expires_in=expires_in,
        )

    @staticmethod
    async def refresh_tokens(
        db: AsyncSession,
        redis: aioredis.Redis,
        raw_refresh_token: str,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> TokenResponse:
        """
        Rotates opaque refresh token and detects reuse/theft attacks.
        """
        token_hash = hash_token(raw_refresh_token)
        stored_token = await TokenRepository.get_by_hash(db, token_hash)

        if not stored_token:
            raise TokenInvalidError("Invalid or unknown refresh token.")

        now = datetime.now(timezone.utc)

        # REUSE DETECTION: If a previously revoked refresh token is presented again,
        # an attacker may have compromised a rotated token!
        if stored_token.revoked_at is not None:
            logger.critical(
                f"SECURITY ALERT: Refresh token reuse detected! User: {stored_token.user_id}, "
                f"Family: {stored_token.family_id}. Revoking entire token family."
            )
            # Revoke entire token family
            await TokenRepository.revoke_family(db, stored_token.family_id)

            # Invalidate all active JWT access tokens for this user in Redis
            try:
                await redis.set(
                    f"user:{stored_token.user_id}:min_iat",
                    str(int(now.timestamp())),
                    ex=settings.ACCESS_TOKEN_MINUTES * 60,
                )
            except Exception:
                pass

            raise TokenReuseError("Refresh token reuse detected. Session terminated for security.")

        # Check Expiration
        expires_at = stored_token.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at < now:
            raise TokenExpiredError("Refresh token has expired. Please log in again.")

        # Verify User Status
        user = await UserRepository.get_by_id(db, stored_token.user_id)
        if not user or not user.is_active:
            raise UserInactiveError("User account is inactive or disabled.")

        # BENIGN ROTATION:
        # Revoke old token and issue new token in the SAME family
        new_raw_token, new_token_hash = generate_refresh_token()
        await TokenRepository.revoke_token(db, stored_token, replaced_by=new_token_hash)

        new_expires = now + timedelta(days=settings.REFRESH_TOKEN_DAYS)
        await TokenRepository.create(
            db=db,
            user_id=user.id,
            token_hash=new_token_hash,
            family_id=stored_token.family_id,
            expires_at=new_expires,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        access_token, _, expires_in = create_access_token(
            user.id, user.role, token_version=getattr(user, "token_version", 1)
        )
        return TokenResponse(
            access_token=access_token,
            refresh_token=new_raw_token,
            token_type="bearer",
            expires_in=expires_in,
        )

    @staticmethod
    async def logout(
        db: AsyncSession,
        redis: aioredis.Redis,
        current_token: str,
        raw_refresh_token: Optional[str] = None,
    ) -> None:
        """
        Revokes presented refresh token and denylists active access token in Redis.
        """
        # 1. Denylist Access Token JTI
        try:
            payload = decode_access_token(current_token)
            jti = payload.get("jti")
            exp = payload.get("exp", 0)
            now = int(datetime.now(timezone.utc).timestamp())
            ttl = max(1, exp - now)

            await redis.set(f"denylist:{jti}", "1", ex=ttl)
        except Exception as e:
            logger.warning(
                f"REDIS OUTAGE WARNING during logout: Unable to denylist JTI in Redis ({e}). "
                f"Refresh token is revoked in PostgreSQL, but current access token remains "
                f"valid until natural expiration (max {settings.ACCESS_TOKEN_MINUTES}m)."
            )

        # 2. Revoke Refresh Token if provided
        if raw_refresh_token:
            token_hash = hash_token(raw_refresh_token)
            token = await TokenRepository.get_by_hash(db, token_hash)
            if token and token.revoked_at is None:
                await TokenRepository.revoke_token(db, token)

    @staticmethod
    async def logout_all(
        db: AsyncSession,
        redis: aioredis.Redis,
        user: User,
        current_token: Optional[str] = None,
    ) -> None:
        """
        Revokes all refresh tokens and invalidates all active access tokens for user.
        """
        now = datetime.now(timezone.utc)
        # 1. Increment token_version in Postgres backstop (invalidates all access tokens instantly even during Redis outage)
        await UserRepository.increment_token_version(db, user)

        # 2. Revoke all refresh tokens in database
        await TokenRepository.revoke_all_for_user(db, user.id)

        # 3. Invalidate all active JWT access tokens via Redis min_iat timestamp
        try:
            await redis.set(
                f"user:{user.id}:min_iat",
                str(int(now.timestamp())),
                ex=settings.ACCESS_TOKEN_MINUTES * 60,
            )
            if current_token:
                payload = decode_access_token(current_token)
                jti = payload.get("jti")
                exp = payload.get("exp", 0)
                ttl = max(1, exp - int(now.timestamp()))
                await redis.set(f"denylist:{jti}", "1", ex=ttl)
        except Exception as e:
            logger.warning(f"Failed to record min_iat in Redis: {e}")

    @staticmethod
    async def change_password(
        db: AsyncSession,
        redis: aioredis.Redis,
        user: User,
        current_password: str,
        new_password: str,
        current_token: Optional[str] = None,
    ) -> None:
        """
        Validates current password, updates to new Argon2id hash, and revokes all active sessions.
        """
        if not verify_password(current_password, user.hashed_password):
            raise InvalidCredentialsError("Current password does not match.")

        validate_password_policy(new_password, user.email)
        new_hash = hash_password(new_password)
        await UserRepository.update_password(db, user, new_hash)

        # Revoke all sessions on password change
        await AuthService.logout_all(db, redis, user, current_token)
        logger.info(f"Password changed and all sessions revoked for user: {user.email}")
