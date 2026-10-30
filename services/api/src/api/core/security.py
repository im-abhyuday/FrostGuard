"""
Cryptographic security module for ColdChainIQ API.
Handles Argon2id password hashing, transparent rehashing, password policies,
opaque refresh token generation, and HS256 JWT encoding/decoding.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import secrets
from typing import Any, Dict, Optional, Tuple
import uuid
import argon2
from argon2 import Type
import jwt

from .config import settings
from .errors import TokenExpiredError, TokenInvalidError

# Enterprise Argon2id Password Hasher (OWASP recommended parameters)
# Memory cost: 64MB (65536 KB), Time cost: 2 iterations, Parallelism: 2 threads
password_hasher = argon2.PasswordHasher(
    time_cost=2,
    memory_cost=65536,
    parallelism=2,
    hash_len=32,
    salt_len=16,
    type=Type.ID,
)

# Pre-computed dummy hash to mitigate user enumeration via timing attacks
DUMMY_ARGON2_HASH = password_hasher.hash("coldchainiq-dummy-salt-password")

COMMON_PASSWORDS = {
    "password123",
    "password1234",
    "1234567890",
    "qwerty1234",
    "admin12345",
    "welcome123",
    "letmein123",
    "coldchain123",
    "frostguard123",
    "iloveyou123",
}


def hash_password(password: str) -> str:
    """Computes Argon2id hash for plaintext password."""
    return password_hasher.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies plaintext against Argon2id hash."""
    try:
        return password_hasher.verify(hashed_password, plain_password)
    except (argon2.exceptions.VerifyMismatchError, argon2.exceptions.VerificationError, argon2.exceptions.InvalidHashError):
        return False


def verify_dummy_password(plain_password: str) -> None:
    """
    Executes Argon2id verification against dummy hash.
    Equalizes execution timing when user account does not exist.
    """
    try:
        password_hasher.verify(DUMMY_ARGON2_HASH, plain_password)
    except Exception:
        pass


def password_needs_rehash(hashed_password: str) -> bool:
    """Checks if password hash was computed with outdated parameters."""
    return password_hasher.check_needs_rehash(hashed_password)


def validate_password_policy(password: str, email: Optional[str] = None) -> None:
    """
    Enforces ColdChainIQ enterprise password strength policy:
    1. Minimum 10 characters
    2. Must not match email (case-insensitive)
    3. Rejects known very common passwords
    """
    if len(password) < 10:
        raise ValueError("Password must be at least 10 characters long.")

    if email and password.lower().strip() == email.lower().strip():
        raise ValueError("Password cannot be identical to the email address.")

    if password.lower() in COMMON_PASSWORDS:
        raise ValueError("Password is too common and easily guessed. Please choose a stronger password.")


def hash_token(raw_token: str) -> str:
    """Computes SHA-256 hex digest for opaque refresh token."""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def generate_refresh_token() -> Tuple[str, str]:
    """
    Generates high-entropy opaque refresh token and its SHA-256 digest.
    Returns: (raw_token, token_hash)
    """
    raw_token = secrets.token_urlsafe(48)
    token_digest = hash_token(raw_token)
    return raw_token, token_digest


def create_access_token(
    user_id: uuid.UUID,
    role: str,
    expires_delta: Optional[timedelta] = None,
    token_version: int = 1,
) -> Tuple[str, str, int]:
    """
    Constructs HS256 JWT access token with standard RFC-7519, role, and tv (token_version) claims.
    Returns: (access_token_jwt, jti, expires_in_seconds)
    """
    now = datetime.now(timezone.utc)
    delta = expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_MINUTES)
    expires_at = now + delta
    jti = str(uuid.uuid4())

    claims: Dict[str, Any] = {
        "sub": str(user_id),
        "role": role,
        "jti": jti,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
        "type": "access",
        "iss": settings.JWT_ISSUER,
        "aud": settings.JWT_AUDIENCE,
        "tv": token_version,
    }

    token = jwt.encode(
        payload=claims,
        key=settings.JWT_SECRET,
        algorithm=settings.JWT_ALGORITHM,
    )
    expires_in_sec = int(delta.total_seconds())
    return token, jti, expires_in_sec


def decode_access_token(token: str) -> Dict[str, Any]:
    """
    Validates signature, expiration, issuer, audience, and type claim.
    Explicitly rejects alg=none and non-access tokens.
    """
    try:
        payload = jwt.decode(
            jwt=token,
            key=settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
            issuer=settings.JWT_ISSUER,
            audience=settings.JWT_AUDIENCE,
            options={
                "verify_signature": True,
                "verify_exp": True,
                "verify_iss": True,
                "verify_aud": True,
                "require": ["sub", "role", "jti", "exp", "iat", "type", "iss", "aud"],
            },
        )
    except jwt.ExpiredSignatureError:
        raise TokenExpiredError("Access token has expired.")
    except (jwt.InvalidSignatureError, jwt.InvalidIssuerError, jwt.InvalidAudienceError, jwt.DecodeError) as e:
        raise TokenInvalidError(f"Access token validation failed: {str(e)}")
    except Exception as e:
        raise TokenInvalidError(f"Malformed access token: {str(e)}")

    if payload.get("type") != "access":
        raise TokenInvalidError("Invalid token type. Expected access token.")

    return payload
