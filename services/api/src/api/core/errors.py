"""
Uniform error handling and custom exceptions for ColdChainIQ API.
Format: {"error": {"code": "...", "message": "...", "request_id": "..."}}
"""

from typing import Any, Dict, Optional
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .logging import get_request_id


class APIException(Exception):
    """Base API exception supporting uniform error envelopes."""
    def __init__(
        self,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        code: str = "INTERNAL_ERROR",
        message: str = "An unexpected error occurred.",
        headers: Optional[Dict[str, str]] = None,
    ):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.headers = headers or {}
        super().__init__(message)


class InvalidCredentialsError(APIException):
    def __init__(self, message: str = "Invalid email or password."):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="INVALID_CREDENTIALS",
            message=message,
            headers={"WWW-Authenticate": "Bearer"},
        )


class AccountLockedError(APIException):
    def __init__(self, retry_after_sec: int = 900):
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="ACCOUNT_LOCKED",
            message=f"Account is locked due to too many failed login attempts. Retry after {retry_after_sec // 60} minutes.",
            headers={"Retry-After": str(retry_after_sec)},
        )


class UserInactiveError(APIException):
    def __init__(self, message: str = "Account is inactive or pending approval."):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            code="USER_INACTIVE",
            message=message,
        )


class TokenRevokedError(APIException):
    def __init__(self, message: str = "Token has been revoked."):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="TOKEN_REVOKED",
            message=message,
            headers={"WWW-Authenticate": "Bearer"},
        )


class TokenExpiredError(APIException):
    def __init__(self, message: str = "Token has expired."):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="TOKEN_EXPIRED",
            message=message,
            headers={"WWW-Authenticate": "Bearer"},
        )


class TokenInvalidError(APIException):
    def __init__(self, message: str = "Invalid or malformed token."):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="TOKEN_INVALID",
            message=message,
            headers={"WWW-Authenticate": "Bearer"},
        )


class TokenReuseError(APIException):
    def __init__(self, message: str = "Refresh token reuse detected. Token family revoked."):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code="TOKEN_THEFT_DETECTED",
            message=message,
            headers={"WWW-Authenticate": "Bearer"},
        )


class PermissionDeniedError(APIException):
    def __init__(self, message: str = "You do not have permission to perform this action."):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            code="PERMISSION_DENIED",
            message=message,
        )


class ResourceNotFoundError(APIException):
    def __init__(self, message: str = "Resource not found."):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            code="NOT_FOUND",
            message=message,
        )


class RateLimitExceededError(APIException):
    def __init__(self, retry_after: int = 60, message: str = "Rate limit exceeded. Please wait before retrying."):
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code="RATE_LIMIT_EXCEEDED",
            message=message,
            headers={"Retry-After": str(retry_after)},
        )


def build_error_response(
    code: str,
    message: str,
    status_code: int,
    request_id: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> JSONResponse:
    req_id = request_id or get_request_id()
    content = {
        "error": {
            "code": code,
            "message": message,
            "request_id": req_id,
        }
    }
    return JSONResponse(status_code=status_code, content=content, headers=headers)


def register_exception_handlers(app: FastAPI) -> None:
    """Configures centralized uniform error handlers on the FastAPI application."""

    @app.exception_handler(APIException)
    async def api_exception_handler(request: Request, exc: APIException) -> JSONResponse:
        return build_error_response(
            code=exc.code,
            message=exc.message,
            status_code=exc.status_code,
            headers=exc.headers,
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code_map = {
            401: "UNAUTHORIZED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            405: "METHOD_NOT_ALLOWED",
            429: "TOO_MANY_REQUESTS",
        }
        code = code_map.get(exc.status_code, "HTTP_ERROR")
        return build_error_response(
            code=code,
            message=str(exc.detail),
            status_code=exc.status_code,
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        first_error = exc.errors()[0] if exc.errors() else {"msg": "Validation failed"}
        loc = " -> ".join(str(l) for l in first_error.get("loc", []))
        msg = f"{loc}: {first_error.get('msg')}" if loc else first_error.get("msg")
        return build_error_response(
            code="VALIDATION_ERROR",
            message=msg,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        return build_error_response(
            code="INTERNAL_SERVER_ERROR",
            message="An unexpected server error occurred.",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
