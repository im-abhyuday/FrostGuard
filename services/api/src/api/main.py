"""
FastAPI application factory and middleware configuration for ColdChainIQ API.
"""

from contextlib import asynccontextmanager
import time
from typing import AsyncGenerator
import uuid

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from .core.config import settings
from .core.deps import get_redis_client
from .core.errors import register_exception_handlers
from .core.logging import get_request_id, set_request_id, setup_api_logging
from .db.session import ping_database
from .routers import admin, auth, shipments, users


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Injects enterprise HTTP security headers into every response.
    Protects against clickjacking, MIME sniffing, and enforces HTTPS when enabled.
    """
    async def dispatch(self, request: Request, call_next) -> Response:
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        if settings.HSTS_ENABLED:
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

        return response


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Ensures every request has a unique X-Request-ID header and sets logging context.
    """
    async def dispatch(self, request: Request, call_next) -> Response:
        req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        set_request_id(req_id)

        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = req_id
        return response


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application startup and shutdown hooks."""
    setup_api_logging(settings.LOG_LEVEL)
    # Pre-warm Redis connection
    try:
        redis = get_redis_client()
        await redis.ping()
    except Exception:
        pass
    yield
    # Shutdown logic
    try:
        redis = get_redis_client()
        await redis.close()
    except Exception:
        pass


def create_app() -> FastAPI:
    """FastAPI application factory."""
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # 1. Custom Security & Request ID Middlewares
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(RequestIDMiddleware)

    # 2. CORS Hardening
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID", "Retry-After"],
    )

    # 3. Register Centralized Exception Envelopes
    register_exception_handlers(app)

    # 4. Include Service Routers
    app.include_router(auth.router)
    app.include_router(users.router)
    app.include_router(admin.router)
    app.include_router(shipments.router)

    # 5. Public Health Probe
    @app.get(
        "/health",
        tags=["System"],
        summary="Service readiness and liveness probe",
    )
    async def health_check():
        db_alive = await ping_database()
        redis_alive = False
        try:
            redis = get_redis_client()
            redis_alive = bool(await redis.ping())
        except Exception:
            pass

        is_healthy = db_alive and redis_alive
        status_code = status.HTTP_200_OK if is_healthy else status.HTTP_503_SERVICE_UNAVAILABLE

        return Response(
            content=f'{{"status":"{"UP" if is_healthy else "DOWN"}","checks":{{"database":{str(db_alive).lower()},"redis":{str(redis_alive).lower()}}}}}',
            media_type="application/json",
            status_code=status_code,
        )

    return app


app = create_app()
