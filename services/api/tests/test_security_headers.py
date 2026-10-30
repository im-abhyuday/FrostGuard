"""
Security headers, CORS policy, Request-ID tracing, and log redaction test suite.
"""

import json
import logging
import pytest
from httpx import AsyncClient

from api.core.logging import RedactionJsonFormatter


@pytest.mark.asyncio
async def test_security_headers_present_on_all_responses(client: AsyncClient):
    """Verifies that mandatory security headers are injected into HTTP responses."""
    res = await client.get("/health")
    assert res.headers["X-Content-Type-Options"] == "nosniff"
    assert res.headers["X-Frame-Options"] == "DENY"
    assert res.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"


@pytest.mark.asyncio
async def test_request_id_middleware(client: AsyncClient):
    """Verifies that every request generates or propagates an X-Request-ID header."""
    # Automatic generation
    res1 = await client.get("/health")
    assert "X-Request-ID" in res1.headers
    generated_id = res1.headers["X-Request-ID"]
    assert len(generated_id) > 10

    # Propagation of client-supplied request ID
    custom_id = "trace-client-12345678"
    res2 = await client.get("/health", headers={"X-Request-ID": custom_id})
    assert res2.headers["X-Request-ID"] == custom_id


@pytest.mark.asyncio
async def test_cors_configuration(client: AsyncClient):
    """Verifies that CORS headers respect explicit origins and disallow wildcards with credentials."""
    # Origin in allowlist
    res_allowed = await client.options(
        "/auth/login",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert res_allowed.headers.get("access-control-allow-origin") == "http://localhost:3000"
    assert res_allowed.headers.get("access-control-allow-credentials") == "true"

    # Origin NOT in allowlist
    res_disallowed = await client.options(
        "/auth/login",
        headers={
            "Origin": "http://evil-unauthorized-site.com",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert res_disallowed.headers.get("access-control-allow-origin") != "*"
    assert res_disallowed.headers.get("access-control-allow-origin") != "http://evil-unauthorized-site.com"


def test_log_redaction_sanitizes_passwords_and_tokens():
    """
    Verifies that RedactionJsonFormatter strips sensitive password fields,
    refresh tokens, and Authorization Bearer headers before emission.
    """
    formatter = RedactionJsonFormatter()

    record = logging.LogRecord(
        name="test_redaction",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Authentication event",
        args=(),
        exc_info=None,
    )
    # Inject sensitive fields into extra dictionary
    record.__dict__["password"] = "TopSecretPassword2026!"
    record.__dict__["refresh_token"] = "sensitive_opaque_refresh_token_string"
    record.__dict__["auth_header"] = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.secretpayload.signature"

    formatted_str = formatter.format(record)
    parsed = json.loads(formatted_str)

    # Assert sensitive values are fully redacted
    assert parsed["password"] == "[REDACTED]"
    assert parsed["refresh_token"] == "[REDACTED]"
    assert parsed["auth_header"] == "Bearer [REDACTED]"
    assert "TopSecretPassword2026!" not in formatted_str
    assert "sensitive_opaque_refresh_token_string" not in formatted_str
