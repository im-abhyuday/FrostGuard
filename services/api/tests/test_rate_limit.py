"""
Rate limiting test suite: Redis sliding window, 429 Retry-After, and fail-open resilience.
"""

import logging
import pytest
from httpx import AsyncClient



@pytest.mark.asyncio
async def test_login_rate_limiting_triggers_429(client: AsyncClient, seed_data: dict):
    """
    Verifies that the 6th login attempt within 1 minute is throttled with 429 Too Many Requests
    and provides a Retry-After header.
    """
    email = "ratelimit_target@test.com"
    # Make 5 attempts (allowed)
    for _ in range(5):
        r = await client.post("/auth/login", data={"username": email, "password": "DummyPassword123!"})
        assert r.status_code == 401  # Bad credentials, but not rate limited yet

    # 6th attempt should be blocked by rate limiter
    r6 = await client.post("/auth/login", data={"username": email, "password": "DummyPassword123!"})
    assert r6.status_code == 429
    assert r6.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"
    assert "Retry-After" in r6.headers
    assert int(r6.headers["Retry-After"]) > 0


@pytest.mark.asyncio
async def test_rate_limiter_fails_open_when_redis_is_down(
    client: AsyncClient,
    fake_redis,
    seed_data: dict,
    caplog,
):
    """
    High-Availability Policy:
    When Redis is disconnected, the rate limiter logs an error and FAILS OPEN,
    allowing legitimate requests to proceed while database lockout acts as backstop.
    """
    # Simulate Redis connection failure
    fake_redis.is_connected = False

    with caplog.at_level(logging.ERROR):
        # Attempt login while Redis is unreachable
        res = await client.post("/auth/login", data={
            "username": seed_data["driver1"].email,
            "password": seed_data["password"],
        })
        # Should succeed (200) instead of crashing with 500
        assert res.status_code == 200
        assert "access_token" in res.json()

        # Check that error was logged
        assert any("Failing OPEN" in record.message for record in caplog.records)
