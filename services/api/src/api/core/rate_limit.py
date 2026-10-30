"""
Redis-based sliding window rate limiter for ColdChainIQ API.
Implements token bucket / sliding log rate limiting with fail-open semantics.
"""

import logging
import time
from typing import Optional, Tuple
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from .errors import RateLimitExceededError

logger = logging.getLogger("coldchainiq.api.ratelimit")


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter using Redis sorted sets (ZSET).
    Calculates exact request timestamps within the rolling window.
    """

    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self.redis = redis_client

    async def check_rate_limit(
        self,
        key: str,
        max_requests: int,
        window_seconds: int,
    ) -> Tuple[bool, int]:
        """
        Evaluates sliding window rate limit for given key.
        Returns: (is_allowed: bool, retry_after_seconds: int)
        Fails OPEN if Redis is unreachable to avoid accidental total denial of service.
        """
        if self.redis is None:
            return True, 0

        now = time.time()
        window_start = now - window_seconds
        redis_key = f"ratelimit:{key}"

        try:
            pipe = self.redis.pipeline(transaction=True)
            # 1. Remove expired timestamps outside the rolling window
            pipe.zremrangebyscore(redis_key, 0, window_start)
            # 2. Count remaining requests in the active window
            pipe.zcard(redis_key)
            # 3. Fetch oldest timestamp in current window for exact Retry-After calculation
            pipe.zrange(redis_key, 0, 0, withscores=True)
            results = await pipe.execute()

            request_count = results[1]
            oldest_entry = results[2]

            if request_count >= max_requests:
                # Exceeded rate limit
                if oldest_entry:
                    oldest_ts = oldest_entry[0][1]
                    retry_after = max(1, int(window_seconds - (now - oldest_ts)))
                else:
                    retry_after = window_seconds

                logger.warning(
                    f"Rate limit exceeded for key '{key}': {request_count}/{max_requests} in {window_seconds}s. "
                    f"Retry-After: {retry_after}s"
                )
                return False, retry_after

            # Within limits: record current request timestamp
            add_pipe = self.redis.pipeline(transaction=True)
            add_pipe.zadd(redis_key, {str(now): now})
            add_pipe.expire(redis_key, window_seconds + 5)
            await add_pipe.execute()

            return True, 0

        except (RedisError, ConnectionError, TimeoutError, OSError) as e:
            # Tradeoff: Fail OPEN so Redis network blips don't lock out legitimate users.
            # Security backstop: Postgres account lockout (failed_login_count) protects accounts.
            logger.error(
                f"Redis failure during rate limit check for '{key}': {e}. "
                "Failing OPEN per high-availability policy."
            )
            return True, 0

    async def enforce(
        self,
        key: str,
        max_requests: int,
        window_seconds: int,
        error_message: str = "Rate limit exceeded. Please wait before retrying.",
    ) -> None:
        """Helper that directly raises RateLimitExceededError if limit is breached."""
        allowed, retry_after = await self.check_rate_limit(key, max_requests, window_seconds)
        if not allowed:
            raise RateLimitExceededError(retry_after=retry_after, message=error_message)
