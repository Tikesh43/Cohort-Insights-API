import logging
from typing import Optional
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.config import settings

logger = logging.getLogger(__name__)

_redis_pool: Optional[aioredis.Redis] = None


def get_redis_client() -> Optional[aioredis.Redis]:
    """Returns the shared Redis client instance."""
    global _redis_pool
    if _redis_pool is None:
        try:
            _redis_pool = aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=2.0,
                socket_timeout=2.0,
            )
        except Exception as e:
            logger.warning(
                "Failed to initialize Redis connection pool; degrading gracefully",
                extra={"error": str(e)},
            )
            return None
    return _redis_pool


async def close_redis() -> None:
    """Closes Redis client connections gracefully."""
    global _redis_pool
    if _redis_pool is not None:
        try:
            await _redis_pool.aclose()
        except Exception as e:
            logger.warning("Error closing Redis connection", extra={"error": str(e)})
        finally:
            _redis_pool = None


async def is_redis_available() -> bool:
    """Checks whether Redis is responsive."""
    client = get_redis_client()
    if client is None:
        return False
    try:
        res = await client.ping()
        return bool(res)
    except (RedisError, ConnectionError, OSError) as e:
        logger.warning("Redis ping failed", extra={"error": str(e)})
        return False
