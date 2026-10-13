import json
import logging
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.config import settings

logger = logging.getLogger(__name__)


def cache_key(content_hash: str) -> str:
    return f"cache:{content_hash}"


async def get_content_cache(
    redis: Optional[aioredis.Redis],
    content_hash: str,
) -> Optional[Dict[str, Any]]:
    """
    Retrieves cached stage results (summary and tags) for a content hash.
    Gracefully returns None if Redis is unavailable or on failure.
    """
    if redis is None:
        return None

    try:
        data = await redis.get(cache_key(content_hash))
        if data:
            logger.info("Content cache HIT", extra={"content_hash": content_hash})
            return json.loads(data)
        logger.debug("Content cache MISS", extra={"content_hash": content_hash})
        return None
    except (RedisError, ConnectionError, OSError, json.JSONDecodeError) as e:
        logger.warning(
            "Content cache read failed; gracefully degrading to pipeline processing",
            extra={"error": str(e), "content_hash": content_hash},
        )
        return None


async def set_content_cache(
    redis: Optional[aioredis.Redis],
    content_hash: str,
    summary: str,
    tags: List[str],
    ttl: Optional[int] = None,
) -> None:
    """
    Caches the completed summary and tags by content_hash with TTL.
    Gracefully logs a warning on failure without throwing.
    """
    if redis is None:
        return

    ttl = ttl or settings.REDIS_CACHE_TTL_SECONDS
    key = cache_key(content_hash)
    payload = json.dumps({"summary": summary, "tags": tags})

    try:
        await redis.set(key, payload, ex=ttl)
        logger.info(
            "Content cache entry stored",
            extra={"content_hash": content_hash, "ttl": ttl},
        )
    except (RedisError, ConnectionError, OSError) as e:
        logger.warning(
            "Content cache write failed; skipping cache update",
            extra={"error": str(e), "content_hash": content_hash},
        )
