import logging
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.config import settings

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = ["queued", "processing", "enriching"]

ACQUIRE_SLOT_LUA = """
local current = redis.call('GET', KEYS[1])
local current_num = current and tonumber(current) or 0

if current_num >= tonumber(ARGV[1]) then
    return 0
end

local new_val = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
"""

RELEASE_SLOT_LUA = """
local current = redis.call('GET', KEYS[1])
local current_num = current and tonumber(current) or 0
if current_num > 0 then
    local new_val = redis.call('DECR', KEYS[1])
    return new_val
else
    redis.call('SET', KEYS[1], 0)
    return 0
end
"""


def rate_limit_key(user_id: str) -> str:
    return f"ratelimit:active:{user_id}"


async def acquire_active_document_slot(
    redis: Optional[aioredis.Redis],
    db: AsyncIOMotorDatabase,
    user_id: str,
    max_limit: Optional[int] = None,
) -> bool:
    """
    Attempts to atomically acquire an active document slot for user_id.
    Returns True if allowed, False if rate limit (max active documents) exceeded.
    Gracefully degrades to querying MongoDB if Redis is unavailable or errors.
    """
    limit = max_limit or settings.MAX_ACTIVE_DOCUMENTS
    key = rate_limit_key(user_id)

    if redis is not None:
        try:
            result = await redis.eval(
                ACQUIRE_SLOT_LUA,
                1,
                key,
                str(limit),
                str(settings.REDIS_RATELIMIT_TTL_SECONDS),
            )
            allowed = (result == 1)
            logger.debug(
                "Redis rate limit check",
                extra={"user_id": user_id, "allowed": allowed, "limit": limit},
            )
            return allowed
        except (RedisError, ConnectionError, OSError) as e:
            logger.warning(
                "Redis rate limiter failed; falling back to MongoDB count",
                extra={"error": str(e), "user_id": user_id},
            )

    # MongoDB Fallback reconciliation / check
    return await _acquire_slot_mongo_fallback(db, user_id, limit)


async def _acquire_slot_mongo_fallback(
    db: AsyncIOMotorDatabase,
    user_id: str,
    limit: int,
) -> bool:
    """Fallback active document count directly from MongoDB."""
    active_count = await db.documents.count_documents({
        "user_id": user_id,
        "status": {"$in": ACTIVE_STATUSES},
    })
    allowed = active_count < limit
    logger.info(
        "MongoDB fallback rate limit check",
        extra={"user_id": user_id, "active_count": active_count, "allowed": allowed, "limit": limit},
    )
    return allowed


async def release_active_document_slot(
    redis: Optional[aioredis.Redis],
    db: AsyncIOMotorDatabase,
    user_id: str,
) -> None:
    """
    Decrements user's active document counter on completion or failure.
    Gracefully logs a warning on Redis errors without throwing.
    """
    if redis is not None:
        key = rate_limit_key(user_id)
        try:
            await redis.eval(RELEASE_SLOT_LUA, 1, key)
            logger.debug("Released active document slot in Redis", extra={"user_id": user_id})
            return
        except (RedisError, ConnectionError, OSError) as e:
            logger.warning(
                "Redis release slot failed; counter will auto-expire or reconcile",
                extra={"error": str(e), "user_id": user_id},
            )


async def reconcile_active_documents(
    redis: Optional[aioredis.Redis],
    db: AsyncIOMotorDatabase,
    user_id: str,
) -> int:
    """
    Authoritative reconciliation: counts active documents in MongoDB
    and updates the Redis counter.
    """
    active_count = await db.documents.count_documents({
        "user_id": user_id,
        "status": {"$in": ACTIVE_STATUSES},
    })

    if redis is not None:
        key = rate_limit_key(user_id)
        try:
            await redis.set(key, active_count, ex=settings.REDIS_RATELIMIT_TTL_SECONDS)
            logger.info(
                "Reconciled active document count into Redis",
                extra={"user_id": user_id, "active_count": active_count},
            )
        except (RedisError, ConnectionError, OSError) as e:
            logger.warning(
                "Failed to reconcile active count in Redis",
                extra={"error": str(e), "user_id": user_id},
            )

    return active_count
