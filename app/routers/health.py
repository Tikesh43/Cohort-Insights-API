import logging
from fastapi import APIRouter, Depends, Response, status
from motor.motor_asyncio import AsyncIOMotorDatabase
import redis.asyncio as aioredis

from app.dependencies import get_db, get_redis
from app.models.schemas import HealthResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    responses={
        200: {"description": "System healthy or degraded (Redis unavailable)"},
        503: {"description": "System unhealthy (MongoDB unavailable)"},
    },
)
async def health_check_endpoint(
    response: Response,
    db: AsyncIOMotorDatabase = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> HealthResponse:
    """
    Health check probing MongoDB and Redis connections.
    Returns 200 'healthy' if both are operational.
    Returns 200 'degraded' if Redis is down (service gracefully degrades with Mongo fallback).
    Returns 503 'unhealthy' if MongoDB is unreachable.
    """
    mongo_status = "disconnected"
    redis_status = "disconnected"

    # Probe MongoDB
    try:
        await db.command("ping")
        mongo_status = "connected"
    except Exception as e:
        logger.error("MongoDB health ping failed", extra={"error": str(e)})

    # Probe Redis
    if redis is not None:
        try:
            pong = await redis.ping()
            if pong:
                redis_status = "connected"
        except Exception as e:
            logger.warning("Redis health ping failed", extra={"error": str(e)})

    # Evaluate composite system health
    if mongo_status == "connected" and redis_status == "connected":
        system_status = "healthy"
        response.status_code = status.HTTP_200_OK
    elif mongo_status == "connected":
        system_status = "degraded"
        response.status_code = status.HTTP_200_OK
    else:
        system_status = "unhealthy"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status=system_status,
        mongo=mongo_status,
        redis=redis_status,
    )
