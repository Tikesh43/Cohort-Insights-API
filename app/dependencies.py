from typing import Optional
from fastapi import Header, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
import redis.asyncio as aioredis

from app.db import get_database
from app.redis_client import get_redis_client


def get_current_user_id(
    x_user_id: Optional[str] = Header(
        None,
        alias="X-User-Id",
        description="Caller identity header scoping access to owner documents",
    ),
) -> str:
    """
    Extracts caller identity from X-User-Id header.
    Rejects requests without authenticated identity.
    """
    if not x_user_id or not x_user_id.strip():
        raise HTTPException(
            status_code=400,
            detail="Missing or empty required header: X-User-Id",
        )
    return x_user_id.strip()


def get_db() -> AsyncIOMotorDatabase:
    """Dependency injection provider for MongoDB database instance."""
    return get_database()


def get_redis() -> Optional[aioredis.Redis]:
    """Dependency injection provider for Redis client instance."""
    return get_redis_client()
