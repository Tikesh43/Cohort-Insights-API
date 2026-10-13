import logging
from typing import Optional
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import pymongo

from app.config import settings

logger = logging.getLogger(__name__)

_client: Optional[AsyncIOMotorClient] = None


def get_motor_client() -> AsyncIOMotorClient:
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(settings.MONGODB_URL)
    return _client


def close_motor_client() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None


def get_database() -> AsyncIOMotorDatabase:
    client = get_motor_client()
    return client[settings.DATABASE_NAME]


async def init_db(db: AsyncIOMotorDatabase) -> None:
    """
    Initializes required MongoDB indexes:
    1. Compound: (user_id, status, created_at desc) for user document listing & status filters.
    2. Single: content_hash for content deduplication & fast lookups.
    3. Partial Unique: client_doc_ref on string type to enforce uniqueness only when present.
    """
    logger.info("Initializing MongoDB indexes...")
    try:
        # Compound index for queries scoped by user and status, sorted by newest
        await db.documents.create_index(
            [("user_id", pymongo.ASCENDING), ("status", pymongo.ASCENDING), ("created_at", pymongo.DESCENDING)],
            name="idx_user_status_created_desc",
        )

        # Content hash index
        await db.documents.create_index(
            [("content_hash", pymongo.ASCENDING)],
            name="idx_content_hash",
        )

        # Partial unique index for client_doc_ref (only applied when client_doc_ref is a string)
        await db.documents.create_index(
            [("client_doc_ref", pymongo.ASCENDING)],
            name="idx_unique_client_doc_ref",
            unique=True,
            partialFilterExpression={"client_doc_ref": {"$type": "string"}},
        )
        logger.info("MongoDB indexes successfully initialized.")
    except pymongo.errors.PyMongoError as e:
        logger.error("Failed to create MongoDB indexes", exc_info=True, extra={"error": str(e)})


def parse_object_id(id_str: str) -> ObjectId:
    """Validates and parses string into BSON ObjectId, raising 404 on format error."""
    try:
        return ObjectId(id_str)
    except (InvalidId, TypeError, ValueError):
        raise HTTPException(status_code=404, detail="Document not found")
