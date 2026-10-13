from datetime import datetime, timezone
import math
from typing import Any, Dict, Optional, Tuple
from bson import ObjectId
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
import pymongo
import redis.asyncio as aioredis

from app.db import parse_object_id
from app.models.db_models import (
    compute_content_hash,
    create_completed_stage,
    create_initial_stage,
    format_document_response,
)
from app.models.schemas import DocumentCreate, DocumentUpdate
from app.services.cache import get_content_cache
from app.services.rate_limiter import (
    ACTIVE_STATUSES,
    acquire_active_document_slot,
    release_active_document_slot,
)


async def create_document(
    db: AsyncIOMotorDatabase,
    redis: Optional[aioredis.Redis],
    payload: DocumentCreate,
    caller_id: str,
) -> Tuple[str, str, int]:
    """
    Handles document creation adhering to:
    1. Caller scoping (user_id if provided must match X-User-Id).
    2. Repeat client_doc_ref policy:
       - Same user, same ref, same hash: idempotent 200 OK.
       - Same user, same ref, different hash: 409 Conflict with existing document_id.
       - Different user, same ref: 409 Conflict without leaking owner details.
    3. Content cache:
       - If content_hash already processed in cache, immediately completes document,
         bypasses pipeline, and does NOT consume a rate limit slot (returns 201).
    4. Rate limiting:
       - Rejects with 429 if caller already has 3 active documents in {queued, processing, enriching}.
    5. Persistence & Enqueueing:
       - Stores document in 'queued' state with version 1 and initializes stage tracking.
    """
    if payload.user_id and payload.user_id != caller_id:
        raise HTTPException(
            status_code=400,
            detail="Payload user_id must match authenticated caller (X-User-Id)",
        )

    content_hash = compute_content_hash(payload.content)
    now = datetime.now(timezone.utc)

    # Repeat client_doc_ref policy evaluation
    if payload.client_doc_ref:
        existing = await db.documents.find_one({"client_doc_ref": payload.client_doc_ref})
        if existing:
            if existing.get("user_id") == caller_id:
                if existing.get("content_hash") == content_hash:
                    # Idempotent retry: return existing document with 200 OK
                    return str(existing["_id"]), existing.get("status", "queued"), 200
                else:
                    # Same ref, different content: conflict, instruct client to PATCH
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "detail": "Document with client_doc_ref already exists with different content. Use PATCH to update.",
                            "document_id": str(existing["_id"]),
                        },
                    )
            else:
                # Same ref, different user: conflict without leaking owner info
                raise HTTPException(
                    status_code=409,
                    detail="client_doc_ref is already in use",
                )

    # Content Cache Fast Path: Check if this content was already enriched
    cached_result = await get_content_cache(redis, content_hash)
    if cached_result is not None:
        # Fast-complete document immediately without hitting pipeline or rate limiter
        doc_record = {
            "user_id": caller_id,
            "title": payload.title,
            "content": payload.content,
            "client_doc_ref": payload.client_doc_ref,
            "content_hash": content_hash,
            "content_version": 1,
            "status": "completed",
            "stages": {
                "processing": create_completed_stage(1),
                "enriching": create_completed_stage(1),
            },
            "result": {
                "summary": cached_result.get("summary"),
                "summary_version": 1,
                "tags": cached_result.get("tags"),
                "tags_version": 1,
                "tags_source_summary_version": 1,
            },
            "created_at": now,
            "updated_at": now,
        }
        try:
            res = await db.documents.insert_one(doc_record)
            return str(res.inserted_id), "completed", 201
        except pymongo.errors.DuplicateKeyError:
            if payload.client_doc_ref:
                existing = await db.documents.find_one({"client_doc_ref": payload.client_doc_ref})
                if existing:
                    if existing.get("user_id") == caller_id:
                        if existing.get("content_hash") == content_hash:
                            return str(existing["_id"]), existing.get("status", "completed"), 200
                        else:
                            raise HTTPException(
                                status_code=409,
                                detail={
                                    "detail": "Document with client_doc_ref already exists with different content. Use PATCH to update.",
                                    "document_id": str(existing["_id"]),
                                },
                            )
                    else:
                        raise HTTPException(status_code=409, detail="client_doc_ref is already in use")
            raise HTTPException(status_code=409, detail="Concurrent creation conflict")

    # Active Documents Rate Limit Check
    allowed = await acquire_active_document_slot(redis, db, caller_id)
    if not allowed:
        if payload.client_doc_ref:
            existing = await db.documents.find_one({"client_doc_ref": payload.client_doc_ref})
            if existing and existing.get("user_id") == caller_id and existing.get("content_hash") == content_hash:
                return str(existing["_id"]), existing.get("status", "queued"), 200
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded: user has 3 active documents in {queued, processing, enriching}",
        )

    # Standard Insertion: Enqueue document for background pipeline
    doc_record = {
        "user_id": caller_id,
        "title": payload.title,
        "content": payload.content,
        "client_doc_ref": payload.client_doc_ref,
        "content_hash": content_hash,
        "content_version": 1,
        "status": "queued",
        "stages": {
            "processing": create_initial_stage(1),
            "enriching": create_initial_stage(1),
        },
        "result": {
            "summary": None,
            "summary_version": None,
            "tags": None,
            "tags_version": None,
            "tags_source_summary_version": None,
        },
        "created_at": now,
        "updated_at": now,
    }
    try:
        res = await db.documents.insert_one(doc_record)
        return str(res.inserted_id), "queued", 201
    except pymongo.errors.DuplicateKeyError:
        await release_active_document_slot(redis, db, caller_id)
        if payload.client_doc_ref:
            existing = await db.documents.find_one({"client_doc_ref": payload.client_doc_ref})
            if existing:
                if existing.get("user_id") == caller_id:
                    if existing.get("content_hash") == content_hash:
                        return str(existing["_id"]), existing.get("status", "queued"), 200
                    else:
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "detail": "Document with client_doc_ref already exists with different content. Use PATCH to update.",
                                "document_id": str(existing["_id"]),
                            },
                        )
                else:
                    raise HTTPException(status_code=409, detail="client_doc_ref is already in use")
        raise HTTPException(status_code=409, detail="Concurrent creation conflict")


async def patch_document(
    db: AsyncIOMotorDatabase,
    redis: Optional[aioredis.Redis],
    document_id: str,
    payload: DocumentUpdate,
    caller_id: str,
) -> Dict[str, Any]:
    """
    Atomically updates document content in place:
    - Verifies ownership (404 for non-owners).
    - Increments content_version and recomputes content_hash.
    - Atomically resets both stages to queued and clears derived result fields.
    - Employs optimistic concurrency control against concurrent PATCH calls.
    - Re-engages rate limiter slot if transitioning from non-active to active.
    """
    oid = parse_object_id(document_id)
    existing = await db.documents.find_one({"_id": oid})
    if not existing or existing.get("user_id") != caller_id:
        raise HTTPException(status_code=404, detail="Document not found")

    old_version = existing.get("content_version", 1)
    new_version = old_version + 1
    new_hash = compute_content_hash(payload.content)
    now = datetime.now(timezone.utc)

    # Optimistic concurrency filter: matches current version
    cas_filter = {
        "_id": oid,
        "user_id": caller_id,
        "content_version": old_version,
    }
    cas_update = {
        "$set": {
            "content": payload.content,
            "content_hash": new_hash,
            "content_version": new_version,
            "status": "queued",
            "stages.processing": create_initial_stage(new_version),
            "stages.enriching": create_initial_stage(new_version),
            "result": {
                "summary": None,
                "summary_version": None,
                "tags": None,
                "tags_version": None,
                "tags_source_summary_version": None,
            },
            "updated_at": now,
        }
    }

    updated = await db.documents.find_one_and_update(
        cas_filter,
        cas_update,
        return_document=pymongo.ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(
            status_code=409,
            detail="Concurrent modification conflict: document content version changed. Please retry.",
        )

    # If the document was previously completed or failed, it now occupies an active slot
    if existing.get("status") not in ACTIVE_STATUSES:
        await acquire_active_document_slot(redis, db, caller_id)

    # Fast-path check: If new content hash is already cached, complete immediately
    cached_result = await get_content_cache(redis, new_hash)
    if cached_result is not None:
        await db.documents.update_one(
            {"_id": oid, "content_version": new_version},
            {
                "$set": {
                    "status": "completed",
                    "stages.processing": create_completed_stage(new_version),
                    "stages.enriching": create_completed_stage(new_version),
                    "result.summary": cached_result.get("summary"),
                    "result.summary_version": new_version,
                    "result.tags": cached_result.get("tags"),
                    "result.tags_version": new_version,
                    "result.tags_source_summary_version": new_version,
                    "updated_at": now,
                }
            },
        )
        updated = await db.documents.find_one({"_id": oid})
        await release_active_document_slot(redis, db, caller_id)

    return format_document_response(updated)


async def get_document_by_id(
    db: AsyncIOMotorDatabase,
    document_id: str,
    caller_id: str,
) -> Dict[str, Any]:
    """Retrieves document by ID, scoped to caller. Returns 404 for non-owners."""
    oid = parse_object_id(document_id)
    doc = await db.documents.find_one({"_id": oid})
    if not doc or doc.get("user_id") != caller_id:
        raise HTTPException(status_code=404, detail="Document not found")
    return format_document_response(doc)


async def get_document_by_ref(
    db: AsyncIOMotorDatabase,
    client_doc_ref: str,
    caller_id: str,
) -> Dict[str, Any]:
    """Retrieves document by client_doc_ref, scoped to caller. Returns 404 for non-owners."""
    doc = await db.documents.find_one({"client_doc_ref": client_doc_ref})
    if not doc or doc.get("user_id") != caller_id:
        raise HTTPException(status_code=404, detail="Document not found")
    return format_document_response(doc)


async def list_user_documents(
    db: AsyncIOMotorDatabase,
    user_id: str,
    caller_id: str,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
) -> Dict[str, Any]:
    """
    Lists documents for user_id with pagination and optional status filter.
    Returns 404 if caller attempts to list another user's documents.
    """
    if caller_id != user_id:
        raise HTTPException(status_code=404, detail="User not found")

    query_filter: Dict[str, Any] = {"user_id": caller_id}
    if status:
        query_filter["status"] = status

    total = await db.documents.count_documents(query_filter)
    total_pages = math.ceil(total / page_size) if total > 0 else 0
    skip = (page - 1) * page_size

    cursor = (
        db.documents.find(query_filter)
        .sort([("created_at", pymongo.DESCENDING)])
        .skip(skip)
        .limit(page_size)
    )
    raw_docs = await cursor.to_list(length=page_size)
    items = [format_document_response(d) for d in raw_docs]

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }
