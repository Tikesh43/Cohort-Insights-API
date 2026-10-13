from fastapi import APIRouter, Depends, Response, status
from motor.motor_asyncio import AsyncIOMotorDatabase
import redis.asyncio as aioredis

from app.dependencies import get_current_user_id, get_db, get_redis
from app.models.schemas import (
    DocumentCreate,
    DocumentCreateResponse,
    DocumentResponse,
    DocumentUpdate,
)
from app.services.documents import (
    create_document,
    get_document_by_id,
    get_document_by_ref,
    patch_document,
)

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post(
    "",
    response_model=DocumentCreateResponse,
    responses={
        200: {"description": "Idempotent retry with identical client_doc_ref and content hash"},
        201: {"description": "Document created and enqueued (or immediately completed via cache)"},
        409: {"description": "Conflict: duplicate client_doc_ref with different content or different user"},
        429: {"description": "Rate limit exceeded: user has 3 active documents in {queued, processing, enriching}"},
    },
)
async def create_document_endpoint(
    payload: DocumentCreate,
    response: Response,
    caller_id: str = Depends(get_current_user_id),
    db: AsyncIOMotorDatabase = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> DocumentCreateResponse:
    """
    Submits a document for cohort insight processing.
    Validates payload, enforces client_doc_ref policies, checks content cache,
    and applies concurrency rate limits.
    """
    doc_id, doc_status, http_status = await create_document(db, redis, payload, caller_id)
    response.status_code = http_status
    return DocumentCreateResponse(document_id=doc_id, status=doc_status)


@router.patch(
    "/{document_id}",
    response_model=DocumentResponse,
    responses={
        200: {"description": "Document updated in place, stages reset to queued, recomputation started"},
        404: {"description": "Document not found or caller is not owner"},
        409: {"description": "Optimistic concurrency conflict on concurrent update"},
    },
)
async def patch_document_endpoint(
    document_id: str,
    payload: DocumentUpdate,
    caller_id: str = Depends(get_current_user_id),
    db: AsyncIOMotorDatabase = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> DocumentResponse:
    """
    Replaces content in place, increments content_version, recomputes content_hash,
    and atomically resets stages and derived fields.
    """
    updated_doc = await patch_document(db, redis, document_id, payload, caller_id)
    return DocumentResponse(**updated_doc)


@router.get(
    "/by-ref/{client_doc_ref}",
    response_model=DocumentResponse,
    responses={
        200: {"description": "Document retrieved via client_doc_ref"},
        404: {"description": "Document not found or caller is not owner"},
    },
)
async def get_document_by_ref_endpoint(
    client_doc_ref: str,
    caller_id: str = Depends(get_current_user_id),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> DocumentResponse:
    """
    Resolves a document by client_doc_ref via unique index.
    Restricted strictly to the caller's documents (404 for others).
    """
    doc = await get_document_by_ref(db, client_doc_ref, caller_id)
    return DocumentResponse(**doc)


@router.get(
    "/{document_id}",
    response_model=DocumentResponse,
    responses={
        200: {"description": "Document status and stage results"},
        404: {"description": "Document not found or caller is not owner"},
    },
)
async def get_document_endpoint(
    document_id: str,
    caller_id: str = Depends(get_current_user_id),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> DocumentResponse:
    """
    Retrieves document by ID.
    Enforces staleness invariant: summary and tags are returned only if
    they strictly match the current content_version.
    """
    doc = await get_document_by_id(db, document_id, caller_id)
    return DocumentResponse(**doc)
