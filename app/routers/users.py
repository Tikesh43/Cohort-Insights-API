from typing import Optional
from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.dependencies import get_current_user_id, get_db
from app.models.schemas import DocumentListResponse, DocumentStatus
from app.services.documents import list_user_documents

router = APIRouter(prefix="/users", tags=["users"])


@router.get(
    "/{user_id}/documents",
    response_model=DocumentListResponse,
    responses={
        200: {"description": "Paginated list of user documents"},
        404: {"description": "User not found or caller attempting to inspect another user's documents"},
    },
)
async def get_user_documents_endpoint(
    user_id: str,
    page: int = Query(1, ge=1, description="Page number starting at 1"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page (max 100)"),
    status: Optional[DocumentStatus] = Query(None, description="Optional status filter"),
    caller_id: str = Depends(get_current_user_id),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> DocumentListResponse:
    """
    Returns paginated documents owned by user_id, ordered newest first.
    Strictly scoped to the caller (returns 404 if caller attempts to list another user's documents).
    """
    status_str = status.value if status else None
    result = await list_user_documents(db, user_id, caller_id, status_str, page, page_size)
    return DocumentListResponse(**result)
