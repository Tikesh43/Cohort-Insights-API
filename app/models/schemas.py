from datetime import datetime
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict


class StageState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class DocumentStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


class DocumentCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=255, description="Title of the document")
    content: str = Field(..., min_length=1, max_length=500_000, description="Raw content to be processed")
    user_id: Optional[str] = Field(None, description="Optional user_id; if omitted or provided, must match caller")
    client_doc_ref: Optional[str] = Field(None, max_length=128, description="Optional client-provided idempotency reference")

    model_config = ConfigDict(extra="forbid")


class DocumentUpdate(BaseModel):
    content: str = Field(..., min_length=1, max_length=500_000, description="New raw content replacing existing content")

    model_config = ConfigDict(extra="forbid")


class StageStatus(BaseModel):
    state: StageState
    attempts: int = 0
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    for_version: int

    model_config = ConfigDict(from_attributes=True)


class StagesResponse(BaseModel):
    processing: StageStatus
    enriching: StageStatus

    model_config = ConfigDict(from_attributes=True)


class DocumentResponse(BaseModel):
    document_id: str
    user_id: str
    title: str
    client_doc_ref: Optional[str] = None
    content_version: int
    content_hash: str
    status: DocumentStatus
    stages: StagesResponse
    is_current: bool
    summary: Optional[str] = None
    tags: Optional[List[str]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class DocumentCreateResponse(BaseModel):
    document_id: str
    status: DocumentStatus

    model_config = ConfigDict(from_attributes=True)


class DocumentListResponse(BaseModel):
    items: List[DocumentResponse]
    total: int
    page: int
    page_size: int
    total_pages: int

    model_config = ConfigDict(from_attributes=True)


class HealthResponse(BaseModel):
    status: str
    mongo: str
    redis: str

    model_config = ConfigDict(from_attributes=True)
