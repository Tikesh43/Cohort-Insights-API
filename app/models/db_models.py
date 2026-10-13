import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from bson import ObjectId


def compute_content_hash(content: str) -> str:
    """Computes SHA-256 hex digest of the normalized content string."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def create_initial_stage(version: int = 1) -> Dict[str, Any]:
    """Builds initial stage metadata sub-document."""
    return {
        "state": "queued",
        "attempts": 0,
        "error": None,
        "started_at": None,
        "finished_at": None,
        "for_version": version,
        "lease_expires_at": None,
    }


def create_completed_stage(version: int = 1) -> Dict[str, Any]:
    """Builds pre-completed stage metadata (used for cache hits)."""
    now = datetime.now(timezone.utc)
    return {
        "state": "completed",
        "attempts": 1,
        "error": None,
        "started_at": now,
        "finished_at": now,
        "for_version": version,
        "lease_expires_at": None,
    }


def format_document_response(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforms a raw MongoDB document into a client-facing response dict.
    Enforces the core staleness invariant:
    Returns summary and tags ONLY IF summary_version == tags_version == content_version
    AND status == 'completed'. Otherwise, both are None.
    """
    content_version = doc.get("content_version", 1)
    result = doc.get("result") or {}
    status = doc.get("status", "queued")

    summary_version = result.get("summary_version")
    tags_version = result.get("tags_version")

    # Strict staleness invariant check
    is_current = (
        status == "completed"
        and summary_version == content_version
        and tags_version == content_version
    )

    summary: Optional[str] = result.get("summary") if is_current else None
    tags: Optional[List[str]] = result.get("tags") if is_current else None

    stages = doc.get("stages") or {}
    processing_stage = stages.get("processing") or create_initial_stage(content_version)
    enriching_stage = stages.get("enriching") or create_initial_stage(content_version)

    return {
        "document_id": str(doc["_id"]),
        "user_id": doc.get("user_id", ""),
        "title": doc.get("title", ""),
        "client_doc_ref": doc.get("client_doc_ref"),
        "content_version": content_version,
        "content_hash": doc.get("content_hash", ""),
        "status": status,
        "stages": {
            "processing": {
                "state": processing_stage.get("state", "queued"),
                "attempts": processing_stage.get("attempts", 0),
                "error": processing_stage.get("error"),
                "started_at": processing_stage.get("started_at"),
                "finished_at": processing_stage.get("finished_at"),
                "for_version": processing_stage.get("for_version", content_version),
            },
            "enriching": {
                "state": enriching_stage.get("state", "queued"),
                "attempts": enriching_stage.get("attempts", 0),
                "error": enriching_stage.get("error"),
                "started_at": enriching_stage.get("started_at"),
                "finished_at": enriching_stage.get("finished_at"),
                "for_version": enriching_stage.get("for_version", content_version),
            },
        },
        "is_current": is_current,
        "summary": summary,
        "tags": tags,
        "created_at": doc.get("created_at") or datetime.now(timezone.utc),
        "updated_at": doc.get("updated_at") or datetime.now(timezone.utc),
    }
