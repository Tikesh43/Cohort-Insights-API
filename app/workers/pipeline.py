import asyncio
from datetime import datetime, timedelta, timezone
import logging
import random
import re
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
import pymongo
import redis.asyncio as aioredis

from app.config import settings
from app.services.cache import set_content_cache
from app.services.rate_limiter import release_active_document_slot

logger = logging.getLogger(__name__)


def generate_mock_summary(title: str, content: str) -> str:
    """Generates deterministic mock executive summary based on content."""
    clean_text = re.sub(r"\s+", " ", content).strip()
    words = clean_text.split()
    preview = " ".join(words[:15]) + ("..." if len(words) > 15 else "")
    return f"Executive Summary for '{title}': Key cohort pattern observed. Text overview: '{preview}'"


def generate_mock_tags(summary: Optional[str], content: Optional[str]) -> List[str]:
    """Generates mock keyword tags extracted from summary and content."""
    base_pool = [
        "cohort-retention", "activation-rate", "churn-risk", "high-ltv",
        "enterprise-tier", "onboarding-funnel", "product-usage", "engagement-spike"
    ]
    s = summary or ""
    c = content or ""
    char_seed = sum(ord(ch) for ch in (c[:20] + s[:20]))
    count = 3 + (char_seed % 3)
    selected_tags = []
    for i in range(count):
        idx = (char_seed + i * 7) % len(base_pool)
        tag = base_pool[idx]
        if tag not in selected_tags:
            selected_tags.append(tag)
    return selected_tags


class PipelineWorker:
    """
    Background worker implementing atomic stage leasing, compare-and-set writes,
    stale work cancellation, and multi-stage lifecycle processing.
    """

    def __init__(self, db: AsyncIOMotorDatabase, redis: Optional[aioredis.Redis] = None):
        self.db = db
        self.redis = redis

    async def claim_next_job(self) -> Optional[Tuple[Dict[str, Any], str]]:
        """
        Atomically claims a document for processing or enriching.
        Uses find_one_and_update to claim either stage 2 (enriching) or stage 1 (processing).
        Returns (claimed_doc, stage_name) or None if no work available.
        """
        now = datetime.now(timezone.utc)
        lease_delta = timedelta(seconds=settings.LEASE_TIMEOUT_SECONDS)
        lease_expires = now + lease_delta

        # 1. Attempt to claim Stage 2: ENRICHING first
        # Prioritizing enriching finalizes in-flight jobs faster and releases rate-limit slots.
        filter_stage2 = {
            "status": {"$in": ["processing", "enriching"]},
            "stages.processing.state": "completed",
            "$expr": {
                "$and": [
                    {"$eq": ["$content_version", "$stages.processing.for_version"]},
                    {"$eq": ["$content_version", "$stages.enriching.for_version"]},
                ]
            },
            "$or": [
                {"stages.enriching.state": "queued"},
                {
                    "stages.enriching.state": "running",
                    "stages.enriching.lease_expires_at": {"$lt": now},
                },
            ],
        }
        update_stage2 = {
            "$set": {
                "status": "enriching",
                "stages.enriching.state": "running",
                "stages.enriching.started_at": now,
                "stages.enriching.lease_expires_at": lease_expires,
                "updated_at": now,
            },
            "$inc": {"stages.enriching.attempts": 1},
        }

        doc = await self.db.documents.find_one_and_update(
            filter_stage2,
            update_stage2,
            return_document=pymongo.ReturnDocument.AFTER,
        )
        if doc:
            logger.info(
                "Claimed enriching stage lease",
                extra={"document_id": str(doc["_id"]), "version": doc["content_version"]},
            )
            return doc, "enriching"

        # 2. Attempt to claim Stage 1: PROCESSING
        filter_stage1 = {
            "status": {"$in": ["queued", "processing"]},
            "$expr": {"$eq": ["$content_version", "$stages.processing.for_version"]},
            "$or": [
                {"stages.processing.state": "queued"},
                {
                    "stages.processing.state": "running",
                    "stages.processing.lease_expires_at": {"$lt": now},
                },
            ],
        }
        update_stage1 = {
            "$set": {
                "status": "processing",
                "stages.processing.state": "running",
                "stages.processing.started_at": now,
                "stages.processing.lease_expires_at": lease_expires,
                "updated_at": now,
            },
            "$inc": {"stages.processing.attempts": 1},
        }

        doc = await self.db.documents.find_one_and_update(
            filter_stage1,
            update_stage1,
            return_document=pymongo.ReturnDocument.AFTER,
        )
        if doc:
            logger.info(
                "Claimed processing stage lease",
                extra={"document_id": str(doc["_id"]), "version": doc["content_version"]},
            )
            return doc, "processing"

        return None

    async def execute_processing_stage(
        self,
        doc: Dict[str, Any],
        random_fn: Optional[Callable[[], float]] = None,
    ) -> None:
        """Executes processing stage (summary generation) with CAS write."""
        doc_id = doc["_id"]
        version = doc["content_version"]
        user_id = doc.get("user_id")

        delay = settings.PROCESSING_SECONDS
        if delay > 0:
            await asyncio.sleep(delay)

        # Injectable random function for deterministic failure testing
        rand_val = random_fn() if random_fn is not None else random.random()
        should_fail = (rand_val < settings.FAILURE_RATE)

        now = datetime.now(timezone.utc)
        if should_fail:
            error_msg = "Simulated worker processing failure (upstream LLM/model timeout)"
            logger.warning(
                "Processing stage simulated failure",
                extra={"document_id": str(doc_id), "version": version, "error": error_msg},
            )
            cas_filter = {
                "_id": doc_id,
                "content_version": version,
                "stages.processing.for_version": version,
                "stages.processing.state": "running",
            }
            cas_update = {
                "$set": {
                    "status": "failed",
                    "stages.processing.state": "failed",
                    "stages.processing.error": error_msg,
                    "stages.processing.finished_at": now,
                    "stages.processing.lease_expires_at": None,
                    "updated_at": now,
                }
            }
            res = await self.db.documents.update_one(cas_filter, cas_update)
            if res.modified_count > 0:
                await release_active_document_slot(self.redis, self.db, user_id)
            return

        # Success path: Generate mock summary
        summary = generate_mock_summary(doc.get("title", ""), doc.get("content", ""))

        # Compare-And-Set write: Must match exact content_version worker started with
        cas_filter = {
            "_id": doc_id,
            "content_version": version,
            "stages.processing.for_version": version,
            "stages.processing.state": "running",
        }
        cas_update = {
            "$set": {
                "result.summary": summary,
                "result.summary_version": version,
                "stages.processing.state": "completed",
                "stages.processing.error": None,
                "stages.processing.finished_at": now,
                "stages.processing.lease_expires_at": None,
                "stages.enriching.state": "queued",
                "stages.enriching.for_version": version,
                "status": "enriching",
                "updated_at": now,
            }
        }

        res = await self.db.documents.update_one(cas_filter, cas_update)
        if res.modified_count == 0:
            logger.info(
                "Processing CAS write discarded (document was patched or preempted)",
                extra={"document_id": str(doc_id), "version": version},
            )
        else:
            logger.info(
                "Processing stage completed successfully",
                extra={"document_id": str(doc_id), "version": version},
            )

    async def execute_enriching_stage(
        self,
        doc: Dict[str, Any],
        random_fn: Optional[Callable[[], float]] = None,
    ) -> None:
        """
        Executes enriching stage (tags generation) using summary from processing stage.
        If enriching fails, processing summary is PRESERVED.
        """
        doc_id = doc["_id"]
        version = doc["content_version"]
        user_id = doc.get("user_id")
        content_hash = doc.get("content_hash")

        delay = settings.ENRICHING_SECONDS
        if delay > 0:
            await asyncio.sleep(delay)

        # Injectable random function for deterministic failure testing
        rand_val = random_fn() if random_fn is not None else random.random()
        should_fail = (rand_val < settings.FAILURE_RATE)

        now = datetime.now(timezone.utc)
        if should_fail:
            error_msg = "Simulated worker enriching failure (tagging service unavailable)"
            logger.warning(
                "Enriching stage simulated failure; preserving processing stage",
                extra={"document_id": str(doc_id), "version": version, "error": error_msg},
            )
            cas_filter = {
                "_id": doc_id,
                "content_version": version,
                "stages.enriching.for_version": version,
                "stages.enriching.state": "running",
            }
            cas_update = {
                "$set": {
                    "status": "failed",
                    "stages.enriching.state": "failed",
                    "stages.enriching.error": error_msg,
                    "stages.enriching.finished_at": now,
                    "stages.enriching.lease_expires_at": None,
                    "updated_at": now,
                }
            }
            res = await self.db.documents.update_one(cas_filter, cas_update)
            if res.modified_count > 0:
                await release_active_document_slot(self.redis, self.db, user_id)
            return

        # Success path: Read summary produced by processing stage
        current_doc = await self.db.documents.find_one({"_id": doc_id})
        if not current_doc or current_doc.get("content_version") != version:
            logger.info(
                "Enriching aborted: document version changed mid-execution",
                extra={"document_id": str(doc_id), "version": version},
            )
            return

        summary = current_doc.get("result", {}).get("summary")
        if not summary or current_doc.get("result", {}).get("summary_version") != version:
            logger.info(
                "Enriching aborted: processing summary not yet available for version",
                extra={"document_id": str(doc_id), "version": version},
            )
            return

        tags = generate_mock_tags(summary, current_doc.get("content", ""))

        # Compare-And-Set write for Enriching completion
        cas_filter = {
            "_id": doc_id,
            "content_version": version,
            "stages.enriching.for_version": version,
            "stages.enriching.state": "running",
        }
        cas_update = {
            "$set": {
                "result.tags": tags,
                "result.tags_version": version,
                "result.tags_source_summary_version": version,
                "stages.enriching.state": "completed",
                "stages.enriching.error": None,
                "stages.enriching.finished_at": now,
                "stages.enriching.lease_expires_at": None,
                "status": "completed",
                "updated_at": now,
            }
        }

        res = await self.db.documents.update_one(cas_filter, cas_update)
        if res.modified_count == 0:
            logger.info(
                "Enriching CAS write discarded (document was patched or preempted)",
                extra={"document_id": str(doc_id), "version": version},
            )
        else:
            logger.info(
                "Enriching stage completed; document now fully completed",
                extra={"document_id": str(doc_id), "version": version},
            )
            if content_hash:
                await set_content_cache(self.redis, content_hash, summary, tags)
            await release_active_document_slot(self.redis, self.db, user_id)

    async def run_stage(
        self,
        doc_id: Union[str, ObjectId],
        stage: str,
        pause_event: Optional[asyncio.Event] = None,
        resume_event: Optional[asyncio.Event] = None,
        random_fn: Optional[Callable[[], float]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Deterministic, testable entrypoint allowing tests to run a single stage directly
        on a specific document, with pause/resume hooks to reproduce race conditions.
        """
        oid = ObjectId(doc_id) if isinstance(doc_id, str) else doc_id
        doc = await self.db.documents.find_one({"_id": oid})
        if not doc:
            return None

        now = datetime.now(timezone.utc)
        lease_expires = now + timedelta(seconds=settings.LEASE_TIMEOUT_SECONDS)

        # Claim stage if queued
        if stage == "processing":
            if doc.get("stages", {}).get("processing", {}).get("state") == "queued":
                doc = await self.db.documents.find_one_and_update(
                    {
                        "_id": oid,
                        "content_version": doc["content_version"],
                        "stages.processing.state": "queued",
                    },
                    {
                        "$set": {
                            "status": "processing",
                            "stages.processing.state": "running",
                            "stages.processing.started_at": now,
                            "stages.processing.lease_expires_at": lease_expires,
                            "updated_at": now,
                        },
                        "$inc": {"stages.processing.attempts": 1},
                    },
                    return_document=pymongo.ReturnDocument.AFTER,
                )
                if not doc:
                    return None
            elif doc.get("stages", {}).get("processing", {}).get("state") != "running":
                return doc

        elif stage == "enriching":
            if doc.get("stages", {}).get("processing", {}).get("state") != "completed":
                return doc
            if doc.get("stages", {}).get("enriching", {}).get("state") == "queued":
                doc = await self.db.documents.find_one_and_update(
                    {
                        "_id": oid,
                        "content_version": doc["content_version"],
                        "stages.enriching.state": "queued",
                    },
                    {
                        "$set": {
                            "status": "enriching",
                            "stages.enriching.state": "running",
                            "stages.enriching.started_at": now,
                            "stages.enriching.lease_expires_at": lease_expires,
                            "updated_at": now,
                        },
                        "$inc": {"stages.enriching.attempts": 1},
                    },
                    return_document=pymongo.ReturnDocument.AFTER,
                )
                if not doc:
                    return None
            elif doc.get("stages", {}).get("enriching", {}).get("state") != "running":
                return doc
        else:
            raise ValueError(f"Unknown stage: {stage}")

        # Mid-stage pause hook for test race conditions
        if pause_event is not None:
            pause_event.set()

        # Wait for test to resume execution
        if resume_event is not None:
            await resume_event.wait()

        # Execute stage
        if stage == "processing":
            await self.execute_processing_stage(doc, random_fn=random_fn)
        elif stage == "enriching":
            await self.execute_enriching_stage(doc, random_fn=random_fn)

        return await self.db.documents.find_one({"_id": oid})

    async def step(self, random_fn: Optional[Callable[[], float]] = None) -> bool:
        """
        Executes a single pipeline step: attempts to claim and execute one stage.
        Returns True if a job was found and processed, False otherwise.
        """
        claimed = await self.claim_next_job()
        if not claimed:
            return False

        doc, stage_name = claimed
        try:
            if stage_name == "processing":
                await self.execute_processing_stage(doc, random_fn=random_fn)
            elif stage_name == "enriching":
                await self.execute_enriching_stage(doc, random_fn=random_fn)
            return True
        except Exception as e:
            logger.error(
                "Unhandled exception during stage execution",
                exc_info=True,
                extra={"document_id": str(doc.get("_id")), "stage": stage_name, "error": str(e)},
            )
            try:
                now = datetime.now(timezone.utc)
                await self.db.documents.update_one(
                    {
                        "_id": doc["_id"],
                        "content_version": doc["content_version"],
                        f"stages.{stage_name}.state": "running",
                    },
                    {
                        "$set": {
                            "status": "failed",
                            f"stages.{stage_name}.state": "failed",
                            f"stages.{stage_name}.error": f"Internal worker error: {str(e)}",
                            f"stages.{stage_name}.finished_at": now,
                            f"stages.{stage_name}.lease_expires_at": None,
                            "updated_at": now,
                        }
                    },
                )
                await release_active_document_slot(self.redis, self.db, doc.get("user_id"))
            except Exception as inner_e:
                logger.error("Failed to mark stage failed after crash", extra={"error": str(inner_e)})
            return True


async def run_worker_loop(
    db: AsyncIOMotorDatabase,
    redis: Optional[aioredis.Redis],
    stop_event: asyncio.Event,
) -> None:
    """Continuous worker loop polling for and executing pipeline stages."""
    worker = PipelineWorker(db, redis)
    logger.info("Pipeline worker loop started")
    while not stop_event.is_set():
        try:
            processed = await worker.step()
            if not processed:
                await asyncio.sleep(settings.WORKER_POLL_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            logger.info("Pipeline worker cancelled")
            break
        except Exception as e:
            logger.error("Unexpected error in worker loop", exc_info=True, extra={"error": str(e)})
            await asyncio.sleep(settings.WORKER_POLL_INTERVAL_SECONDS)
    logger.info("Pipeline worker loop exited")
