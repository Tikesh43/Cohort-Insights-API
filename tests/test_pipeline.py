import asyncio
from datetime import datetime, timedelta, timezone
import pytest
from httpx import AsyncClient
from bson import ObjectId
from app.config import settings
from tests.conftest import make_doc, make_headers, wait_for_status


@pytest.mark.asyncio
async def test_pipeline_happy_path(client: AsyncClient, pipeline_worker):
    """Happy path: queued -> processing -> enriching -> completed."""
    user = "user_pipeline_happy"
    res = await make_doc(client, user, title="Pipeline Happy Doc", content="Content progressing through pipeline.")
    doc_id = res["data"]["document_id"]

    # 1. Initially queued
    d0 = (await client.get(f"/documents/{doc_id}", headers=make_headers(user))).json()
    assert d0["status"] == "queued"
    assert d0["stages"]["processing"]["state"] == "queued"
    assert d0["stages"]["enriching"]["state"] == "queued"

    # 2. Run processing stage
    await pipeline_worker.run_stage(doc_id, "processing")
    d1 = (await client.get(f"/documents/{doc_id}", headers=make_headers(user))).json()
    assert d1["status"] == "enriching"
    assert d1["stages"]["processing"]["state"] == "completed"
    assert d1["stages"]["enriching"]["state"] == "queued"
    assert d1["is_current"] is False

    # 3. Run enriching stage
    await pipeline_worker.run_stage(doc_id, "enriching")
    d2 = (await client.get(f"/documents/{doc_id}", headers=make_headers(user))).json()
    assert d2["status"] == "completed"
    assert d2["stages"]["processing"]["state"] == "completed"
    assert d2["stages"]["enriching"]["state"] == "completed"
    assert d2["is_current"] is True
    assert d2["summary"] is not None
    assert d2["tags"] is not None


@pytest.mark.asyncio
async def test_force_enriching_failure_preserves_processing_summary(client: AsyncClient, pipeline_worker, test_db):
    """
    Force an enriching failure:
    - Status shows stage=enriching failed.
    - Processing result is preserved.
    - Processing attempts == 1.
    """
    user = "user_enrich_fail"
    res = await make_doc(client, user, title="Enrich Fail Doc", content="Content to test enrich failure.")
    doc_id = res["data"]["document_id"]

    # Process stage 1 successfully
    await pipeline_worker.run_stage(doc_id, "processing", random_fn=lambda: 1.0)
    d1 = (await client.get(f"/documents/{doc_id}", headers=make_headers(user))).json()
    assert d1["stages"]["processing"]["state"] == "completed"
    assert d1["stages"]["processing"]["attempts"] == 1

    # Force stage 2 (enriching) to fail via injected random_fn=0.0 (where failure_rate is > 0)
    settings.FAILURE_RATE = 0.5
    try:
        await pipeline_worker.run_stage(doc_id, "enriching", random_fn=lambda: 0.0)
    finally:
        settings.FAILURE_RATE = 0.0

    d2 = (await client.get(f"/documents/{doc_id}", headers=make_headers(user))).json()
    assert d2["status"] == "failed"
    assert d2["stages"]["processing"]["state"] == "completed"
    assert d2["stages"]["processing"]["attempts"] == 1
    assert d2["stages"]["enriching"]["state"] == "failed"
    assert d2["stages"]["enriching"]["error"] is not None

    # Verify directly in MongoDB that processing summary remains intact
    raw_doc = await test_db.documents.find_one({"_id": ObjectId(doc_id)})
    assert raw_doc["result"]["summary"] is not None
    assert raw_doc["result"]["summary_version"] == 1


@pytest.mark.asyncio
async def test_force_processing_failure_never_starts_enriching(client: AsyncClient, pipeline_worker):
    """Force a processing failure: enriching is never started."""
    user = "user_proc_fail"
    res = await make_doc(client, user, title="Proc Fail Doc", content="Content to test proc failure.")
    doc_id = res["data"]["document_id"]

    # Force stage 1 (processing) failure via injected random_fn
    settings.FAILURE_RATE = 0.5
    try:
        await pipeline_worker.run_stage(doc_id, "processing", random_fn=lambda: 0.0)
    finally:
        settings.FAILURE_RATE = 0.0

    d = (await client.get(f"/documents/{doc_id}", headers=make_headers(user))).json()
    assert d["status"] == "failed"
    assert d["stages"]["processing"]["state"] == "failed"
    assert d["stages"]["enriching"]["state"] == "queued"
    assert d["stages"]["enriching"]["attempts"] == 0


@pytest.mark.asyncio
async def test_two_workers_claim_same_job_only_one_wins(test_db, test_redis):
    """Two workers claim the same job concurrently: exactly one wins via find_one_and_update."""
    worker1 = type(test_redis.pipeline_worker if hasattr(test_redis, 'pipeline_worker') else None)(
        db=test_db, redis=test_redis
    ) if False else None
    from app.workers.pipeline import PipelineWorker
    w1 = PipelineWorker(db=test_db, redis=test_redis)
    w2 = PipelineWorker(db=test_db, redis=test_redis)

    # Insert a single queued doc
    now = datetime.now(timezone.utc)
    res = await test_db.documents.insert_one({
        "user_id": "user_claim_race",
        "title": "Claim Race Doc",
        "content": "Single job race",
        "content_version": 1,
        "content_hash": "hash_claim",
        "status": "queued",
        "stages": {
            "processing": {"state": "queued", "attempts": 0, "for_version": 1},
            "enriching": {"state": "queued", "attempts": 0, "for_version": 1},
        },
        "created_at": now,
        "updated_at": now,
    })

    # Concurrently attempt claim
    claims = await asyncio.gather(w1.claim_next_job(), w2.claim_next_job())
    won = [c for c in claims if c is not None]
    lost = [c for c in claims if c is None]

    assert len(won) == 1, "Exactly one worker must successfully claim the stage"
    assert len(lost) == 1, "The competing worker must receive None"
    doc, stage = won[0]
    assert doc["_id"] == res.inserted_id
    assert stage == "processing"


@pytest.mark.asyncio
async def test_crashed_worker_lease_expiry_reclaimed(pipeline_worker, test_db):
    """Crashed worker lease expiry: the job is reclaimed after lease_expires_at."""
    now = datetime.now(timezone.utc)
    expired_time = now - timedelta(seconds=120)

    # Document whose worker crashed while processing
    res = await test_db.documents.insert_one({
        "user_id": "user_crashed_worker",
        "title": "Crashed Worker Doc",
        "content": "Crashed job",
        "content_version": 1,
        "content_hash": "hash_crashed",
        "status": "processing",
        "stages": {
            "processing": {
                "state": "running",
                "attempts": 1,
                "for_version": 1,
                "lease_expires_at": expired_time,
            },
            "enriching": {
                "state": "queued",
                "attempts": 0,
                "for_version": 1,
            },
        },
        "created_at": now,
        "updated_at": now,
    })

    # Pipeline worker claims and reclaims the expired lease
    claimed = await pipeline_worker.claim_next_job()
    assert claimed is not None
    claimed_doc, stage = claimed
    assert claimed_doc["_id"] == res.inserted_id
    assert stage == "processing"
    assert claimed_doc["stages"]["processing"]["attempts"] == 2  # Reclaimed attempt incremented
