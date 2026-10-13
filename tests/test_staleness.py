import asyncio
import random
import pytest
from httpx import AsyncClient
from bson import ObjectId
from tests.conftest import make_doc, make_headers, wait_for_status


@pytest.mark.asyncio
async def test_patch_mid_processing_discards_stale_worker_write(
    client: AsyncClient, test_db, pipeline_worker
):
    """
    Pause the worker at v1 mid-processing, PATCH to v2, release the worker,
    and assert the DB has discarded the v1 write without polluting v2.
    """
    user = "user_staleness_cas"
    res = await make_doc(client, user, title="V1 Doc", content="Content for version 1.")
    doc_id = res["data"]["document_id"]

    pause_event = asyncio.Event()
    resume_event = asyncio.Event()

    # 1. Start worker on stage 'processing' with pause/resume hooks
    worker_task = asyncio.create_task(
        pipeline_worker.run_stage(
            doc_id,
            "processing",
            pause_event=pause_event,
            resume_event=resume_event,
        )
    )

    # 2. Wait until worker has claimed the job and is paused mid-stage
    await pause_event.wait()

    # 3. Client executes PATCH to v2 while worker is paused
    patch_resp = await client.patch(
        f"/documents/{doc_id}",
        json={"content": "Content updated to version 2."},
        headers=make_headers(user),
    )
    assert patch_resp.status_code == 200
    patched_data = patch_resp.json()
    assert patched_data["content_version"] == 2
    assert patched_data["status"] == "queued"
    assert patched_data["summary"] is None
    assert patched_data["tags"] is None

    # 4. Release the worker so it attempts its CAS write with version 1
    resume_event.set()
    await worker_task

    # 5. Assert the DB has NO v1 summary and remains queued at v2
    raw_doc = await test_db.documents.find_one({"_id": ObjectId(doc_id)})
    assert raw_doc["content_version"] == 2
    assert raw_doc["status"] == "queued"
    assert raw_doc.get("result", {}).get("summary") is None
    assert raw_doc.get("result", {}).get("summary_version") is None

    # 6. Reprocess cleanly through v2
    await pipeline_worker.run_stage(doc_id, "processing")
    await pipeline_worker.run_stage(doc_id, "enriching")

    final_resp = await client.get(f"/documents/{doc_id}", headers=make_headers(user))
    final_doc = final_resp.json()
    assert final_doc["content_version"] == 2
    assert final_doc["status"] == "completed"
    assert final_doc["is_current"] is True
    assert "version 2" in final_doc["summary"]


@pytest.mark.asyncio
async def test_get_returns_null_summary_and_reprocessing_status_until_v2_completes(
    client: AsyncClient, pipeline_worker
):
    """
    After PATCH, GET returns summary=null and reprocessing status until v2 completes.
    Final result has summary_version == tags_version == content_version.
    """
    user = "user_reprocess_status"
    res = await make_doc(client, user, title="Reprocess Test", content="Original content.")
    doc_id = res["data"]["document_id"]

    # Complete v1
    await pipeline_worker.run_stage(doc_id, "processing")
    await pipeline_worker.run_stage(doc_id, "enriching")

    v1_doc = await wait_for_status(client, doc_id, "completed", user_id=user)
    assert v1_doc["is_current"] is True
    assert v1_doc["summary"] is not None

    # PATCH to v2
    patch_resp = await client.patch(
        f"/documents/{doc_id}",
        json={"content": "New v2 content."},
        headers=make_headers(user),
    )
    assert patch_resp.status_code == 200

    # Intermediate checks: must return summary=null while reprocessing
    get1 = await client.get(f"/documents/{doc_id}", headers=make_headers(user))
    d1 = get1.json()
    assert d1["status"] == "queued"
    assert d1["summary"] is None
    assert d1["tags"] is None
    assert d1["is_current"] is False

    # Complete v2 processing
    await pipeline_worker.run_stage(doc_id, "processing")
    get2 = await client.get(f"/documents/{doc_id}", headers=make_headers(user))
    d2 = get2.json()
    assert d2["status"] == "enriching"
    assert d2["summary"] is None  # Still null because tags not completed for v2
    assert d2["is_current"] is False

    # Complete v2 enriching
    await pipeline_worker.run_stage(doc_id, "enriching")
    final_doc = await wait_for_status(client, doc_id, "completed", user_id=user)
    assert final_doc["content_version"] == 2
    assert final_doc["is_current"] is True
    assert final_doc["summary"] is not None
    assert final_doc["tags"] is not None


@pytest.mark.asyncio
async def test_staleness_property_loop_50_interleavings(client: AsyncClient, pipeline_worker):
    """
    Property-style loop: 50 random PATCH, poll, and worker stage execution interleavings.
    Asserts the staleness invariant on EVERY single read:
    - Never returns mixed versions.
    - summary/tags are only present when is_current is True.
    """
    user = "user_property_loop"
    res = await make_doc(client, user, title="Property Doc", content="Initial content.")
    doc_id = res["data"]["document_id"]

    for iteration in range(50):
        action = random.choice(["poll", "patch", "worker_step"])

        if action == "poll":
            resp = await client.get(f"/documents/{doc_id}", headers=make_headers(user))
            assert resp.status_code == 200
            data = resp.json()
            is_current = data["is_current"]
            summary = data["summary"]
            tags = data["tags"]
            status = data["status"]

            if is_current:
                assert status == "completed", f"Status must be completed when is_current=True, got {status}"
                assert summary is not None, "Summary must be present when is_current=True"
                assert tags is not None, "Tags must be present when is_current=True"
            else:
                assert summary is None, f"Summary must be None when is_current=False (iteration {iteration})"
                assert tags is None, f"Tags must be None when is_current=False (iteration {iteration})"

        elif action == "patch":
            new_text = f"Content revised at iteration {iteration} with random token {random.randint(1000, 9999)}."
            p_resp = await client.patch(
                f"/documents/{doc_id}",
                json={"content": new_text},
                headers=make_headers(user),
            )
            assert p_resp.status_code in [200, 409]
            if p_resp.status_code == 200:
                p_data = p_resp.json()
                assert p_data["summary"] is None
                assert p_data["tags"] is None
                assert p_data["is_current"] is False

        elif action == "worker_step":
            # Attempt either processing or enriching
            stg = random.choice(["processing", "enriching"])
            await pipeline_worker.run_stage(doc_id, stg)

        # Immediate poll verification after any action
        check_resp = await client.get(f"/documents/{doc_id}", headers=make_headers(user))
        check_data = check_resp.json()
        if not check_data["is_current"]:
            assert check_data["summary"] is None
            assert check_data["tags"] is None
        else:
            assert check_data["status"] == "completed"
            assert check_data["summary"] is not None
            assert check_data["tags"] is not None
