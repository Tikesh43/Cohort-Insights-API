import pytest
from httpx import AsyncClient
from app.models.db_models import compute_content_hash
from app.services.cache import cache_key
from tests.conftest import make_doc, make_headers


@pytest.mark.asyncio
async def test_cache_deduplication_and_rate_limit_bypass(client: AsyncClient, pipeline_worker):
    """
    Same content from two docs:
    The second returns completed immediately and doesn't count toward the rate limit.
    """
    user1 = "user_cache_alpha"
    user2 = "user_cache_beta"
    shared_content = "Deterministic cohort analysis data regarding Q3 conversion rates."

    # 1. First doc created and processed to completion
    res1 = await make_doc(client, user1, title="Doc 1", content=shared_content)
    assert res1["status_code"] == 201
    assert res1["data"]["status"] == "queued"
    doc1_id = res1["data"]["document_id"]

    await pipeline_worker.run_stage(doc1_id, "processing")
    await pipeline_worker.run_stage(doc1_id, "enriching")

    doc1_data = (await client.get(f"/documents/{doc1_id}", headers=make_headers(user1))).json()
    assert doc1_data["status"] == "completed"

    # Fill user2's active quota to 3 with other documents
    for i in range(3):
        res_fill = await make_doc(client, user2, title=f"Fill {i}", content=f"Unique fill text {i}")
        assert res_fill["status_code"] == 201

    # User2 now submits shared_content: Cache hit!
    # Bypasses rate limit (would 429 if active) and immediately completes
    res2 = await make_doc(client, user2, title="Doc 2", content=shared_content)
    assert res2["status_code"] == 201
    assert res2["data"]["status"] == "completed"
    doc2_id = res2["data"]["document_id"]

    doc2_data = (await client.get(f"/documents/{doc2_id}", headers=make_headers(user2))).json()
    assert doc2_data["status"] == "completed"
    assert doc2_data["is_current"] is True
    assert doc2_data["summary"] == doc1_data["summary"]
    assert doc2_data["tags"] == doc1_data["tags"]


@pytest.mark.asyncio
async def test_patch_to_new_content_and_back_to_old_content(client: AsyncClient, pipeline_worker):
    """
    - PATCH to new content never returns the old content's cached result.
    - PATCH back to the old content hits the old cache entry correctly (the key is the content hash).
    """
    user = "user_cache_patch"
    content_a = "Original cached text alpha."
    content_b = "Revised brand new text beta."

    # 1. Create and complete content_a
    res = await make_doc(client, user, title="Alpha Doc", content=content_a)
    doc_id = res["data"]["document_id"]
    await pipeline_worker.run_stage(doc_id, "processing")
    await pipeline_worker.run_stage(doc_id, "enriching")

    d_a = (await client.get(f"/documents/{doc_id}", headers=make_headers(user))).json()
    summary_a = d_a["summary"]
    tags_a = d_a["tags"]
    assert summary_a is not None

    # 2. PATCH to content_b (uncached)
    patch_b = await client.patch(
        f"/documents/{doc_id}",
        json={"content": content_b},
        headers=make_headers(user),
    )
    assert patch_b.status_code == 200
    d_b = patch_b.json()
    assert d_b["status"] == "queued"
    assert d_b["content_version"] == 2
    # Never leaks old cached result
    assert d_b["summary"] is None
    assert d_b["tags"] is None
    assert d_b["is_current"] is False

    # 3. PATCH back to content_a: hits old cache entry keyed by content_hash!
    patch_a_again = await client.patch(
        f"/documents/{doc_id}",
        json={"content": content_a},
        headers=make_headers(user),
    )
    assert patch_a_again.status_code == 200
    d_a_again = patch_a_again.json()
    assert d_a_again["content_version"] == 3
    # Fast-completed from cache!
    assert d_a_again["status"] == "completed"
    assert d_a_again["is_current"] is True
    assert d_a_again["summary"] == summary_a
    assert d_a_again["tags"] == tags_a


@pytest.mark.asyncio
async def test_cache_entries_have_ttl(client: AsyncClient, pipeline_worker, test_redis):
    """Assert that cache entries stored in Redis have a positive TTL set."""
    user = "user_cache_ttl"
    content = "Content to verify Redis cache TTL expiration."

    res = await make_doc(client, user, title="TTL Doc", content=content)
    doc_id = res["data"]["document_id"]
    await pipeline_worker.run_stage(doc_id, "processing")
    await pipeline_worker.run_stage(doc_id, "enriching")

    h = compute_content_hash(content)
    key = cache_key(h)

    ttl = await test_redis.ttl(key)
    # TTL must be set and greater than 0
    assert ttl > 0, f"Expected positive TTL on cache key '{key}', got {ttl}"
