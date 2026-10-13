import asyncio
import pytest
from httpx import AsyncClient
from app.dependencies import get_redis
from app.main import app
from tests.conftest import make_doc, make_headers


@pytest.mark.asyncio
async def test_rate_limit_fourth_active_doc_returns_429(client: AsyncClient, pipeline_worker):
    """
    4th active doc -> 429; the counter covers queued + processing + enriching.
    After one completes or fails, a new submission succeeds.
    """
    user = "user_rate_limit_active"

    # Submit 3 active documents
    doc_ids = []
    for i in range(3):
        res = await make_doc(client, user, title=f"Doc {i}", content=f"Content {i}")
        assert res["status_code"] == 201
        doc_ids.append(res["data"]["document_id"])

    # 4th submission must be rejected with 429
    res4 = await make_doc(client, user, title="Doc 4", content="Content 4")
    assert res4["status_code"] == 429
    assert "Rate limit exceeded" in res4["data"]["detail"]

    # Transition doc 0 through pipeline to completed
    await pipeline_worker.run_stage(doc_ids[0], "processing")
    await pipeline_worker.run_stage(doc_ids[0], "enriching")

    # Now that doc 0 exited the active state, a new submission succeeds
    res_retry = await make_doc(client, user, title="Doc 4 Allowed", content="Content 4 Allowed")
    assert res_retry["status_code"] == 201


@pytest.mark.asyncio
async def test_ten_concurrent_submissions_exactly_three_succeed(client: AsyncClient):
    """
    10 concurrent submissions from one user:
    Atomically handled via Redis Lua script, exactly 3 succeed (201) and 7 receive 429.
    """
    user = "user_concurrent_ten"
    tasks = [
        make_doc(client, user, title=f"Burst Doc {i}", content=f"Burst Content {i}")
        for i in range(10)
    ]
    results = await asyncio.gather(*tasks)

    status_codes = [r["status_code"] for r in results]
    successes = [code for code in status_codes if code == 201]
    rate_limited = [code for code in status_codes if code == 429]

    assert len(successes) == 3, f"Expected exactly 3 successes, got {len(successes)}: {status_codes}"
    assert len(rate_limited) == 7, f"Expected exactly 7 429s, got {len(rate_limited)}: {status_codes}"


@pytest.mark.asyncio
async def test_rate_limit_mongo_fallback_when_redis_down(client: AsyncClient):
    """
    Redis down (mock client returning None):
    The request still works with the Mongo fallback and never crashes.
    """
    user = "user_redis_offline"

    # Simulate Redis offline
    app.dependency_overrides[get_redis] = lambda: None
    try:
        # Submit 3 active documents successfully via Mongo fallback
        for i in range(3):
            res = await make_doc(client, user, title=f"Fallback {i}", content=f"Fallback {i}")
            assert res["status_code"] == 201

        # 4th active document correctly blocked by Mongo fallback with 429
        res4 = await make_doc(client, user, title="Fallback 4", content="Blocked")
        assert res4["status_code"] == 429
        assert "Rate limit exceeded" in res4["data"]["detail"]
    finally:
        app.dependency_overrides.pop(get_redis, None)
