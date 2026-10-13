import asyncio
import pytest
from httpx import AsyncClient
from tests.conftest import make_doc, make_headers


@pytest.mark.asyncio
async def test_concurrent_patches_optimistic_concurrency_defined_outcome(client: AsyncClient):
    """
    Two PATCH requests race to update the exact same document simultaneously.
    Defined outcome:
    - Under optimistic concurrency, if both requests read version V, exactly one update
      wins (200 OK, bumping version to V+1), while the conflicting concurrent request
      receives 409 Conflict.
    - If serialized at the socket layer, both succeed sequentially with version increments (V -> V+1 -> V+2).
    - Under all outcomes: no lost updates, version monotonicity is preserved, and the database remains consistent.
    """
    user = "user_concurrent_patch_race"
    res = await make_doc(client, user, title="Concurrent Doc", content="Initial Version 1 text.")
    assert res["status_code"] == 201
    doc_id = res["data"]["document_id"]

    # Concurrently dispatch 2 competing PATCH requests
    patch1_task = client.patch(
        f"/documents/{doc_id}",
        json={"content": "Content modification from Worker Thread A."},
        headers=make_headers(user),
    )
    patch2_task = client.patch(
        f"/documents/{doc_id}",
        json={"content": "Content modification from Worker Thread B."},
        headers=make_headers(user),
    )

    resp1, resp2 = await asyncio.gather(patch1_task, patch2_task)
    status_codes = sorted([resp1.status_code, resp2.status_code])

    # Assert defined outcome: either [200, 409] (optimistic concurrency conflict)
    # or [200, 200] (sequential version increments)
    assert status_codes in ([200, 409], [200, 200]), f"Unexpected race status codes: {status_codes}"

    if 409 in status_codes:
        conflict_resp = resp1 if resp1.status_code == 409 else resp2
        assert "Concurrent modification conflict" in conflict_resp.json()["detail"]

    # Final document state check: version is at least 2, status is queued, derived fields are null
    final_resp = await client.get(f"/documents/{doc_id}", headers=make_headers(user))
    assert final_resp.status_code == 200
    final_doc = final_resp.json()
    assert final_doc["content_version"] >= 2
    assert final_doc["status"] == "queued"
    assert final_doc["summary"] is None
    assert final_doc["tags"] is None
    assert final_doc["is_current"] is False
