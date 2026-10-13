import asyncio
import pytest
from httpx import AsyncClient
from tests.conftest import make_doc, make_headers


@pytest.mark.asyncio
async def test_by_ref_lookup_and_scoping(client: AsyncClient):
    """by-ref returns the doc, 404 for an unknown ref, 404 for another user's ref."""
    user1 = "user_ref_owner"
    user2 = "user_ref_other"
    ref = "ref_lookup_test_1"

    res = await make_doc(client, user1, title="Ref Doc", content="Content", client_doc_ref=ref)
    assert res["status_code"] == 201
    doc_id = res["data"]["document_id"]

    # 1. Owner lookup succeeds
    get_resp = await client.get(f"/documents/by-ref/{ref}", headers=make_headers(user1))
    assert get_resp.status_code == 200
    assert get_resp.json()["document_id"] == doc_id
    assert get_resp.json()["client_doc_ref"] == ref

    # 2. Unknown ref returns 404
    get_unknown = await client.get("/documents/by-ref/nonexistent_ref_999", headers=make_headers(user1))
    assert get_unknown.status_code == 404

    # 3. Another user's lookup returns 404 (never 403, no leakage)
    get_other = await client.get(f"/documents/by-ref/{ref}", headers=make_headers(user2))
    assert get_other.status_code == 404
    assert get_other.json()["detail"] == "Document not found"


@pytest.mark.asyncio
async def test_repeat_ref_policies_same_content_and_different_content(client: AsyncClient):
    """
    Same ref, same content: idempotent 200 with the same document_id.
    Same ref, different content: 409 with the existing document_id.
    Same ref from a different user: 409 with no data leak.
    """
    user1 = "user_policy_1"
    user2 = "user_policy_2"
    ref = "ref_policy_test_2"
    content1 = "Base content for repeat ref policy test."
    content2 = "Modified content for repeat ref collision test."

    # 1. Initial creation
    res1 = await make_doc(client, user1, title="Initial", content=content1, client_doc_ref=ref)
    assert res1["status_code"] == 201
    doc_id1 = res1["data"]["document_id"]

    # 2. Same user, same ref, same content -> idempotent 200 with same document_id
    res_same = await make_doc(client, user1, title="Retry", content=content1, client_doc_ref=ref)
    assert res_same["status_code"] == 200
    assert res_same["data"]["document_id"] == doc_id1

    # 3. Same user, same ref, different content -> 409 with existing document_id
    res_diff = await make_doc(client, user1, title="Change", content=content2, client_doc_ref=ref)
    assert res_diff["status_code"] == 409
    err = res_diff["data"]
    assert "document_id" in err["detail"]
    assert err["detail"]["document_id"] == doc_id1
    assert "PATCH" in err["detail"]["detail"]

    # 4. Different user, same ref -> 409 with no data leak
    res_other = await make_doc(client, user2, title="Other", content="Arbitrary text", client_doc_ref=ref)
    assert res_other["status_code"] == 409
    assert res_other["data"]["detail"] == "client_doc_ref is already in use"
    assert user1 not in str(res_other["data"])
    assert doc_id1 not in str(res_other["data"])


@pytest.mark.asyncio
async def test_concurrent_duplicate_submissions_same_ref_exactly_one_created(client: AsyncClient, test_db):
    """
    Concurrent duplicate submissions with the same ref:
    Exactly one document is created in MongoDB.
    """
    user = "user_concurrent_ref"
    ref = "concurrent_ref_unique_1"
    content = "Identical content sent concurrently"

    # Fire 5 concurrent submissions with the same ref and identical content
    tasks = [
        make_doc(client, user, title=f"Doc {i}", content=content, client_doc_ref=ref)
        for i in range(5)
    ]
    results = await asyncio.gather(*tasks)

    status_codes = [r["status_code"] for r in results]
    # Exactly one should be 201 (or all 200/201 without errors)
    assert 201 in status_codes
    assert all(code in [200, 201] for code in status_codes)

    # Assert exactly one document exists in the collection with this ref
    count = await test_db.documents.count_documents({"client_doc_ref": ref})
    assert count == 1


@pytest.mark.asyncio
async def test_by_ref_query_uses_unique_index_ixscan(test_db):
    """
    Assert via explain() or index metadata that queries by client_doc_ref
    use the unique index (IXSCAN, not COLLSCAN).
    """
    ref = "explain_test_ref"
    await test_db.documents.insert_one({
        "client_doc_ref": ref,
        "user_id": "test",
        "content_version": 1,
        "status": "queued",
    })

    cursor = test_db.documents.find({"client_doc_ref": ref})
    if hasattr(cursor, "explain"):
        plan = await cursor.explain()
        plan_str = str(plan.get("queryPlanner", {}).get("winningPlan", {}))
        assert "IXSCAN" in plan_str, f"Winning plan should use IXSCAN: {plan_str}"
        assert "COLLSCAN" not in plan_str, f"Winning plan must not use COLLSCAN: {plan_str}"
    else:
        # On mongomock where cursor.explain() is not present, assert index configuration
        indexes = await test_db.documents.index_information()
        assert "idx_unique_client_doc_ref" in indexes
        assert indexes["idx_unique_client_doc_ref"]["unique"] is True
        assert indexes["idx_unique_client_doc_ref"]["partialFilterExpression"] == {
            "client_doc_ref": {"$type": "string"}
        }
