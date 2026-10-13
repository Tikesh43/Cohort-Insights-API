import pytest
from httpx import AsyncClient
from tests.conftest import make_doc, make_headers


@pytest.mark.asyncio
async def test_another_users_get_and_patch_returns_404_identical_to_nonexistent(client: AsyncClient):
    """
    Another user's GET and PATCH -> 404 (same response body as a nonexistent id).
    Guarantees zero-information leakage.
    """
    user_alice = "user_alice_own"
    user_bob = "user_bob_intruder"

    # Alice creates a document
    res = await make_doc(client, user_alice, title="Alice Secret", content="Private content")
    alice_doc_id = res["data"]["document_id"]
    nonexistent_id = "507f1f77bcf86cd799439011"

    # 1. Nonexistent GET response
    nonexistent_resp = await client.get(f"/documents/{nonexistent_id}", headers=make_headers(user_bob))
    assert nonexistent_resp.status_code == 404
    expected_body = nonexistent_resp.json()

    # 2. Bob attempts GET on Alice's document -> 404 with identical body
    bob_get = await client.get(f"/documents/{alice_doc_id}", headers=make_headers(user_bob))
    assert bob_get.status_code == 404
    assert bob_get.json() == expected_body

    # 3. Nonexistent PATCH response
    nonexistent_patch = await client.patch(
        f"/documents/{nonexistent_id}",
        json={"content": "Malicious payload"},
        headers=make_headers(user_bob),
    )
    assert nonexistent_patch.status_code == 404
    expected_patch_body = nonexistent_patch.json()

    # 4. Bob attempts PATCH on Alice's document -> 404 with identical body
    bob_patch = await client.patch(
        f"/documents/{alice_doc_id}",
        json={"content": "Malicious payload"},
        headers=make_headers(user_bob),
    )
    assert bob_patch.status_code == 404
    assert bob_patch.json() == expected_patch_body


@pytest.mark.asyncio
async def test_list_endpoint_only_returns_callers_docs(client: AsyncClient):
    """List endpoint only returns the caller's docs, never leaking other tenants' records."""
    user_alice = "user_alice_listing"
    user_bob = "user_bob_listing"

    # Alice creates 2 documents
    await make_doc(client, user_alice, title="Alice Doc 1", content="Text 1")
    await make_doc(client, user_alice, title="Alice Doc 2", content="Text 2")

    # Bob creates 1 document
    await make_doc(client, user_bob, title="Bob Doc 1", content="Text B1")

    # Bob lists his documents
    bob_list = await client.get(f"/users/{user_bob}/documents", headers=make_headers(user_bob))
    assert bob_list.status_code == 200
    data = bob_list.json()
    assert data["total"] == 1
    assert data["items"][0]["title"] == "Bob Doc 1"

    # Bob attempts to list Alice's documents -> 404
    bob_spies = await client.get(f"/users/{user_alice}/documents", headers=make_headers(user_bob))
    assert bob_spies.status_code == 404


@pytest.mark.asyncio
async def test_invalid_objectid_returns_404_or_422_never_500(client: AsyncClient):
    """Invalid ObjectId strings return 404 or 422, never 500 internal server error."""
    user = "user_invalid_oid"
    invalid_ids = ["not-an-objectid", "123", "undefined", "null", "12345678901234567890123z"]

    for inv_id in invalid_ids:
        # GET invalid ID
        resp_get = await client.get(f"/documents/{inv_id}", headers=make_headers(user))
        assert resp_get.status_code in [404, 422], f"Expected 404 or 422 for GET {inv_id}, got {resp_get.status_code}"

        # PATCH invalid ID
        resp_patch = await client.patch(
            f"/documents/{inv_id}",
            json={"content": "Content"},
            headers=make_headers(user),
        )
        assert resp_patch.status_code in [404, 422], f"Expected 404 or 422 for PATCH {inv_id}, got {resp_patch.status_code}"
