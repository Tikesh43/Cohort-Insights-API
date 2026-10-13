import asyncio
from datetime import datetime, timedelta, timezone
import pytest
from httpx import AsyncClient
from tests.conftest import make_headers


@pytest.mark.asyncio
async def test_listing_order_pagination_status_filter_and_validation(client: AsyncClient, test_db):
    """
    Validates:
    - Newest first ordering by created_at desc.
    - page and page_size pagination.
    - status filter query parameter.
    - Empty page returns empty items list.
    - page_size upper bound validation (>100 -> 422).
    """
    user = "user_listing_full"
    base_time = datetime.now(timezone.utc)

    # Seed 6 documents with explicitly spaced created_at timestamps
    # 4 queued, 2 completed
    for i in range(6):
        doc_status = "completed" if i < 2 else "queued"
        t = base_time + timedelta(seconds=i * 10)
        await test_db.documents.insert_one({
            "user_id": user,
            "title": f"Doc {i}",
            "content": f"Content {i}",
            "content_hash": f"hash_{i}",
            "content_version": 1,
            "status": doc_status,
            "created_at": t,
            "updated_at": t,
        })

    # 1. Newest first check: page=1, page_size=6
    res_all = await client.get(f"/users/{user}/documents?page=1&page_size=10", headers=make_headers(user))
    assert res_all.status_code == 200
    all_data = res_all.json()
    assert all_data["total"] == 6
    assert len(all_data["items"]) == 6
    # Item 0 must be Doc 5 (newest), item 5 must be Doc 0 (oldest)
    assert all_data["items"][0]["title"] == "Doc 5"
    assert all_data["items"][-1]["title"] == "Doc 0"

    # 2. Pagination: page 1 of size 2 -> Doc 5, Doc 4
    res_p1 = await client.get(f"/users/{user}/documents?page=1&page_size=2", headers=make_headers(user))
    assert res_p1.status_code == 200
    p1_data = res_p1.json()
    assert p1_data["page"] == 1
    assert p1_data["page_size"] == 2
    assert p1_data["total_pages"] == 3
    assert [d["title"] for d in p1_data["items"]] == ["Doc 5", "Doc 4"]

    # Page 2 of size 2 -> Doc 3, Doc 2
    res_p2 = await client.get(f"/users/{user}/documents?page=2&page_size=2", headers=make_headers(user))
    assert res_p2.status_code == 200
    assert [d["title"] for d in res_p2.json()["items"]] == ["Doc 3", "Doc 2"]

    # 3. Status filter: ?status=completed (Doc 1, Doc 0)
    res_completed = await client.get(f"/users/{user}/documents?status=completed", headers=make_headers(user))
    assert res_completed.status_code == 200
    c_data = res_completed.json()
    assert c_data["total"] == 2
    assert all(d["status"] == "completed" for d in c_data["items"])

    # ?status=queued (Doc 5, Doc 4, Doc 3, Doc 2)
    res_queued = await client.get(f"/users/{user}/documents?status=queued", headers=make_headers(user))
    assert res_queued.status_code == 200
    assert res_queued.json()["total"] == 4

    # 4. Empty page: requesting page far beyond bounds
    res_empty = await client.get(f"/users/{user}/documents?page=50&page_size=10", headers=make_headers(user))
    assert res_empty.status_code == 200
    empty_data = res_empty.json()
    assert empty_data["total"] == 6
    assert empty_data["page"] == 50
    assert empty_data["items"] == []

    # 5. page_size upper bound validation: page_size=101 must return 422
    res_oversized = await client.get(f"/users/{user}/documents?page_size=101", headers=make_headers(user))
    assert res_oversized.status_code == 422
