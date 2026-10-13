import pytest
from httpx import AsyncClient
from app.dependencies import get_db, get_redis
from app.main import app
from tests.conftest import make_headers


@pytest.mark.asyncio
async def test_validation_empty_oversized_and_missing_headers(client: AsyncClient):
    """
    Validates:
    - Empty title -> 422
    - Empty content -> 422
    - Oversized content (>500,000 chars) -> 422
    - Missing X-User-Id header -> 400 or 401 or 422
    """
    valid_headers = make_headers("user_val_test")

    # 1. Empty title -> 422
    res_empty_title = await client.post(
        "/documents",
        json={"title": "", "content": "Valid content"},
        headers=valid_headers,
    )
    assert res_empty_title.status_code == 422

    # 2. Empty content -> 422
    res_empty_content = await client.post(
        "/documents",
        json={"title": "Valid title", "content": ""},
        headers=valid_headers,
    )
    assert res_empty_content.status_code == 422

    # 3. Oversized content (>500,000 chars) -> 422
    oversized_text = "x" * 500_001
    res_oversized = await client.post(
        "/documents",
        json={"title": "Valid title", "content": oversized_text},
        headers=valid_headers,
    )
    assert res_oversized.status_code == 422

    # 4. Missing user header -> 400/401/422
    res_no_header = await client.post(
        "/documents",
        json={"title": "Valid title", "content": "Valid content"},
        # No X-User-Id header
    )
    assert res_no_header.status_code in [400, 401, 422]


@pytest.mark.asyncio
async def test_health_check_operational_states(client: AsyncClient):
    """
    /health returns:
    - 200 'healthy' when both services operational.
    - 200 'degraded' when Redis is down.
    - 503 'unhealthy' when MongoDB is unreachable.
    """
    # 1. Healthy state
    h1 = await client.get("/health")
    assert h1.status_code == 200
    d1 = h1.json()
    assert d1["status"] in ["healthy", "degraded"]
    assert d1["mongo"] == "connected"

    # 2. Degraded state (Redis unavailable)
    app.dependency_overrides[get_redis] = lambda: None
    try:
        h2 = await client.get("/health")
        assert h2.status_code == 200
        d2 = h2.json()
        assert d2["status"] == "degraded"
        assert d2["mongo"] == "connected"
        assert d2["redis"] == "disconnected"
    finally:
        app.dependency_overrides.pop(get_redis, None)

    # 3. Unhealthy state (MongoDB unavailable)
    class FailingMongo:
        async def command(self, cmd):
            raise ConnectionError("MongoDB unreachable")

    app.dependency_overrides[get_db] = lambda: FailingMongo()
    try:
        h3 = await client.get("/health")
        assert h3.status_code == 503
        d3 = h3.json()
        assert d3["status"] == "unhealthy"
        assert d3["mongo"] == "disconnected"
    finally:
        app.dependency_overrides.pop(get_db, None)
