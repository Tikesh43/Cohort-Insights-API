import asyncio
import os
import socket
import time
from typing import AsyncGenerator, Callable, Dict, Optional
from bson import ObjectId
import fakeredis.aioredis
from httpx import ASGITransport, AsyncClient
from mongomock_motor import AsyncMongoMockClient
from motor.motor_asyncio import AsyncIOMotorClient
import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from app.config import settings
from app.db import init_db
from app.dependencies import get_db, get_redis
from app.main import app
from app.workers.pipeline import PipelineWorker

# Apply test-friendly configurations: Fast stage simulation, 0 failure rate by default
settings.PROCESSING_SECONDS = 0.05
settings.ENRICHING_SECONDS = 0.05
settings.FAILURE_RATE = 0.0
settings.ENABLE_IN_PROCESS_WORKER = False
settings.DATABASE_NAME = settings.TEST_DATABASE_NAME


def is_service_online(host: str, port: int, timeout: float = 0.5) -> bool:
    """Probes whether a host:port TCP socket accepts connections."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def get_host_port_from_url(url: str, default_port: int) -> tuple[str, int]:
    clean_url = url.split("://")[-1].split("/")[0].split("@")[-1]
    if ":" in clean_url:
        host, port = clean_url.split(":")
        return host, int(port)
    return clean_url, default_port


# Detect whether real MongoDB and Redis are available
mongo_host, mongo_port = get_host_port_from_url(settings.MONGODB_URL, 27017)
redis_host, redis_port = get_host_port_from_url(settings.REDIS_URL, 6379)

USE_REAL_MONGO = is_service_online(mongo_host, mongo_port)
USE_REAL_REDIS = is_service_online(redis_host, redis_port)


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def test_db():
    """Provides cleaned, index-initialized test MongoDB database."""
    if USE_REAL_MONGO:
        client = AsyncIOMotorClient(settings.MONGODB_URL)
        db = client[settings.TEST_DATABASE_NAME]
    else:
        client = AsyncMongoMockClient()
        db = client[settings.TEST_DATABASE_NAME]

    # Clean collection and recreate indexes before each test
    await db.documents.delete_many({})
    await init_db(db)

    yield db

    # Cleanup after test
    await db.documents.delete_many({})
    if USE_REAL_MONGO:
        client.close()


@pytest_asyncio.fixture
async def test_redis():
    """Provides cleaned test Redis client (real or FakeRedis)."""
    if USE_REAL_REDIS:
        r = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=1.0,
        )
    else:
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)

    await r.flushall()
    yield r
    await r.flushall()
    await r.aclose()


@pytest_asyncio.fixture
async def pipeline_worker(test_db, test_redis):
    return PipelineWorker(db=test_db, redis=test_redis)


@pytest_asyncio.fixture
async def client(test_db, test_redis) -> AsyncGenerator[AsyncClient, None]:
    """Test AsyncClient with dependency overrides to test_db and test_redis."""
    app.dependency_overrides[get_db] = lambda: test_db
    app.dependency_overrides[get_redis] = lambda: test_redis

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


def make_headers(user_id: str = "user_test_1") -> dict:
    """Helper to return caller identity headers."""
    return {"X-User-Id": user_id}


async def make_doc(
    client: AsyncClient,
    user: str,
    title: str = "Test Doc",
    content: str = "Standard test content for document pipeline.",
    client_doc_ref: Optional[str] = None,
    **overrides,
) -> dict:
    """Helper to create a document via API and return the response payload."""
    payload = {
        "title": title,
        "content": content,
    }
    if client_doc_ref is not None:
        payload["client_doc_ref"] = client_doc_ref
    payload.update(overrides)

    resp = await client.post("/documents", json=payload, headers=make_headers(user))
    return {"status_code": resp.status_code, "data": resp.json()}


async def wait_for_status(
    client: AsyncClient,
    doc_id: str,
    target_status: str,
    timeout: float = 3.0,
    poll_interval: float = 0.05,
    user_id: str = "user_test_1",
) -> dict:
    """
    Polls GET /documents/{doc_id} until doc achieves target_status or timeout expires.
    Uses short 50ms polling intervals to prevent flaky timing.
    """
    start_time = time.time()
    last_doc = {}
    while time.time() - start_time < timeout:
        resp = await client.get(f"/documents/{doc_id}", headers=make_headers(user_id))
        if resp.status_code == 200:
            last_doc = resp.json()
            if last_doc.get("status") == target_status:
                return last_doc
        await asyncio.sleep(poll_interval)

    raise TimeoutError(
        f"Document {doc_id} failed to reach status '{target_status}' within {timeout}s. "
        f"Last state: {last_doc}"
    )
