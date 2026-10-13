# Cohort Insights API

A production-grade, asynchronous REST API designed for processing and extracting cohort insights from raw customer documents. Built with **Python 3.11**, **FastAPI**, **MongoDB (Motor)**, and **Redis (aioredis)**, complete with simulated multi-stage pipelines, strict staleness guarantees, atomic concurrency leases, and content-hash caching.

---

## Architecture Overview

```
                                      +-------------------------+
                                      |      Client Caller      |
                                      +-------------------------+
                                                   |
                                       X-User-Id   | HTTP REST
                                                   v
                                      +-------------------------+
                                      |     FastAPI Service     |
                                      +-------------------------+
                                        /          |          \
                 Cache & Rate-Limit    /           |           \  Scoped CRUD & State
                                      v            |            v
                            +---------------+      |      +------------------+
                            |     Redis     |      |      |     MongoDB      |
                            | Rate Limiter  |      |      | Document Storage |
                            | Content Cache |      |      | Partial Unique IX|
                            +---------------+      |      +------------------+
                                                   v
                                      +-------------------------+
                                      |    Pipeline Worker      |
                                      |   (Lease Claim & CAS)   |
                                      +-------------------------+
                                        /                     \
                             Stage 1   /                       \   Stage 2
                                      v                         v
                           +--------------------+      +--------------------+
                           |     Processing     |----->|     Enriching      |
                           |  (10-20s mock sum) |      | (5-15s mock tags)  |
                           +--------------------+      +--------------------+
```

---

## Key Features

1. **Two-Stage Pipeline with Failure Isolation**:
   - `queued` $\rightarrow$ `processing` (10–20s sim) $\rightarrow$ `enriching` (5–15s sim) $\rightarrow$ `completed` | `failed`.
   - Simulated 10% random failure rate per stage.
   - Stage failures are completely isolated: an enriching failure preserves the generated processing summary and never causes processing to rerun.
2. **Strict Staleness Prevention by Construction**:
   - Every document is tracked via `content_hash` (SHA-256) and monotonically increasing `content_version` (int).
   - Worker writes use Compare-and-Set (`content_version` bound in filter). Any `PATCH` applied mid-execution safely causes stale worker writes to be discarded.
   - Responses return derived data (`summary`, `tags`) **only** when `summary_version == tags_version == content_version`.
3. **Atomic Rate Limiting with MongoDB Fallback**:
   - Caps each user at 3 concurrent active documents across `{queued, processing, enriching}`.
   - Managed via atomic Redis Lua scripts (`ratelimit:active:{user_id}`).
   - Automatically decrements upon document completion or failure.
   - Includes graceful degradation: if Redis is offline, the API falls back to counting active records directly in MongoDB without crashing.
4. **Content-Hash Deduplication Cache**:
   - Keyed by content hash (`cache:{content_hash}`), independent of `document_id`.
   - Identical content across different documents or users completes immediately, bypasses the pipeline, and consumes zero rate-limit slots.
5. **Idempotent Client References (`client_doc_ref`)**:
   - Partial unique MongoDB index on `client_doc_ref` (string type).
   - Same user + same ref + same content $\rightarrow$ `200 OK` (idempotent retry).
   - Same user + same ref + different content $\rightarrow$ `409 Conflict` (prompts `PATCH`).
   - Different user + same ref $\rightarrow$ `409 Conflict` without leaking owner identity.
6. **Caller Identity & Ownership Isolation**:
   - Authenticated via the `X-User-Id` request header.
   - Requests targeting resources owned by other users return `404 Not Found` (never `403 Forbidden`), completely preventing enumeration attacks.

---

## Schema & Staleness Design

In distributed data pipelines where human clients iteratively revise input content while background workers execute asynchronous enrichments, race conditions between writes and completions often result in **split-brain states** or **mixed-version reads** (e.g., tags computed from v1 displayed alongside a summary computed from v2). 

The Cohort Insights API guarantees that **a mixed-version read is impossible by construction, not just unlikely**:

### 1. Document Versioning & Version Stamping
Every document stored in MongoDB maintains:
- `content_version` (integer, starts at 1, incremented atomically upon every `PATCH`).
- `content_hash` (SHA-256 hex digest of the raw content string).
- Derived metadata fields stamped with the exact version from which they were generated:
  - `result.summary` + `result.summary_version`
  - `result.tags` + `result.tags_version`
  - `result.tags_source_summary_version`
- Sub-document per-stage tracking (`stages.processing` and `stages.enriching`) recording:
  - `{ state, attempts, error, started_at, finished_at, for_version, lease_expires_at }`

### 2. Compare-and-Set (CAS) Worker Writes
When a worker claims a document stage via `find_one_and_update`, it records the document's active `content_version` at claim time.
When the worker finishes its computation (e.g. generating the summary), it commits its write using a CAS filter:
```python
filter = {
    "_id": doc_id,
    "content_version": started_version,
    "stages.processing.for_version": started_version,
    "stages.processing.state": "running"
}
```
If a client submitted a `PATCH` while the worker was simulating its 10–20 second run, the document's `content_version` was already bumped to `started_version + 1`. Consequently, MongoDB's atomic filter matches **0 documents**, and the worker's write is safely discarded. The stale computation never overwrites the new version.

### 3. Atomic `PATCH` Resets
When a `PATCH /documents/{document_id}` request is received:
```python
{
    "$set": {
        "content": new_content,
        "content_hash": new_hash,
        "content_version": old_version + 1,
        "status": "queued",
        "stages.processing": create_initial_stage(old_version + 1),
        "stages.enriching": create_initial_stage(old_version + 1),
        "result.summary": None,
        "result.summary_version": None,
        "result.tags": None,
        "result.tags_version": None,
        "result.tags_source_summary_version": None,
        "updated_at": now
    }
}
```
Because MongoDB applies updates to a single document atomically, there is no interim state where content is v2 but derived fields or stage indicators still reference v1.

### 4. Zero-Tolerance Read-Time Invariant
The `GET /documents/{document_id}` endpoint enforces this invariant during response formatting:
```python
is_current = (
    doc["status"] == "completed"
    and doc.get("result", {}).get("summary_version") == doc["content_version"]
    and doc.get("result", {}).get("tags_version") == doc["content_version"]
)

summary = doc["result"]["summary"] if is_current else None
tags = doc["result"]["tags"] if is_current else None
```
Even in hypothetical edge cases where an orphaned process wrote out of sequence, the read path will refuse to surface any derived artifacts unless both `summary_version` and `tags_version` strictly match `content_version`.

---

## Repeat `client_doc_ref` Policy

The `client_doc_ref` parameter enables robust, client-driven idempotency over unreliable networks.

```
                  POST /documents with client_doc_ref
                                  |
                   Does client_doc_ref exist?
                                  |
                +-----------------+-----------------+
                | YES                               | NO
                v                                   v
        Owned by caller?                     Create Document (201)
                |
        +-------+-------+
        | YES           | NO
        v               v
  Same hash?     409 Conflict
        |        (Zero leak: generic ref collision)
    +---+---+
    | YES   | NO
    v       v
 200 OK   409 Conflict (with doc_id; instruct PATCH)
(Idempotent)
```

### Why This Design?
1. **Network Retries & Duplicates**: If a client submits a document, but the connection drops before receiving the HTTP response, the client can safely replay the exact same `POST` request. The API recognizes identical content with the same reference and returns `200 OK` with the existing `document_id`, preventing duplicate pipeline executions and rate-limit penalties.
2. **Out-of-Order Delivery & Accidental Overwrites**: If a client intends to update an existing reference with new content, sending another `POST` indicates an architectural misuse. Returning `409 Conflict` containing `{ document_id }` prevents accidental silent overwrites and explicitly forces the client to use `PATCH /documents/{document_id}`.
3. **Multi-Tenant Security**: If User B attempts to register a `client_doc_ref` that is already claimed by User A, the endpoint responds with `409 Conflict: client_doc_ref is already in use`, withholding User A's identity and document ID.

---

## At 100x Scale (Architectural Deep Dive)

Scaling this system to 100x throughput represents a jump to thousands of concurrent document submissions per second, hundreds of millions of stored documents, and tenants managing upwards of 500,000 documents each. 

### 1. 500K Documents per User and the `user_id` Indexes
A compound index `(user_id, status, created_at desc)` allows single-tenant queries to seek directly to the tenant's index partition and scan in reverse chronological order. For a user with 500K documents, the index subtree for that single `user_id` spans approximately 35–45 MB. As long as working-set memory exceeds active index branches, point queries and bounded range scans remain within 2–5 B-tree hops. However, updates and deletions begin contending on index write locks. To preserve sub-millisecond query latencies at 100x, we must ensure high index locality in the WiredTiger cache and avoid unbounded status scans across large completed sets by keeping high-cardinality compound prefixes.

### 2. Shard Key Choice: Hashed `user_id` vs Compound Key
Choosing a pure `{ _id: "hashed" }` key distributes documents uniformly across cluster shards, but forces every user-scoped query (`GET /users/{id}/documents`) into a cluster-wide scatter-gather operation across all shards, crippling tail latency. 
Conversely, a ranged `{ user_id: 1 }` key directs all queries for a given user to a single shard, but introduces severe write hot-spotting if a single enterprise tenant submits high-volume batches. 
The optimal shard key at 100x is a compound hashed key: `{ user_id: "hashed", _id: 1 }`. This ensures:
- All documents for any single user reside on one shard, enabling fast, localized single-shard queries.
- Users are uniformly hashed across the cluster, preventing chunk imbalance and write hot spots.
- Secondary chunks can be rebalanced cleanly without cross-shard joins.

### 3. Redis Rate-Limiter Counter at 100x Submit QPS
At 100x submit QPS (e.g. 50,000 requests/sec), executing Lua scripts against a single Redis primary becomes a throughput bottleneck due to Redis's single-threaded event loop and network socket overhead. While Lua scripts execute in sub-millisecond time, high burst rates against a single node induce connection queueing. 
To scale this tier:
- Deploy a Redis Cluster sharded by user key (`{user_id}` hash tags), distributing user counters across independent master nodes.
- Introduce an in-memory client-side token bucket or leaky bucket tier at the API layer (e.g. per-pod token caches) to absorb bursts locally before consulting Redis.
- Decouple active lease counts from synchronous Lua locks using an asynchronous event-driven counter synchronized via Redis Streams or Kafka.

### 4. Skip/Limit Pagination vs Keyset/Cursor Pagination
MongoDB's `skip(N).limit(M)` suffers from an $O(N)$ execution cost because the storage engine must traverse and discard $N$ index leaf nodes before reading $M$ documents. For a user page request at offset 450,000, `skip` scans 450,000 index keys, consuming CPU and evicting pages from the WiredTiger cache. 
At 100x, pagination must transition to **keyset (cursor-based) pagination** indexed on `(user_id, created_at, _id)`:
```
WHERE user_id = :uid AND (created_at, _id) < (:last_created_at, :last_id)
ORDER BY created_at DESC, _id DESC
LIMIT :page_size
```
Keyset pagination converts deep pagination into an $O(1)$ index seek, maintaining identical single-millisecond response times regardless of whether the user requests page 1 or page 50,000.

---

## Design Decisions & Assumptions

| Decision | Implementation / Rationale |
| :--- | :--- |
| **Authentication Assumption** | Caller identity is passed via the `X-User-Id` header (e.g. `X-User-Id: user_alice`). In production, this would be replaced with verified JWT claims or mTLS certs. |
| **404 Over 403 on Cross-Tenant Access** | Accessing documents or user endpoints belonging to another user returns `404 Not Found` (never `403 Forbidden`). This prevents malicious callers from enumerating valid document IDs or references. |
| **Configurable Simulation Parameters** | Delays (`SIMULATION_PROCESSING_DELAY_MIN`, etc.) and failure rates (`STAGE_FAILURE_RATE`) are exposed via `pydantic-settings`. In tests, delays and failure rates default to `0.0` for sub-second test suites. |
| **Worker Lease Reclamation** | Atomic leases (`find_one_and_update`) use `lease_expires_at = now + 60s`. If a worker container crashes mid-execution, orphaned jobs are reclaimed automatically by surviving workers. |
| **Graceful Degradation** | If Redis crashes or experiences network isolation, the rate limiter falls back to counting active records directly in MongoDB, and cache checks are safely skipped. The application logs a structured warning and never crashes. |
| **Structured JSON Logging** | Standard Python logging is configured with a JSON formatter emitting timestamps, log level, event names, and context parameters (`user_id`, `document_id`, `stage`). |

---

## What I'd Do With More Time

1. **Dedicated Worker Queue (Redis Streams / RabbitMQ)**: Replace polling `find_one_and_update` queries with persistent event streams and consumer groups for lower latency and zero database polling overhead.
2. **Distributed Tracing (OpenTelemetry)**: Inject traceparent headers through API requests, MongoDB commands, Redis operations, and worker spans for end-to-end request tracing in Jaeger/Honeycomb.
3. **Dead Letter Queue (DLQ) & Exponential Backoff**: Implement automated retries with exponential backoff and jitter for transient worker failures before marking a stage as permanently failed.
4. **WebSocket / SSE Real-Time Updates**: Provide a `/documents/{document_id}/events` Server-Sent Events endpoint allowing client UIs to render live stage progress bars without polling.
5. **Partial Updates / JSON Patch (RFC 6902)**: Support RFC 6902 patch operations allowing fine-grained document updates and title revisions without triggering full reprocessing.

---

## API Reference

### 1. Submit Document
`POST /documents`
- **Headers**: `X-User-Id: <user_id>`
- **Body**:
  ```json
  {
    "title": "Enterprise Retention Analysis",
    "content": "Raw document text containing metrics and cohort observations...",
    "client_doc_ref": "ref_client_abc_123"
  }
  ```
- **Responses**:
  - `201 Created`: Document accepted and enqueued: `{"document_id": "...", "status": "queued"}`. (Or `status: "completed"` if hit via content cache).
  - `200 OK`: Idempotent return of existing document when `client_doc_ref` and `content_hash` match.
  - `409 Conflict`: `client_doc_ref` exists with different content (prompts `PATCH`), or ref is owned by another user.
  - `429 Too Many Requests`: Caller already has 3 documents in `{queued, processing, enriching}`.

### 2. Update Document
`PATCH /documents/{document_id}`
- **Headers**: `X-User-Id: <user_id>`
- **Body**:
  ```json
  {
    "content": "Updated raw document content..."
  }
  ```
- **Responses**:
  - `200 OK`: Content updated, `content_version` bumped, stages reset to queued, derived fields wiped.
  - `404 Not Found`: Document not found or caller is not owner.
  - `409 Conflict`: Optimistic concurrency conflict on concurrent write.

### 3. Get Document
`GET /documents/{document_id}`
- **Headers**: `X-User-Id: <user_id>`
- **Responses**:
  - `200 OK`: Stage-level breakdown and derived summary/tags (present only if `summary_version == tags_version == content_version`).
  - `404 Not Found`: Caller is not owner.

### 4. Get Document by Reference
`GET /documents/by-ref/{client_doc_ref}`
- **Headers**: `X-User-Id: <user_id>`
- **Responses**:
  - `200 OK`: Document details.
  - `404 Not Found`: Reference missing or owned by another user.

### 5. List User Documents (Paginated)
`GET /users/{user_id}/documents?page=1&page_size=10&status=completed`
- **Headers**: `X-User-Id: <user_id>`
- **Responses**:
  - `200 OK`: `{ "items": [...], "total": 42, "page": 1, "page_size": 10, "total_pages": 5 }`.
  - `404 Not Found`: Caller attempting to inspect another user's documents.

### 6. Health Check
`GET /health`
- **Responses**:
  - `200 OK`: `{"status": "healthy", "mongo": "connected", "redis": "connected"}` or `{"status": "degraded", "mongo": "connected", "redis": "disconnected"}`.
  - `503 Service Unavailable`: MongoDB is unreachable.

---

## Local Setup & Testing

### Option A: Running via Docker Compose (Recommended)
Launch the entire system (FastAPI + MongoDB + Redis) with a single command:
```bash
docker compose up --build
```
- API is available at: `http://localhost:8000`
- Interactive Swagger UI: `http://localhost:8000/docs`
- Health check: `http://localhost:8000/health`

### Option B: Running Locally with Python 3.11

1. **Create and Activate Virtual Environment**:
   ```bash
   python -m venv .venv
   # Windows:
   .venv\Scripts\activate
   # Linux/macOS:
   source .venv/bin/activate
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Start the API Server**:
   ```bash
   uvicorn app.main:app --reload --port 8000
   ```

## Testing

The test suite is built on **pytest**, **pytest-asyncio**, and **httpx.AsyncClient** using `ASGITransport`. It runs completely self-contained in both containerized environments (with live MongoDB and Redis instances) and offline developer environments.

### How to Run

1. **Local Test Execution (Sub-5 seconds)**:
   ```bash
   # Via pytest directly
   pytest -v

   # Or via Makefile
   make test
   ```

2. **Docker Compose Test Runner (Against Live Mongo & Redis Containers)**:
   ```bash
   # Executes pytest inside an isolated test container
   docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm test

   # Or via Makefile
   make test-docker
   ```

3. **Manual Smoke-Test Scripts**:
   ```bash
   # Linux / macOS / Git Bash:
   bash scripts/smoke_test.sh

   # Windows PowerShell:
   powershell -File scripts/smoke_test.ps1
   ```

---

### What Each Test File Proves

| File | Test Scenarios & Proved Invariants |
| :--- | :--- |
| **`test_staleness.py`** | **1.** Proves CAS worker writes are discarded when a `PATCH` lands mid-stage.<br>**2.** Proves `GET` returns `summary=null` and reprocessing status until v2 finishes.<br>**3.** Property-style test executing 50 random interleaved PATCH, poll, and worker steps asserting zero mixed-version states on every single read. |
| **`test_pipeline.py`** | **1.** Verifies the complete happy path: `queued -> processing -> enriching -> completed`.<br>**2.** Proves forced enriching failures preserve the generated processing summary without rerunning stage 1.<br>**3.** Proves processing failure prevents enrichment from starting.<br>**4.** Proves `find_one_and_update` atomic stage claiming (two workers race; exactly one wins).<br>**5.** Proves crashed worker jobs are reclaimed once `lease_expires_at` elapses. |
| **`test_crosswalk.py`** | **1.** Verifies `by-ref` lookups and cross-tenant `404` isolation.<br>**2.** Proves repeat ref policies: same user + same content $\rightarrow$ `200 OK`; same user + different content $\rightarrow$ `409 Conflict` with `document_id`; different user $\rightarrow$ `409` with no data leakage.<br>**3.** Proves concurrent duplicate submissions create exactly one document.<br>**4.** Asserts via `explain()` that queries use the `idx_unique_client_doc_ref` (`IXSCAN`, not `COLLSCAN`). |
| **`test_rate_limit.py`** | **1.** Proves 4th active document in `{queued, processing, enriching}` receives `429 Too Many Requests`.<br>**2.** Proves completed/failed documents immediately free active slots.<br>**3.** Proves burst concurrency: 10 concurrent submissions yield exactly 3 successes (201) and 7 rejections (429).<br>**4.** Proves rate limiter gracefully degrades to MongoDB counts when Redis is down. |
| **`test_cache.py`** | **1.** Proves duplicate content hash completes immediately, skips pipeline, and does not consume rate-limit slots.<br>**2.** Proves `PATCH` to new content isolates cache and clears derived fields.<br>**3.** Proves `PATCH` back to original content hits the previous cache entry.<br>**4.** Asserts positive TTL expiration on all Redis cache keys. |
| **`test_ownership.py`** | **1.** Proves non-owner `GET` and `PATCH` requests return `404 Not Found` with response bodies identical to non-existent documents.<br>**2.** Proves list endpoint is strictly scoped to the caller's documents.<br>**3.** Proves malformed ObjectId strings return `404` or `422`, never `500`. |
| **`test_listing.py`** | **1.** Verifies newest-first ordering by `created_at` descending.<br>**2.** Verifies `page` and `page_size` pagination calculations.<br>**3.** Verifies `?status=` query filtering.<br>**4.** Verifies empty page behavior on out-of-bounds requests.<br>**5.** Proves `page_size > 100` upper bound validation triggers `422 Unprocessable Entity`. |
| **`test_validation_and_health.py`** | **1.** Proves empty `title` or `content` returns `422`.<br>**2.** Proves oversized content (>500,000 characters) returns `422`.<br>**3.** Proves missing `X-User-Id` header returns `400/401/422`.<br>**4.** Proves `/health` returns 200 `healthy`, 200 `degraded` when Redis is down, and 503 `unhealthy` when MongoDB is down. |
| **`test_concurrent_patch.py`** | **1.** Simulates simultaneous competing `PATCH` requests on the same document.<br>**2.** Asserts the defined outcome: optimistic concurrency allows the winning write (`200 OK`) and returns `409 Conflict` to the conflicting write (or sequential increments), preventing lost updates and corruption. |

---

### What Isn't Covered

1. **Jepsen-Style Physical Network Partitions**: Testing distributed MongoDB Replica Set elections or Redis Sentinel split-brain failovers during physical network cuts.
2. **True LLM Non-Determinism**: Real LLM API latency jitter, rate limits (e.g. OpenAI 429 TPM/RPM), and prompt degradation (intentionally simulated via deterministic mock functions).
3. **Multi-Region Cross-Cluster Replication**: Latency delays associated with bi-directional multi-region active-active database replication.

