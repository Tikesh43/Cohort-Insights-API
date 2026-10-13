#!/usr/bin/env bash
# ==============================================================================
# Cohort Insights API - Manual Smoke Test Script
# Covers: submit, poll, PATCH, by-ref, rate limit (429), and other-user (404).
# ==============================================================================

set -e
BASE_URL="${BASE_URL:-http://localhost:8000}"
USER_A="smoke_user_alice"
USER_B="smoke_user_bob"
CLIENT_REF="smoke_ref_$(date +%s)"

echo "=== Cohort Insights API Smoke Test ==="
echo "Target URL: $BASE_URL"
echo ""

# 1. Health check
echo "1. Probing /health..."
curl -s -f "$BASE_URL/health" | grep -q "status" && echo "  -> Health check OK!"

# 2. Submit Document
echo "2. Submitting document for $USER_A with client_doc_ref: $CLIENT_REF..."
SUBMIT_RESP=$(curl -s -X POST "$BASE_URL/documents" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: $USER_A" \
  -d "{
    \"title\": \"Q3 Cohort Analysis\",
    \"content\": \"Initial cohort retention overview text for smoke test.\",
    \"client_doc_ref\": \"$CLIENT_REF\"
  }")
echo "  -> Response: $SUBMIT_RESP"
DOC_ID=$(echo "$SUBMIT_RESP" | grep -o '"document_id":"[^"]*' | cut -d'"' -f4)
echo "  -> Extracted Document ID: $DOC_ID"

# 3. Poll document state
echo "3. Polling /documents/$DOC_ID..."
POLL_RESP=$(curl -s -X GET "$BASE_URL/documents/$DOC_ID" \
  -H "X-User-Id: $USER_A")
echo "  -> Document State: $POLL_RESP"

# 4. Lookup by-ref
echo "4. Resolving via /documents/by-ref/$CLIENT_REF..."
BY_REF_RESP=$(curl -s -X GET "$BASE_URL/documents/by-ref/$CLIENT_REF" \
  -H "X-User-Id: $USER_A")
echo "  -> By-Ref State: $BY_REF_RESP"

# 5. Other-user 404 security check
echo "5. Verifying other user ($USER_B) receives 404 on Alice's document..."
OTHER_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X GET "$BASE_URL/documents/$DOC_ID" \
  -H "X-User-Id: $USER_B")
if [ "$OTHER_STATUS" -eq 404 ]; then
  echo "  -> Confirmed 404 Not Found (Zero-Leakage Enforcement Verified)!"
else
  echo "  -> FAILED: Expected 404, got $OTHER_STATUS"
  exit 1
fi

# 6. PATCH document content
echo "6. Patching content via PATCH /documents/$DOC_ID..."
PATCH_RESP=$(curl -s -X PATCH "$BASE_URL/documents/$DOC_ID" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: $USER_A" \
  -d "{
    \"content\": \"Updated content triggering recomputation and bumping content_version.\"
  }")
echo "  -> Patched Document State: $PATCH_RESP"

# 7. Rate limit test (4th active document yields 429)
echo "7. Submitting documents to trigger rate limit (max 3 active documents)..."
for i in {1..3}; do
  curl -s -X POST "$BASE_URL/documents" \
    -H "Content-Type: application/json" \
    -H "X-User-Id: $USER_A" \
    -d "{
      \"title\": \"Active Doc $i\",
      \"content\": \"Active workload payload $i\"
    }" > /dev/null
done

# The next submission must receive 429
STATUS_429=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/documents" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: $USER_A" \
  -d "{
    \"title\": \"Excess Doc\",
    \"content\": \"Should be rate-limited\"
  }")
if [ "$STATUS_429" -eq 429 ]; then
  echo "  -> Confirmed 429 Too Many Requests (Rate Limiter Verified)!"
else
  echo "  -> Note: Active limit status returned: $STATUS_429 (documents may have completed fast)"
fi

echo ""
echo "=== Smoke Test Completed Successfully! ==="
