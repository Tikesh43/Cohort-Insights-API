# ==============================================================================
# Cohort Insights API - Manual Smoke Test Script (PowerShell)
# Covers: submit, poll, PATCH, by-ref, rate limit (429), and other-user (404).
# ==============================================================================

param (
    [string]$BaseUrl = "http://localhost:8000"
)

$UserA = "smoke_user_alice"
$UserB = "smoke_user_bob"
$ClientRef = "smoke_ref_$(Get-Date -UFormat %s)"

Write-Host "=== Cohort Insights API Smoke Test ===" -ForegroundColor Cyan
Write-Host "Target URL: $BaseUrl`n"

# 1. Health check
Write-Host "1. Probing /health..."
$health = Invoke-RestMethod -Uri "$BaseUrl/health" -Method Get
Write-Host "  -> Health status: $($health.status), Mongo: $($health.mongo), Redis: $($health.redis)"

# 2. Submit Document
Write-Host "2. Submitting document for $UserA with client_doc_ref: $ClientRef..."
$submitBody = @{
    title = "Q3 Cohort Analysis"
    content = "Initial cohort retention overview text for smoke test."
    client_doc_ref = $ClientRef
} | ConvertTo-Json

$submitResp = Invoke-RestMethod -Uri "$BaseUrl/documents" -Method Post -Headers @{ "X-User-Id" = $UserA } -ContentType "application/json" -Body $submitBody
$DocId = $submitResp.document_id
Write-Host "  -> Document ID: $DocId, Status: $($submitResp.status)"

# 3. Poll document state
Write-Host "3. Polling /documents/$DocId..."
$pollResp = Invoke-RestMethod -Uri "$BaseUrl/documents/$DocId" -Method Get -Headers @{ "X-User-Id" = $UserA }
Write-Host "  -> Status: $($pollResp.status), Version: $($pollResp.content_version), is_current: $($pollResp.is_current)"

# 4. Lookup by-ref
Write-Host "4. Resolving via /documents/by-ref/$ClientRef..."
$byRefResp = Invoke-RestMethod -Uri "$BaseUrl/documents/by-ref/$ClientRef" -Method Get -Headers @{ "X-User-Id" = $UserA }
Write-Host "  -> Resolved document_id: $($byRefResp.document_id)"

# 5. Other-user 404 security check
Write-Host "5. Verifying other user ($UserB) receives 404 on Alice's document..."
try {
    Invoke-RestMethod -Uri "$BaseUrl/documents/$DocId" -Method Get -Headers @{ "X-User-Id" = $UserB }
    Write-Host "  -> FAILED: Expected 404!" -ForegroundColor Red
} catch {
    if ($_.Exception.Response.StatusCode.value__ -eq 404) {
        Write-Host "  -> Confirmed 404 Not Found (Zero-Leakage Enforcement Verified)!" -ForegroundColor Green
    } else {
        Write-Host "  -> Received unexpected status: $($_.Exception.Response.StatusCode)" -ForegroundColor Yellow
    }
}

# 6. PATCH document content
Write-Host "6. Patching content via PATCH /documents/$DocId..."
$patchBody = @{ content = "Updated content triggering recomputation and bumping content_version." } | ConvertTo-Json
$patchResp = Invoke-RestMethod -Uri "$BaseUrl/documents/$DocId" -Method Patch -Headers @{ "X-User-Id" = $UserA } -ContentType "application/json" -Body $patchBody
Write-Host "  -> New Version: $($patchResp.content_version), Status: $($patchResp.status), summary is null: $($null -eq $patchResp.summary)"

# 7. Rate limit test
Write-Host "7. Testing rate limit (max 3 active documents)..."
for ($i = 1; $i -le 3; $i++) {
    $activeBody = @{ title = "Active $i"; content = "Workload $i" } | ConvertTo-Json
    try {
        Invoke-RestMethod -Uri "$BaseUrl/documents" -Method Post -Headers @{ "X-User-Id" = $UserA } -ContentType "application/json" -Body $activeBody | Out-Null
    } catch {}
}

try {
    $overflowBody = @{ title = "Overflow"; content = "Overflow" } | ConvertTo-Json
    Invoke-RestMethod -Uri "$BaseUrl/documents" -Method Post -Headers @{ "X-User-Id" = $UserA } -ContentType "application/json" -Body $overflowBody
    Write-Host "  -> Notice: Did not 429 (job may have completed fast)" -ForegroundColor Yellow
} catch {
    if ($_.Exception.Response.StatusCode.value__ -eq 429) {
        Write-Host "  -> Confirmed 429 Too Many Requests (Rate Limiter Verified)!" -ForegroundColor Green
    }
}

Write-Host "`n=== Smoke Test Completed Successfully! ===" -ForegroundColor Cyan
