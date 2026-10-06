<#
.SYNOPSIS
    PHASE 03 smoke test for the local container foundation (PostgreSQL + MinIO).

.DESCRIPTION
    Read-only verification of the running stack. It does NOT create any database
    schema, bucket or project data. It checks:
      1. the Compose file renders (docker compose config)
      2. both containers are running and healthy
      3. PostgreSQL accepts a connection and returns SELECT 1
      4. MinIO liveness/readiness endpoints answer 200
      5. service-to-service DNS resolution (postgres <-> minio)

    Credentials are read from the Compose-managed environment only; no secret is
    printed.

.EXAMPLE
    pwsh -File infrastructure/scripts/smoke-test.ps1
#>
[CmdletBinding()]
param(
    [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$composeFile = Join-Path $repoRoot "infrastructure\docker\docker-compose.yml"
$envPath = Join-Path $repoRoot $EnvFile

$dc = @("compose", "--env-file", $envPath, "-f", $composeFile)
$failures = 0

function Step([string]$message) { Write-Host "==> $message" -ForegroundColor Cyan }
function Ok([string]$message) { Write-Host "    OK  $message" -ForegroundColor Green }
function Fail([string]$message) { Write-Host "    FAIL $message" -ForegroundColor Red; $script:failures++ }

if (-not (Test-Path -LiteralPath $envPath)) {
    Write-Host "Missing environment file: $envPath" -ForegroundColor Red
    Write-Host "Copy .env.example to .env and fill in local values first."
    exit 2
}

Step "Rendering Compose configuration"
docker @dc config *> $null
if ($LASTEXITCODE -eq 0) { Ok "docker compose config is valid" }
else { Fail "docker compose config failed" }

Step "Container health"
foreach ($name in @("mri-postgres", "mri-minio")) {
    $state = docker inspect --format "{{.State.Status}}" $name 2>$null
    $health = docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}" $name 2>&1
    if ($LASTEXITCODE -ne 0) { Fail "$name is not present"; continue }
    if ($state -ne "running") { Fail "$name is '$state' (expected running)"; continue }
    if ($health -eq "healthy") { Ok "$name is running and healthy" }
    else { Fail "$name health is '$health' (expected healthy)" }
}

if ($failures -eq 0) {
    Step "PostgreSQL connection + SELECT 1"
    $result = (docker exec mri-postgres psql -U retail_app -d retail -tAc "SELECT 1;") 2>&1
    if ("$LASTEXITCODE" -eq "0" -and ("$result".Trim() -eq "1")) { Ok "SELECT 1 returned 1" }
    else { Fail "PostgreSQL query failed: $result" }

    Step "MinIO health endpoints"
    foreach ($ep in @("live", "ready")) {
        $code = curl.exe -s -o NUL -w "%{http_code}" "http://localhost:9000/minio/health/$ep"
        if ($code -eq "200") { Ok "/minio/health/$ep -> 200" } else { Fail "/minio/health/$ep -> $code" }
    }

    Step "Service-to-service DNS"
    docker exec mri-postgres getent hosts minio *> $null
    if ($LASTEXITCODE -eq 0) { Ok "postgres resolves 'minio'" } else { Fail "postgres cannot resolve 'minio'" }
    docker exec mri-minio getent hosts postgres *> $null
    if ($LASTEXITCODE -eq 0) { Ok "minio resolves 'postgres'" } else { Fail "minio cannot resolve 'postgres'" }
}

if ($failures -gt 0) {
    Write-Host ""
    Write-Host "SMOKE TEST FAILED ($failures check(s))." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "SMOKE TEST PASSED - PostgreSQL and MinIO containers are healthy." -ForegroundColor Green
exit 0