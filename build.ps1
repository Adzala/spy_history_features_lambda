# Build Script for SPY History Features Lambda
# Installs dependencies and prepares deployment package

$ErrorActionPreference = "Stop"

$separator = "=" * 80

Write-Host $separator -ForegroundColor Cyan
Write-Host "SPY History Features Lambda - Build" -ForegroundColor Cyan
Write-Host $separator -ForegroundColor Cyan
Write-Host ""

# Step 1: Clean previous build
Write-Host "[1/2] Cleaning previous build..." -ForegroundColor Yellow
if (Test-Path ".aws-sam") {
    Remove-Item -Recurse -Force ".aws-sam"
    Write-Host "Removed .aws-sam directory" -ForegroundColor Gray
}
Write-Host ""

# Step 2: Build with SAM
Write-Host "[2/2] Building SAM application..." -ForegroundColor Yellow
Write-Host ""

sam build --template-file template.yaml

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "ERROR: SAM build failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host $separator -ForegroundColor Cyan
Write-Host "Build Complete!" -ForegroundColor Green
Write-Host $separator -ForegroundColor Cyan
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "  Run: .\deploy.ps1" -ForegroundColor White
Write-Host ""
