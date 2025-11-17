# Deploy Script for SPY History Features Lambda
# Deploys the Lambda function to AWS

$ErrorActionPreference = "Stop"

$separator = "=" * 80
$stackName = "spy-history-features"
$region = "us-east-1"

Write-Host $separator -ForegroundColor Cyan
Write-Host "SPY History Features Lambda - Deploy" -ForegroundColor Cyan
Write-Host $separator -ForegroundColor Cyan
Write-Host ""

# Check if build exists
if (-not (Test-Path ".aws-sam/build/template.yaml")) {
    Write-Host "Build not found. Running build first..." -ForegroundColor Yellow
    Write-Host ""
    .\build.ps1
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "ERROR: Build failed" -ForegroundColor Red
        exit 1
    }
}

# Deploy
Write-Host "Deploying to AWS..." -ForegroundColor Yellow
Write-Host "  Stack Name: $stackName" -ForegroundColor Gray
Write-Host "  Region: $region" -ForegroundColor Gray
Write-Host "  IAM Role: Admin-Aum" -ForegroundColor Gray
Write-Host ""

sam deploy `
    --template-file .aws-sam/build/template.yaml `
    --stack-name $stackName `
    --capabilities CAPABILITY_IAM `
    --region $region `
    --parameter-overrides "SourceBucket=spy-no-history-features DestBucket=spy-with-history-features" `
    --resolve-s3 `
    --no-confirm-changeset

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "ERROR: SAM deploy failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host $separator -ForegroundColor Cyan
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host $separator -ForegroundColor Cyan
Write-Host ""

# Get stack outputs
Write-Host "Retrieving stack outputs..." -ForegroundColor Cyan
$outputs = aws cloudformation describe-stacks `
    --stack-name $stackName `
    --region $region `
    --query "Stacks[0].Outputs" `
    --output json | ConvertFrom-Json

Write-Host ""
Write-Host "Stack Outputs:" -ForegroundColor Cyan
foreach ($output in $outputs) {
    Write-Host "  $($output.OutputKey): $($output.OutputValue)" -ForegroundColor White
}
Write-Host ""

# Get function ARN specifically
$functionArn = ($outputs | Where-Object { $_.OutputKey -eq "FunctionArn" }).OutputValue
Write-Host "Lambda Function ARN:" -ForegroundColor Yellow
Write-Host "  $functionArn" -ForegroundColor Green
Write-Host ""

Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Test Lambda: aws lambda invoke --function-name spy-history-features --payload file://test-event.json response.json" -ForegroundColor White
Write-Host "  2. View logs: aws logs tail /aws/lambda/spy-history-features --region $region --follow" -ForegroundColor White
Write-Host "  3. Run batch: python batch_processor.py" -ForegroundColor White
Write-Host ""
