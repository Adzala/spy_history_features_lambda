# Lambda Test Events

This directory contains sample event JSON files for testing the SPY History Features Lambda.

## Event Files

### 1. `live_mode_event.json`
Tests **live mode** processing with a single file.

**Usage:**
```bash
# Test locally with SAM
sam local invoke HistoryFeaturesFunction -e events/live_mode_event.json

# Test deployed Lambda
aws lambda invoke \
  --function-name spy-history-features-HistoryFeaturesFunction-XXXXX \
  --payload file://events/live_mode_event.json \
  --region us-east-1 \
  response.json
```

**Expected Response:**
```json
{
  "data": [
    {
      "expirDate": "2025-03-28",
      "strike": 590.0,
      "CallMid": 5.25,
      "UnderlyingReturn_L1": 0.0012,
      "ATM_CallIVChange_L1": 0.0015,
      ...
    }
  ],
  "metadata": {
    "processing_time_ms": 1234,
    "feature_count": 120,
    "row_count": 45,
    "date": "20250325",
    "minute": "0935",
    "stock_price": 595.42,
    "feature_version_hash": "0613f3aadd0197063"
  }
}
```

### 2. `batch_mode_event.json`
Tests **batch mode** processing with multiple files (6 consecutive minutes).

**Usage:**
```bash
# Test locally with SAM
sam local invoke HistoryFeaturesFunction -e events/batch_mode_event.json

# Test deployed Lambda
aws lambda invoke \
  --function-name spy-history-features-HistoryFeaturesFunction-XXXXX \
  --payload file://events/batch_mode_event.json \
  --region us-east-1 \
  response.json
```

**Expected Response:**
```json
{
  "success_count": 6,
  "failure_count": 0,
  "failures": [],
  "unprocessed_files": []
}
```

## Event Structure

### Required Fields

- **`mode`**: Processing mode
  - `"live"`: Process single file, return JSON response (no S3 write)
  - `"batch"`: Process multiple files, write to S3 with tags
  
- **`s3_uris`**: Array of S3 URIs to process
  - Live mode: Must contain exactly 1 URI
  - Batch mode: Can contain multiple URIs

### Example Event Structure

```json
{
  "mode": "live",
  "s3_uris": [
    "s3://spy-no-history-features/one-minute/YYYYMMDD/strikes_YYYYMMDDHHMM.parquet"
  ]
}
```

## Customizing Events

To test with different files:

1. List available files in S3:
```bash
aws s3 ls s3://spy-no-history-features/one-minute/20250325/
```

2. Update the `s3_uris` array with the desired file paths (format: `s3://spy-no-history-features/one-minute/YYYYMMDD/strikes_YYYYMMDDHHMM.parquet`)

3. Ensure files are in chronological order for batch mode (the HistoryManager validates temporal ordering)

## Notes

- **Live mode** is designed for real-time API responses (e.g., from API Gateway)
  - Preloads up to 60 minutes of historical context from S3 before processing
  - Returns JSON response immediately without writing to S3
  
- **Batch mode** is designed for bulk processing (e.g., from Step Functions or EventBridge)
  - Preloads historical context for the first file only
  - Subsequent files use the rolling in-memory queue (FIFO with automatic eviction)
  - Writes processed files to destination S3 bucket with feature version tags
  
- Files must be processed in chronological order to maintain history integrity
- The Lambda has a 300-minute (5-hour) window size for historical features
- Historical context loading attempts to load up to 60 minutes before the current file
- Feature version hash `0613f3aadd0197063` includes all 93 features across 5 sections
