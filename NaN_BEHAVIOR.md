# Understanding NaN Values in History Features

## Why Do I Get NaN Values?

NaN (Not a Number) values appear in lookback features when there is **insufficient historical data** to compute the feature. This is expected and by design.

## Root Cause Analysis

### Your Specific Case (0935 file)

When processing `strikes_202503250935.parquet`:
- **Current time**: 09:35 AM (5 minutes after market open at 09:30)
- **Available history**: Only 5 minutes (0930, 0931, 0932, 0933, 0934)
- **Required for 15-min features**: 15 minutes of history
- **Result**: 15-minute and 30-minute features return NaN

### What's Working vs. What's NaN

**✅ Features with values (5-minute window or less):**
- `ATM_CallIVChange_L5`: 0.0018 ✓
- `ATM_CallIVZ_5`: 0.4906 ✓
- `UnderlyingReturn_L5`: -0.0005 ✓
- `UnderlyingSMA_5`: 575.3383 ✓
- `CallMidReturn_L1`, `CallMidReturn_L2`, `CallMidReturn_L3`: ✓

**❌ Features with NaN (15+ minute window):**
- `ATM_CallIVChange_L15`: NaN (needs 15 minutes)
- `ATM_CallIVZ_15`: NaN (needs 15 minutes)
- `ATM_CallIVZ_30`: NaN (needs 30 minutes)
- `UnderlyingReturn_L15`: NaN (needs 15 minutes)
- `UnderlyingSMA_15`, `UnderlyingSMA_30`: NaN

## When Will Features Have Values?

| Time After Market Open | Available Features |
|------------------------|-------------------|
| 09:30 - 09:34 (0-4 min) | Only L1 features (1-minute lag) |
| 09:35 - 09:44 (5-14 min) | L1, L5, and 5-minute window features |
| 09:45 - 09:59 (15-29 min) | L1, L5, L15, and 5/15-minute window features |
| 10:00+ (30+ min) | All features including 30-minute windows |

## Feature Window Requirements

### Section 2.1: Underlying Features
- **L1, L5, L15**: Requires 1, 5, 15 minutes respectively
- **5/15/30-minute windows**: Requires 5, 15, 30 minutes respectively

### Section 2.2: ATM Features
- **L1, L5, L15**: Requires 1, 5, 15 minutes respectively
- **Z-scores (5/15/30)**: Requires 5, 15, 30 minutes respectively

### Section 3: Offset Features
- **L1, L5**: Requires 1, 5 minutes respectively
- **Z-scores (5/15)**: Requires 5, 15 minutes respectively

### Section 4: Cross-Sectional Features
- **L5, L15**: Requires 5, 15 minutes respectively
- **SMA (15/30)**: Requires 15, 30 minutes respectively

### Section 5: Contract Features
- **L1, L2, L3**: Requires 1, 2, 3 minutes respectively
- **Z-scores (3/5)**: Requires 3, 5 minutes respectively

## Solutions

### 1. Wait for More Data (Recommended)
Process files later in the trading day when full history is available:
```json
{
  "mode": "live",
  "s3_uris": [
    "s3://spy-no-history-features/one-minute/20250325/strikes_202503251000.parquet"
  ]
}
```
At 10:00 AM (30 minutes after open), all features will have values.

### 2. Use Batch Mode with Sequential Files
Process multiple consecutive files so history builds up:
```json
{
  "mode": "batch",
  "s3_uris": [
    "s3://spy-no-history-features/one-minute/20250325/strikes_202503250930.parquet",
    "s3://spy-no-history-features/one-minute/20250325/strikes_202503250931.parquet",
    ...
    "s3://spy-no-history-features/one-minute/20250325/strikes_202503251000.parquet"
  ]
}
```
By the time you reach 1000, all features will be populated.

### 3. Accept NaN for Early Morning Data
This is normal behavior. Your ML models should handle NaN values appropriately:
- Drop rows with NaN
- Impute with default values
- Use models that handle missing data (e.g., XGBoost, LightGBM)

## Checking History in Response

The live mode response now includes `history_minutes_loaded` in metadata:

```json
{
  "metadata": {
    "processing_time_ms": 1234,
    "feature_count": 120,
    "row_count": 45,
    "date": "20250325",
    "minute": "0935",
    "stock_price": 595.42,
    "feature_version_hash": "0613f3aadd0197063",
    "history_minutes_loaded": 5
  }
}
```

If `history_minutes_loaded < 30`, expect NaN values for 30-minute window features.

## Historical Context Loading

The Lambda automatically attempts to load up to **60 minutes** of historical context from S3 before processing:

1. **Live mode**: Always loads history from S3
2. **Batch mode**: Loads history for first file only, then uses in-memory queue

However, if files don't exist (e.g., before market open), they are skipped gracefully.

## Summary

**NaN values are expected and correct behavior** when processing files shortly after market open or when historical data is unavailable. The system is working as designed - it's just that you need more historical minutes for longer-window features to compute.
