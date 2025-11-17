# SPY History Features Lambda

AWS Lambda function that computes rolling history-based features for SPY options data. This Lambda enriches snapshot features with lookback features computed from a rolling 5-hour window of historical data.

## Overview

This Lambda processes 1-minute options data and adds 93 time-series features across 5 sections:
- **Section 2.1**: Underlying stock price lookback (15 features)
- **Section 2.2**: ATM node lookback (12 features)
- **Section 3**: Relative moneyness node features (35 features)
- **Section 4**: Cross-sectional dynamics (9 features)
- **Section 5**: Per-contract short history (22 features)

## Architecture

```
S3 (spy-no-history-features)
    ↓
Lambda (spy-history-features)
    ├── HistoryManager (300-minute rolling window)
    ├── FeatureEngine (orchestrates feature computation)
    └── FeatureRegistry (manages feature sections)
    ↓
S3 (spy-with-history-features)
```

## Features

### Key Capabilities
- **Temporal validation**: Prevents look-ahead bias with strict chronological ordering
- **Cross-date history**: Automatically loads historical context from previous trading days
- **Dual modes**: Batch processing for bulk data, live mode for real-time API responses
- **S3 indexing**: Efficiently discovers available historical files across dates
- **Feature versioning**: SHA256 hash of enabled features for reproducibility

### Processing Modes

**Batch Mode**: Process multiple files sequentially, write to S3
```json
{
  "mode": "batch",
  "s3_uris": ["s3://spy-no-history-features/one-minute/20250325/strikes_202503250930.parquet", ...]
}
```

**Live Mode**: Process single file, return JSON response
```json
{
  "mode": "live",
  "s3_uris": ["s3://spy-no-history-features/one-minute/20250325/strikes_202503250935.parquet"]
}
```

## Project Structure

```
spy_history_features_lambda/
├── features/                    # Feature computation modules
│   ├── __init__.py             # Registry initialization
│   ├── registry.py             # Feature registry and base classes
│   ├── section_underlying.py  # Section 2.1: Underlying features
│   ├── section_atm.py          # Section 2.2: ATM features
│   ├── section_offset.py       # Section 3: Offset features
│   ├── section_cross_sectional.py  # Section 4: Cross-sectional features
│   └── section_contract.py     # Section 5: Contract features
├── events/                      # Sample Lambda events
│   ├── live_mode_event.json
│   ├── batch_mode_event.json
│   └── README.md
├── handler.py                   # Main Lambda handler
├── history_manager.py           # Rolling window manager
├── feature_engine.py            # Feature orchestration
├── s3_manager.py                # S3 operations
├── config.py                    # Configuration management
├── utils.py                     # Utility functions
├── batch_processor.py           # Batch processing script
├── lambda_client.py             # Lambda invocation client
├── date_thread.py               # Multi-threaded date processing
├── template.yaml                # SAM template
├── requirements.txt             # Python dependencies
├── build.ps1                    # Build script
├── deploy.ps1                   # Deployment script
├── verify_features.py           # Feature verification script
├── FEATURE_SECTIONS_SUMMARY.md  # Feature documentation
└── NaN_BEHAVIOR.md              # NaN value explanation
```

## Setup

### Prerequisites
- Python 3.11+
- AWS CLI configured
- AWS SAM CLI
- IAM role with S3 and Lambda permissions

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Adzala/spy_history_features_lambda.git
cd spy_history_features_lambda
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
```bash
aws configure
```

## Deployment

### Build and Deploy

```powershell
# Build the Lambda
.\build.ps1

# Deploy to AWS
.\deploy.ps1
```

The deployment creates:
- Lambda function: `spy-history-features`
- CloudFormation stack: `spy-history-features`
- Memory: 1024 MB
- Timeout: 15 minutes
- Concurrency: 40

### Configuration

Environment variables (set in `template.yaml`):
- `SOURCE_BUCKET`: Input S3 bucket (default: `spy-no-history-features`)
- `DEST_BUCKET`: Output S3 bucket (default: `spy-with-history-features`)
- `HISTORY_WINDOW_SIZE`: Rolling window size in minutes (default: 300)
- `NUMERIC_PRECISION`: Decimal places for rounding (default: 4)
- `TIMEOUT_BUFFER_SECONDS`: Safety buffer before Lambda timeout (default: 5)

## Usage

### Test Locally

```bash
# Test with SAM local
sam local invoke HistoryFeaturesFunction -e events/live_mode_event.json
```

### Invoke Deployed Lambda

```bash
# Live mode
aws lambda invoke \
  --function-name spy-history-features \
  --payload file://events/live_mode_event.json \
  response.json

# Batch mode
aws lambda invoke \
  --function-name spy-history-features \
  --payload file://events/batch_mode_event.json \
  response.json
```

### Batch Processing Script

Process all files for a date range:
```bash
python batch_processor.py
```

### Verify Features

List all registered features and compute version hash:
```bash
python verify_features.py
```

## Feature Computation

### Historical Context Loading

The Lambda automatically loads historical context from S3:
1. Lists available files across multiple dates (up to 5 days back)
2. Builds an index of files by timestamp
3. Loads up to 60 most recent files before current timestamp
4. Treats data as continuous time series across dates

### Feature Sections

Each section is independently registered and can be enabled/disabled:

```python
from features import create_default_registry

registry = create_default_registry()
registry.disable_section('section_contract')  # Disable if needed
```

### NaN Handling

Features return NaN when insufficient historical data is available. See [NaN_BEHAVIOR.md](NaN_BEHAVIOR.md) for details.

## Monitoring

### CloudWatch Logs

```bash
aws logs tail /aws/lambda/spy-history-features --follow
```

### Metrics

Key metrics to monitor:
- Duration
- Memory usage
- Error rate
- Throttles

## Development

### Adding New Features

1. Create a new section class inheriting from `FeatureSection`
2. Implement `feature_names` property and `compute` method
3. Register in `features/__init__.py`

Example:
```python
class SectionNewFeatures(FeatureSection):
    @property
    def feature_names(self) -> list:
        return ['NewFeature_L1', 'NewFeature_L5']
    
    def compute(self, df: pd.DataFrame, history_mgr: HistoryManager, **kwargs) -> pd.DataFrame:
        # Compute features
        return df
```

### Running Tests

```bash
# Verify all features are registered
python verify_features.py

# Test with sample event
sam local invoke -e events/live_mode_event.json
```

## Performance

- **Processing time**: ~1-2 seconds per file (with history loading)
- **Memory usage**: ~500-700 MB (with 300-minute window)
- **Throughput**: 40 concurrent executions
- **History loading**: ~100-200ms for 60 files

## Troubleshooting

### NaN Values in Features

See [NaN_BEHAVIOR.md](NaN_BEHAVIOR.md) for detailed explanation. Common causes:
- Processing files shortly after market open
- Insufficient historical data
- Missing historical files in S3

### Lambda Timeout

Increase timeout in `template.yaml` or reduce `HISTORY_WINDOW_SIZE`.

### Memory Issues

Increase memory allocation in `template.yaml` or reduce `HISTORY_WINDOW_SIZE`.

## License

MIT

## Contact

For questions or issues, please open a GitHub issue.
