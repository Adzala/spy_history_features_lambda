"""Main Lambda handler for SPY History Features Lambda."""

import logging
import time
import traceback
from datetime import datetime
from typing import Dict, Any
import pandas as pd

from config import Config
from s3_manager import S3Manager
from history_manager import HistoryManager
from feature_engine import FeatureEngine
from features import create_default_registry
from utils import parse_cache_key

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler supporting both batch and live modes.
    
    Event Structure:
    {
        "s3_uris": ["s3://...", "s3://..."],
        "mode": "batch" | "live"  # default: "batch"
    }
    
    Args:
        event: Lambda event containing s3_uris and mode
        context: Lambda context object
    
    Returns:
        Batch mode: {success_count, failure_count, failures, unprocessed_files}
        Live mode: {data, metadata}
    """
    start_time = time.time()
    
    try:
        # Parse event
        mode = event.get('mode', 'batch')
        s3_uris = event.get('s3_uris', [])
        
        logger.info(f"Lambda handler invoked: mode={mode}, uris={len(s3_uris)}")
        
        # Validate input
        if not s3_uris:
            raise ValueError("No S3 URIs provided in event")
        
        if mode not in ['batch', 'live']:
            raise ValueError(f"Invalid mode: {mode}. Must be 'batch' or 'live'")
        
        if mode == 'live' and len(s3_uris) != 1:
            raise ValueError(f"Live mode requires exactly 1 S3 URI, got {len(s3_uris)}")
        
        # Validate configuration
        Config.validate()
        
        # Initialize components
        logger.info("Initializing components...")
        s3_mgr = S3Manager()
        history_mgr = HistoryManager(window_size=Config.get_history_window_size())
        registry = create_default_registry()
        engine = FeatureEngine(registry)
        
        logger.info(f"HistoryManager initialized: window_size={Config.get_history_window_size()}")
        
        # Calculate timeout threshold (Lambda timeout - buffer)
        timeout_buffer = Config.get_timeout_buffer()
        remaining_time_ms = context.get_remaining_time_in_millis()
        timeout_threshold = (remaining_time_ms / 1000) - timeout_buffer
        
        logger.info(f"Timeout threshold: {timeout_threshold:.1f}s (buffer: {timeout_buffer}s)")
        
        # Initialize results tracking
        results = {
            'success_count': 0,
            'failure_count': 0,
            'failures': [],
            'unprocessed_files': []
        }
        
        # Process files
        for idx, uri in enumerate(s3_uris):
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout_threshold:
                logger.warning(f"Approaching timeout at {elapsed:.1f}s, stopping processing")
                results['unprocessed_files'] = s3_uris[idx:]
                break
            
            try:
                logger.info(f"Processing file {idx + 1}/{len(s3_uris)}: {uri}")
                
                # Process single file
                result = process_file(uri, engine, s3_mgr, history_mgr, mode, start_time)
                
                # Live mode: return immediately
                if mode == 'live':
                    return result
                
                # Batch mode: track success
                results['success_count'] += 1
                logger.info(f"✓ Successfully processed: {uri}")
                
            except Exception as e:
                # Log error with full traceback
                logger.error(f"Failed to process {uri}: {e}")
                logger.error(traceback.format_exc())
                
                results['failure_count'] += 1
                results['failures'].append({
                    'uri': uri,
                    'error': str(e),
                    'error_type': type(e).__name__
                })
                
                # Continue processing remaining files in batch mode
                continue
        
        # Log final statistics
        total_elapsed = time.time() - start_time
        logger.info(f"Processing complete: {results['success_count']} succeeded, "
                   f"{results['failure_count']} failed, "
                   f"{len(results['unprocessed_files'])} unprocessed, "
                   f"elapsed={total_elapsed:.2f}s")
        
        return results
        
    except Exception as e:
        # Fatal error - log and return error response
        logger.error(f"Fatal error in lambda_handler: {e}")
        logger.error(traceback.format_exc())
        
        return {
            'success_count': 0,
            'failure_count': len(event.get('s3_uris', [])),
            'failures': [{
                'error': str(e),
                'error_type': type(e).__name__,
                'message': 'Fatal error in handler initialization'
            }],
            'unprocessed_files': event.get('s3_uris', [])
        }


def load_historical_context(uri: str, s3_mgr: S3Manager, history_mgr: HistoryManager, 
                           max_history_minutes: int = 60) -> None:
    """
    Load historical files from S3 to populate the history manager.
    
    Uses S3 listing to find available files across dates, treating history as continuous.
    Loads up to max_history_minutes of data before the current file's timestamp.
    
    Args:
        uri: Current file S3 URI
        s3_mgr: S3Manager instance
        history_mgr: HistoryManager instance
        max_history_minutes: Maximum number of historical minutes to load
    """
    import boto3
    from datetime import datetime, timedelta
    
    # Extract date and minute from current URI
    filename = uri.split('/')[-1]
    current_date, current_minute = parse_cache_key(filename)
    
    # Parse current timestamp
    current_dt = datetime.strptime(f"{current_date}{current_minute}", "%Y%m%d%H%M")
    
    # Determine how many minutes to load (up to max_history_minutes)
    minutes_to_load = min(max_history_minutes, Config.get_history_window_size())
    
    logger.info(f"Loading up to {minutes_to_load} minutes of historical context before {current_date} {current_minute}")
    
    # Build index of available S3 files by listing dates around current date
    bucket = Config.get_source_bucket()
    s3_client = boto3.client('s3')
    
    # List dates to check (current date and up to 5 days before)
    dates_to_check = []
    for i in range(5, -1, -1):  # 5 days before to current
        check_dt = current_dt - timedelta(days=i)
        dates_to_check.append(check_dt.strftime("%Y%m%d"))
    
    # Build index of all available files
    available_files = {}  # timestamp_int -> (uri, date, minute)
    
    for date_str in dates_to_check:
        prefix = f"one-minute/{date_str}/"
        try:
            logger.debug(f"Listing S3 files for date: {date_str}")
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    file_name = key.split('/')[-1]
                    
                    if file_name.startswith('strikes_') and file_name.endswith('.parquet'):
                        try:
                            file_date, file_minute = parse_cache_key(file_name)
                            timestamp_int = int(file_date + file_minute)
                            file_uri = f"s3://{bucket}/{key}"
                            available_files[timestamp_int] = (file_uri, file_date, file_minute)
                        except:
                            continue
        except Exception as e:
            logger.debug(f"Could not list files for date {date_str}: {e}")
            continue
    
    logger.info(f"Found {len(available_files)} available files across {len(dates_to_check)} dates")
    
    # Sort timestamps and find files before current timestamp
    current_timestamp_int = int(current_date + current_minute)
    sorted_timestamps = sorted([ts for ts in available_files.keys() if ts < current_timestamp_int], reverse=True)
    
    # Load up to max_history_minutes files
    files_to_load = sorted_timestamps[:minutes_to_load]
    files_to_load.reverse()  # Load oldest first for chronological order
    
    logger.info(f"Will attempt to load {len(files_to_load)} historical files")
    
    # Load historical files
    loaded_count = 0
    for timestamp_int in files_to_load:
        hist_uri, hist_date, hist_minute = available_files[timestamp_int]
        try:
            logger.debug(f"Loading historical file: {hist_uri}")
            hist_df = s3_mgr.read_parquet(hist_uri)
            
            if not hist_df.empty:
                history_mgr.add_minute(hist_df, hist_date, hist_minute)
                loaded_count += 1
        except Exception as e:
            logger.warning(f"Failed to load historical file {hist_uri}: {e}")
            continue
    
    logger.info(f"Successfully loaded {loaded_count} historical minutes into context")


def process_file(uri: str, engine: FeatureEngine, s3_mgr: S3Manager,
                 history_mgr: HistoryManager, mode: str, start_time: float) -> Dict[str, Any]:
    """
    Process a single parquet file.
    
    Args:
        uri: S3 URI of source file
        engine: FeatureEngine instance
        s3_mgr: S3Manager instance
        history_mgr: HistoryManager instance
        mode: Processing mode ('batch' or 'live')
        start_time: Handler start time for elapsed calculation
    
    Returns:
        Batch mode: {'status': 'success', 'uri': uri}
        Live mode: {'data': [...], 'metadata': {...}}
    
    Raises:
        Exception: If processing fails
    """
    file_start_time = time.time()
    
    # Step 0: Load historical context if history queue is empty
    # This happens:
    # - In live mode: Always (single file invocation)
    # - In batch mode: Only for the first file (subsequent files use the rolling queue)
    if history_mgr.get_current_size() == 0:
        logger.info("History queue is empty, loading historical context from S3...")
        try:
            load_historical_context(uri, s3_mgr, history_mgr, max_history_minutes=60)
        except Exception as e:
            logger.warning(f"Failed to load historical context: {e}")
            # Continue processing - features will be NaN but won't fail
    
    # Step 1: Read parquet from S3
    logger.info(f"Reading parquet from S3: {uri}")
    df = s3_mgr.read_parquet(uri)
    
    # Step 2: Validate DataFrame
    if df.empty:
        raise ValueError(f"Empty parquet file: {uri}")
    
    required_columns = ['stockPrice', 'expirDate', 'strike', 'distance_to_atm']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    logger.info(f"Loaded DataFrame: {len(df)} rows, {len(df.columns)} columns")
    
    # Step 3: Extract date and minute from filename
    filename = uri.split('/')[-1]
    date, minute = parse_cache_key(filename)
    
    logger.info(f"Processing date={date}, minute={minute}")
    
    # Step 4: Add DataFrame to history manager with temporal validation
    history_mgr.add_minute(df, date, minute)
    logger.info(f"History queue size: {history_mgr.get_current_size()}")
    
    # Step 5: Compute features using history manager
    logger.info("Computing features...")
    df_with_features = engine.compute_features(df, history_mgr, filename=filename)
    
    # Step 6: Round numerics
    logger.info("Rounding numeric values...")
    df_rounded = engine.round_numerics(df_with_features, decimals=Config.get_numeric_precision())
    
    # Step 7: Extract stock price (common value for the minute)
    stock_price = float(df_rounded['stockPrice'].iloc[0])
    
    # Step 8: Mode-specific processing
    if mode == 'batch':
        return process_batch_mode(uri, df_rounded, date, minute, s3_mgr, file_start_time)
    else:
        history_size = history_mgr.get_current_size()
        return process_live_mode(uri, df_rounded, date, minute, stock_price, 
                                start_time, file_start_time, history_size)


def process_batch_mode(uri: str, df: pd.DataFrame, date: str, minute: str,
                      s3_mgr: S3Manager, file_start_time: float) -> Dict[str, Any]:
    """
    Process file in batch mode: save to S3 with tags.
    
    Args:
        uri: Source S3 URI
        df: Processed DataFrame
        date: Date string (YYYYMMDD)
        minute: Minute timestamp (HHMM)
        s3_mgr: S3Manager instance
        file_start_time: File processing start time
    
    Returns:
        {'status': 'success', 'uri': uri}
    """
    # Step 1: Construct destination S3 URI
    dest_uri = uri.replace(
        Config.get_source_bucket(),
        Config.get_dest_bucket()
    )
    
    logger.info(f"Destination URI: {dest_uri}")
    
    # Step 2: Compute source checksum
    try:
        source_checksum = s3_mgr.compute_checksum(uri)
    except Exception as e:
        logger.warning(f"Failed to compute checksum: {e}")
        source_checksum = ""
    
    # Step 3: Get feature version hash
    from features import create_default_registry
    registry = create_default_registry()
    feature_version_hash = registry.compute_version_hash()
    
    # Step 4: Prepare S3 tags
    tags = {
        'feature_version_hash': feature_version_hash,
        'processing_timestamp': datetime.utcnow().isoformat() + 'Z',
        'source_data_checksum': source_checksum
    }
    
    # Step 5: Write to S3 destination
    logger.info(f"Writing to S3: {dest_uri}")
    logger.info(f"DataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns")
    s3_mgr.write_parquet(df, dest_uri, tags)
    
    # Calculate processing time
    processing_time = time.time() - file_start_time
    logger.info(f"✓ Batch processing complete: {processing_time:.2f}s")
    
    return {
        'status': 'success',
        'uri': uri
    }


def process_live_mode(uri: str, df: pd.DataFrame, date: str, minute: str,
                     stock_price: float, handler_start_time: float, 
                     file_start_time: float, history_size: int = 0) -> Dict[str, Any]:
    """
    Process file in live mode: return JSON response without S3 storage.
    
    Args:
        uri: Source S3 URI
        df: Processed DataFrame
        date: Date string (YYYYMMDD)
        minute: Minute timestamp (HHMM)
        stock_price: Stock price for the minute
        handler_start_time: Handler start time
        file_start_time: File processing start time
    
    Returns:
        {'data': [...], 'metadata': {...}}
    """
    # Step 1: Convert DataFrame to JSON
    logger.info("Converting DataFrame to JSON for live mode response...")
    data_json = df.to_dict(orient='records')
    
    # Step 2: Calculate processing time
    processing_time_ms = int((time.time() - handler_start_time) * 1000)
    
    # Step 3: Get feature version hash
    from features import create_default_registry
    registry = create_default_registry()
    feature_version_hash = registry.compute_version_hash()
    
    logger.info(f"Live processing complete: {processing_time_ms}ms")
    
    # Step 4: Return JSON response
    return {
        'data': data_json,
        'metadata': {
            'processing_time_ms': processing_time_ms,
            'feature_count': len(df.columns),
            'row_count': len(df),
            'date': date,
            'minute': minute,
            'stock_price': stock_price,
            'feature_version_hash': feature_version_hash,
            'history_minutes_loaded': history_size
        }
    }
