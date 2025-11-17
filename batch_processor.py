"""Batch processing orchestration for SPY History Features Lambda."""

import logging
import boto3
import yaml
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Optional
from datetime import datetime
from pathlib import Path

from lambda_client import LambdaClient
from date_thread import DateThread

logger = logging.getLogger(__name__)


class BatchProcessor:
    """Main orchestration script for batch processing."""
    
    def __init__(self, 
                 function_name: Optional[str] = None,
                 source_bucket: Optional[str] = None,
                 dest_bucket: Optional[str] = None,
                 max_threads: Optional[int] = None,
                 region: Optional[str] = None,
                 config_path: Optional[str] = None):
        """
        Initialize BatchProcessor.
        
        Args:
            function_name: Lambda function name (overrides config file)
            source_bucket: Source S3 bucket (overrides config file)
            dest_bucket: Destination S3 bucket (overrides config file)
            max_threads: Maximum concurrent date threads (overrides config file)
            region: AWS region (overrides config file)
            config_path: Path to config.yaml file (default: config.yaml in same directory)
        """
        # Load configuration from YAML file
        config = self._load_config(config_path)
        
        # Command-line arguments override config file
        self.function_name = function_name or config.get('lambda', {}).get('function_name', 'spy-history-features')
        self.source_bucket = source_bucket or config.get('s3', {}).get('source_bucket', 'spy-no-history-features')
        self.dest_bucket = dest_bucket or config.get('s3', {}).get('dest_bucket', 'spy-with-history-features')
        self.max_threads = max_threads or config.get('threading', {}).get('max_threads', 20)
        self.region = region or config.get('lambda', {}).get('region', 'us-east-1')
        
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.lambda_client = LambdaClient(self.function_name, self.region)
        
        logger.info(f"BatchProcessor initialized: {self.function_name}, max_threads={self.max_threads}")
    
    def _load_config(self, config_path: Optional[str] = None) -> Dict:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to config.yaml file
        
        Returns:
            Configuration dictionary
        """
        if config_path is None:
            # Default to config.yaml in the same directory as this file
            config_path = Path(__file__).parent / 'config.yaml'
        else:
            config_path = Path(config_path)
        
        if not config_path.exists():
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return {}
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded configuration from {config_path}")
                return config or {}
        except Exception as e:
            logger.warning(f"Failed to load config file {config_path}: {e}, using defaults")
            return {}
    
    def discover_unprocessed_files(self) -> Dict[str, List[str]]:
        """
        Compare source and destination buckets to find unprocessed files.
        
        Returns:
            Dict mapping date -> list of unprocessed S3 URIs (sorted chronologically)
        """
        logger.info("Discovering unprocessed files...")
        
        # List source files
        source_files = self._list_s3_files(self.source_bucket, 'one-minute/')
        logger.info(f"Source bucket: {len(source_files)} files")
        
        # List destination files
        dest_files = self._list_s3_files(self.dest_bucket, 'one-minute/')
        logger.info(f"Destination bucket: {len(dest_files)} files")
        
        # Find unprocessed (files in source but not in dest)
        source_keys = {self._extract_key_suffix(f) for f in source_files}
        dest_keys = {self._extract_key_suffix(f) for f in dest_files}
        unprocessed_keys = source_keys - dest_keys
        
        logger.info(f"Unprocessed: {len(unprocessed_keys)} files")
        
        # Group by date
        by_date = defaultdict(list)
        for key in unprocessed_keys:
            date = self._extract_date_from_key(key)
            # key is like "20250729/strikes_202507291611.parquet"
            # need to add "one-minute/" prefix
            s3_uri = f"s3://{self.source_bucket}/one-minute/{key}"
            by_date[date].append(s3_uri)
        
        # Sort URIs within each date chronologically
        for date in by_date:
            by_date[date] = sorted(by_date[date])
        
        # Sort dates chronologically
        sorted_by_date = dict(sorted(by_date.items()))
        
        # Log discovered dates and file counts
        dates_list = sorted(sorted_by_date.keys())
        if dates_list:
            logger.info(f"Discovered {len(dates_list)} dates to process:")
            logger.info(f"  Date range: {dates_list[0]} to {dates_list[-1]}")
            logger.info(f"  Dates: {', '.join(dates_list)}")
            
            # Log file counts per date
            for date, uris in sorted_by_date.items():
                logger.info(f"  {date}: {len(uris)} files")
        
        return sorted_by_date
    
    def run(self) -> Dict:
        """
        Execute batch processing.
        
        Returns:
            Summary report with results
        """
        start_time = datetime.now()
        logger.info("Starting batch processing...")
        
        # Discover unprocessed files
        unprocessed_by_date = self.discover_unprocessed_files()
        
        if not unprocessed_by_date:
            logger.info("No unprocessed files found")
            return {
                'status': 'success',
                'dates_processed': 0,
                'files_processed': 0,
                'files_failed': 0,
                'duration_seconds': 0
            }
        
        # Create thread pool and process dates in chronological order
        results = []
        total_dates = len(unprocessed_by_date)
        
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {}
            
            # Submit dates in chronological order
            for date, uris in unprocessed_by_date.items():
                thread = DateThread(date, uris, self.lambda_client)
                future = executor.submit(thread.process)
                futures[future] = date
            
            # Wait for completion and show progress
            completed_count = 0
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    completed_count += 1
                    
                    # Format result with emoji status
                    status = "✅" if result['failed'] == 0 else "❌"
                    files_info = f"{result['completed']} processed"
                    if result['failed'] > 0:
                        files_info += f", {result['failed']} failed"
                    
                    logger.info(f"{status} [{completed_count}/{total_dates}] {result['date']}: {files_info}")
                    
                    # Show errors if any
                    if result.get('failures'):
                        for failure in result['failures'][:3]:  # Show first 3 errors
                            error_msg = failure.get('error', 'Unknown error')
                            if len(error_msg) > 80:
                                error_msg = error_msg[:77] + "..."
                            logger.error(f"   💥 {failure.get('uri', 'Unknown file')}: {error_msg}")
                        if len(result['failures']) > 3:
                            logger.error(f"   ... and {len(result['failures']) - 3} more errors")
                    
                except Exception as e:
                    completed_count += 1
                    date = futures[future]
                    logger.error(f"❌ [{completed_count}/{total_dates}] {date}: Exception - {str(e)}")
        
        # Generate summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        summary = self._generate_summary(results, duration)
        self._print_summary(summary)
        
        return summary
    
    def _list_s3_files(self, bucket: str, prefix: str) -> List[str]:
        """
        List all files in S3 bucket with prefix.
        
        Args:
            bucket: S3 bucket name
            prefix: Key prefix
        
        Returns:
            List of S3 keys
        """
        keys = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # Only include parquet files
                    if key.endswith('.parquet'):
                        keys.append(key)
        
        return keys
    
    def _extract_key_suffix(self, key: str) -> str:
        """Extract the date/filename portion of the key."""
        # e.g., "one-minute/20250724/strikes_202507240931.parquet" -> "20250724/strikes_202507240931.parquet"
        parts = key.split('/', 1)
        if len(parts) == 2:
            return parts[1]
        return key
    
    def _extract_date_from_key(self, key: str) -> str:
        """Extract date from S3 key."""
        # e.g., "20250724/strikes_202507240931.parquet" -> "20250724"
        parts = key.split('/')
        if len(parts) >= 2:
            return parts[0]
        return 'unknown'
    
    def _generate_summary(self, results: List[Dict], duration: float) -> Dict:
        """Generate summary report from results."""
        total_completed = sum(r['completed'] for r in results)
        total_failed = sum(r['failed'] for r in results)
        
        failed_dates = [r for r in results if r['failed'] > 0]
        
        return {
            'status': 'success' if total_failed == 0 else 'partial',
            'dates_processed': len(results),
            'files_processed': total_completed,
            'files_failed': total_failed,
            'duration_seconds': duration,
            'failed_dates': failed_dates
        }
    
    def _print_summary(self, summary: Dict) -> None:
        """Print summary report."""
        print("\n" + "="*80)
        print("PROCESSING COMPLETE")
        print("="*80)
        
        # Calculate success rate
        total_dates = summary['dates_processed']
        successful_dates = total_dates - len(summary.get('failed_dates', []))
        success_rate = (successful_dates / total_dates * 100) if total_dates > 0 else 0
        
        print(f"📊 SUMMARY:")
        print(f"   Total Dates: {summary['dates_processed']}")
        print(f"   Successful: {successful_dates} ✅")
        print(f"   Failed: {len(summary.get('failed_dates', []))} ❌")
        print(f"   Total Files Processed: {summary['files_processed']} 📄")
        print(f"   Total Files Failed: {summary['files_failed']} 💥")
        duration_min = summary['duration_seconds'] / 60
        print(f"   Total Duration: {summary['duration_seconds']:.1f}s ({duration_min:.1f} minutes) ⏱️")
        print()
        
        if summary['failed_dates']:
            print("❌ FAILED DATES:")
            for date_result in summary['failed_dates']:
                print(f"   • {date_result['date']}: {date_result['failed']} failures")
            print()
        
        print(f"🎯 Success Rate: {success_rate:.1f}%")
        print("="*80 + "\n")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Batch process SPY history features',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default config.yaml
  python -m spy_history_features_lambda.batch_processor
  
  # Override specific settings
  python -m spy_history_features_lambda.batch_processor --threads 10 --function-name my-lambda
  
  # Use custom config file
  python -m spy_history_features_lambda.batch_processor --config /path/to/config.yaml
        """
    )
    parser.add_argument('--config', dest='config_path', 
                       help='Path to config.yaml file (default: config.yaml in module directory)')
    parser.add_argument('--function-name', 
                       help='Lambda function name (overrides config file)')
    parser.add_argument('--source-bucket',
                       help='Source S3 bucket (overrides config file)')
    parser.add_argument('--dest-bucket',
                       help='Destination S3 bucket (overrides config file)')
    parser.add_argument('--threads', type=int,
                       help='Maximum concurrent threads (overrides config file)')
    parser.add_argument('--region',
                       help='AWS region (overrides config file)')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Logging level (default: INFO)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run batch processor
    processor = BatchProcessor(
        function_name=args.function_name,
        source_bucket=args.source_bucket,
        dest_bucket=args.dest_bucket,
        max_threads=args.threads,
        region=args.region,
        config_path=args.config_path
    )
    
    summary = processor.run()
    
    # Exit with error code if failures
    exit(0 if summary['files_failed'] == 0 else 1)
