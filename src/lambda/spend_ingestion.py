"""
Spend Data Ingestion Lambda Function
Fetches daily marketing spend data from S3 and writes to Bronze layer
"""

import json
import os
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging
import urllib.request

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Environment variables
BRONZE_BUCKET = os.environ['BRONZE_BUCKET']
DDB_TABLE_NAME = os.environ['DDB_TABLE']
ENVIRONMENT = os.environ['ENVIRONMENT']

# Source data configuration
SPEND_DATA_BASE_URL = 'https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data'
FILE_INDEX_URL = f'{SPEND_DATA_BASE_URL}/file_index.json'

# DynamoDB table
state_table = dynamodb.Table(DDB_TABLE_NAME)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for spend data ingestion
    
    Args:
        event: Lambda event (can contain execution_date)
        context: Lambda context
        
    Returns:
        Ingestion results
    """
    try:
        # Determine execution date (default to yesterday)
        execution_date_str = event.get('execution_date')
        if execution_date_str:
            execution_date = datetime.fromisoformat(execution_date_str.replace('Z', ''))
        else:
            execution_date = datetime.utcnow() - timedelta(days=1)
        
        target_date = execution_date.strftime('%Y-%m-%d')
        logger.info(f"Ingesting spend data for date: {target_date}")
        
        # Fetch available files from index
        available_files = fetch_file_index()
        logger.info(f"Available files: {len(available_files)}")
        
        # Determine which files to fetch (last 30 days or available)
        files_to_fetch = determine_files_to_fetch(available_files, execution_date)
        logger.info(f"Files to fetch: {len(files_to_fetch)}")
        
        # Fetch and process spend data
        all_spend_records = []
        for file_date in files_to_fetch:
            try:
                spend_data = fetch_spend_data(file_date)
                all_spend_records.extend(spend_data)
                logger.info(f"Fetched {len(spend_data)} records for {file_date}")
            except Exception as e:
                logger.error(f"Error fetching data for {file_date}: {str(e)}")
        
        # Write to Bronze layer
        if all_spend_records:
            write_to_bronze_layer(all_spend_records, execution_date)
        
        # Update pipeline state
        update_pipeline_state(
            pipeline_id='spend_ingestion',
            execution_date=target_date,
            records_processed=len(all_spend_records),
            files_processed=len(files_to_fetch)
        )
        
        logger.info(f"Spend ingestion completed. Processed {len(all_spend_records)} records")
        
        return {
            'statusCode': 200,
            'execution_date': target_date,
            'records_processed': len(all_spend_records),
            'files_processed': len(files_to_fetch)
        }
        
    except Exception as e:
        logger.error(f"Error in spend ingestion: {str(e)}", exc_info=True)
        
        # Update state with failure
        update_pipeline_state(
            pipeline_id='spend_ingestion',
            execution_date=target_date if 'target_date' in locals() else 'unknown',
            records_processed=0,
            files_processed=0,
            status='failed',
            error_message=str(e)
        )
        
        raise


def fetch_file_index() -> List[str]:
    """
    Fetch the file index to determine available spend data files
    
    Returns:
        List of available file dates
    """
    try:
        with urllib.request.urlopen(FILE_INDEX_URL) as response:
            file_index = json.loads(response.read().decode('utf-8'))
        
        # Extract dates from filenames
        available_dates = []
        for filename in file_index.get('files', []):
            # Extract date from filename: spend_data_YYYY-MM-DD.json
            if filename.startswith('spend_data_') and filename.endswith('.json'):
                date_str = filename.replace('spend_data_', '').replace('.json', '')
                available_dates.append(date_str)
        
        return sorted(available_dates, reverse=True)  # Most recent first
        
    except Exception as e:
        logger.warning(f"Error fetching file index: {str(e)}. Using date-based approach.")
        return []


def determine_files_to_fetch(available_files: List[str], execution_date: datetime, 
                             lookback_days: int = 30) -> List[str]:
    """
    Determine which files to fetch based on availability and lookback period
    
    Args:
        available_files: List of available file dates
        execution_date: Execution date
        lookback_days: Number of days to look back
        
    Returns:
        List of dates to fetch
    """
    files_to_fetch = []
    
    if available_files:
        # Use available files list (up to last 30 days)
        for file_date in available_files[:lookback_days]:
            files_to_fetch.append(file_date)
    else:
        # Fallback: generate dates for last N days
        for i in range(lookback_days):
            date = execution_date - timedelta(days=i)
            files_to_fetch.append(date.strftime('%Y-%m-%d'))
    
    return files_to_fetch


def fetch_spend_data(date_str: str) -> List[Dict[str, Any]]:
    """
    Fetch spend data for a specific date
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        List of spend records
    """
    url = f'{SPEND_DATA_BASE_URL}/spend_data_{date_str}.json'
    
    try:
        with urllib.request.urlopen(url) as response:
            spend_data = json.loads(response.read().decode('utf-8'))
        
        # Enrich records with ingestion metadata
        enriched_records = []
        for record in spend_data:
            enriched_record = {
                **record,
                'ingested_at': datetime.utcnow().isoformat(),
                'source_file': f'spend_data_{date_str}.json',
                'ingestion_date': datetime.utcnow().strftime('%Y-%m-%d')
            }
            enriched_records.append(enriched_record)
        
        return enriched_records
        
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning(f"Spend data not found for {date_str}")
            return []
        raise


def write_to_bronze_layer(spend_records: List[Dict[str, Any]], execution_date: datetime) -> None:
    """
    Write spend data to Bronze layer in S3
    
    Args:
        spend_records: List of spend records
        execution_date: Execution date for partitioning
    """
    # Group records by date and channel for efficient storage
    records_by_date_channel = {}
    
    for record in spend_records:
        date = record['date']
        channel = record['channel']
        key = f"{date}_{channel}"
        
        if key not in records_by_date_channel:
            records_by_date_channel[key] = []
        records_by_date_channel[key].append(record)
    
    # Write each group to S3
    for key, records in records_by_date_channel.items():
        date, channel = key.split('_', 1)
        
        s3_key = (f"marketing_spend/"
                 f"channel={channel}/"
                 f"date={date}/"
                 f"spend_data.json")
        
        # Prepare the data structure for Delta Lake
        data_to_write = {
            'records': records,
            'metadata': {
                'ingestion_date': execution_date.strftime('%Y-%m-%d'),
                'record_count': len(records),
                'channel': channel,
                'date': date
            }
        }
        
        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=json.dumps(data_to_write, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Written {len(records)} records to s3://{BRONZE_BUCKET}/{s3_key}")


def update_pipeline_state(pipeline_id: str, execution_date: str, records_processed: int,
                          files_processed: int, status: str = 'completed',
                          error_message: str = None) -> None:
    """
    Update pipeline execution state in DynamoDB
    
    Args:
        pipeline_id: Pipeline identifier
        execution_date: Execution date
        records_processed: Count of processed records
        files_processed: Count of processed files
        status: Pipeline status
        error_message: Error message if failed
    """
    try:
        item = {
            'pipeline_id': pipeline_id,
            'execution_date': execution_date,
            'timestamp': datetime.utcnow().isoformat(),
            'records_processed': records_processed,
            'files_processed': files_processed,
            'status': status
        }
        
        if error_message:
            item['error_message'] = error_message
        
        state_table.put_item(Item=item)
        logger.info(f"Updated pipeline state for {pipeline_id}")
        
    except Exception as e:
        logger.error(f"Error updating pipeline state: {str(e)}")
