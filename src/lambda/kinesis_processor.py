"""
Kinesis Processor Lambda Function - FIXED VERSION
Processes events from Kinesis and writes to Bronze layer
"""
import json
import os
import boto3
import base64
from datetime import datetime
from typing import Dict, Any, List
import logging
import hashlib

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

# Environment variables
BRONZE_BUCKET = os.environ['BRONZE_BUCKET']
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'prod')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Process Kinesis records and write to Bronze layer
    """
    try:
        logger.info(f"Processing {len(event['Records'])} Kinesis records")
        processed_records = []
        failed_records = []

        for record in event['Records']:
            try:
                # Decode Kinesis data
                payload = json.loads(base64.b64decode(record['kinesis']['data']))

                # Process the record
                process_calendly_event(payload, record)
                processed_records.append(record['kinesis']['sequenceNumber'])

            except Exception as e:
                logger.error(f"Error processing record {record['kinesis']['sequenceNumber']}: {str(e)}")
                failed_records.append({
                    'sequence_number': record['kinesis']['sequenceNumber'],
                    'error': str(e)
                })

        logger.info(f"Processed: {len(processed_records)}, Failed: {len(failed_records)}")

        return {
            'statusCode': 200,
            'processed': len(processed_records),
            'failed': len(failed_records),
            'failed_records': failed_records
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        raise


def process_calendly_event(payload: Dict[str, Any], kinesis_record: Dict[str, Any]) -> None:
    """
    Process a single Calendly event and write to Bronze layer
    """
    # Extract key information
    marketing_channel = payload.get('marketing_channel', 'direct')
    invitee_data = payload.get('invitee_data', {})
    scheduled_event = payload.get('scheduled_event_data', {})
    tracking = payload.get('tracking_data', {})

    # Generate safe event ID (no colons or special characters)
    sequence_number = kinesis_record['kinesis']['sequenceNumber']
    safe_sequence = sequence_number.replace(':', '-')  # Replace colons with dashes
    event_id = f"event-{safe_sequence}"

    # Create flattened record for Bronze layer
    bronze_record = {
        # Event metadata
        'event_id': event_id,
        'kinesis_sequence_number': sequence_number,
        'kinesis_partition_key': kinesis_record['kinesis']['partitionKey'],
        'kinesis_arrival_timestamp': kinesis_record['kinesis']['approximateArrivalTimestamp'],
        'processed_at': datetime.utcnow().isoformat(),

        # Webhook metadata
        'webhook_event': payload.get('webhook_event'),
        'webhook_received_at': payload.get('webhook_received_at'),
        'webhook_created_at': payload.get('created_at'),

        # Marketing channel
        'marketing_channel': marketing_channel,
        'event_type_url': payload.get('event_type_url'),

        # Invitee information
        'invitee_uri': invitee_data.get('uri'),
        'invitee_email': invitee_data.get('email'),
        'invitee_name': invitee_data.get('name'),
        'invitee_first_name': invitee_data.get('first_name'),
        'invitee_last_name': invitee_data.get('last_name'),
        'invitee_created_at': invitee_data.get('created_at'),
        'invitee_status': invitee_data.get('status'),
        'invitee_timezone': invitee_data.get('timezone'),
        'invitee_phone': extract_phone_number(invitee_data.get('questions_and_answers', [])),

        # Scheduled event information
        'scheduled_event_uri': scheduled_event.get('uri'),
        'scheduled_event_name': scheduled_event.get('name'),
        'scheduled_event_start_time': scheduled_event.get('start_time'),
        'scheduled_event_end_time': scheduled_event.get('end_time'),
        'scheduled_event_created_at': scheduled_event.get('created_at'),
        'scheduled_event_status': scheduled_event.get('status'),
        'scheduled_event_location_type': scheduled_event.get('location', {}).get('type'),
        'scheduled_event_location': scheduled_event.get('location', {}).get('location'),

        # Employee/Host information
        'host_email': extract_host_email(scheduled_event.get('event_memberships', [])),
        'host_name': extract_host_name(scheduled_event.get('event_memberships', [])),

        # UTM tracking parameters
        'utm_source': tracking.get('utm_source'),
        'utm_medium': tracking.get('utm_medium'),
        'utm_campaign': tracking.get('utm_campaign'),
        'utm_content': tracking.get('utm_content'),
        'utm_term': tracking.get('utm_term'),

        # Date partitions
        'booking_date': extract_date(scheduled_event.get('start_time')),
        'booking_year': extract_year(scheduled_event.get('start_time')),
        'booking_month': extract_month(scheduled_event.get('start_time')),
        'booking_day': extract_day(scheduled_event.get('start_time')),
        'booking_hour': extract_hour(scheduled_event.get('start_time')),
        'booking_day_of_week': extract_day_of_week(scheduled_event.get('start_time'))
    }

    # Write to Bronze layer
    write_to_bronze_layer(bronze_record, marketing_channel)


def write_to_bronze_layer(record: Dict[str, Any], marketing_channel: str) -> None:
    """
    Write record to Bronze layer in S3
    FIXED: Use safe filename without special characters
    """
    booking_date = record.get('booking_date', datetime.utcnow().strftime('%Y-%m-%d'))
    event_id = record.get('event_id', f"event-{datetime.utcnow().timestamp()}")

    # S3 key with partitioning - SAFE FILENAME
    s3_key = (f"calendly_events/"
              f"channel={marketing_channel}/"
              f"date={booking_date}/"
              f"{event_id}.json")

    # Write to S3
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=json.dumps(record, indent=2),
        ContentType='application/json'
    )

    logger.info(f"Written to Bronze: s3://{BRONZE_BUCKET}/{s3_key}")


def extract_phone_number(questions_and_answers: List[Dict]) -> str:
    """Extract phone number from Q&A"""
    for qa in questions_and_answers:
        if qa and 'phone' in qa.get('question', '').lower():
            return qa.get('answer', '')
    return None


def extract_host_email(event_memberships: List[Dict]) -> str:
    """Extract host email from event memberships"""
    if event_memberships and len(event_memberships) > 0:
        return event_memberships[0].get('user_email')
    return None


def extract_host_name(event_memberships: List[Dict]) -> str:
    """Extract host name from event memberships"""
    if event_memberships and len(event_memberships) > 0:
        return event_memberships[0].get('user_name')
    return None


def extract_date(timestamp_str: str) -> str:
    """Extract date from ISO timestamp"""
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except:
            pass
    return datetime.utcnow().strftime('%Y-%m-%d')


def extract_year(timestamp_str: str) -> int:
    """Extract year from ISO timestamp"""
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.year
        except:
            pass
    return datetime.utcnow().year


def extract_month(timestamp_str: str) -> int:
    """Extract month from ISO timestamp"""
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.month
        except:
            pass
    return datetime.utcnow().month


def extract_day(timestamp_str: str) -> int:
    """Extract day from ISO timestamp"""
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.day
        except:
            pass
    return datetime.utcnow().day


def extract_hour(timestamp_str: str) -> int:
    """Extract hour from ISO timestamp"""
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.hour
        except:
            pass
    return datetime.utcnow().hour


def extract_day_of_week(timestamp_str: str) -> str:
    """Extract day of week from ISO timestamp"""
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime('%A')
        except:
            pass
    return datetime.utcnow().strftime('%A')