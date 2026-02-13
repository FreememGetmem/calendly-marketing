"""
Calendly Webhook Receiver Lambda Function
Receives webhooks from Calendly and streams to Kinesis
"""

import json
import os
import boto3
import base64
from datetime import datetime
from typing import Dict, Any
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
kinesis_client = boto3.client('kinesis')
s3_client = boto3.client('s3')

# Environment variables
KINESIS_STREAM_NAME = os.environ['KINESIS_STREAM_NAME']
RAW_BUCKET = os.environ['RAW_BUCKET']
ENVIRONMENT = os.environ['ENVIRONMENT']

# Event types to filter
ALLOWED_EVENT_TYPES = {
    'https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78': 'facebook_paid_ads',
    'https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098': 'youtube_paid_ads',
    'https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312': 'tiktok_paid_ads'
}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for Calendly webhooks
    Args:
        event: Lambda event containing webhook payload
        context: Lambda context
    Returns:
        API Gateway response
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        # Parse the webhook payload
        if 'body' in event:
            # API Gateway format
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        else:
            # Direct invocation
            body = event
        # Extract webhook data
        webhook_event = body.get('event')
        payload = body.get('payload', {})
        created_at = body.get('created_at')
        logger.info(f"Processing webhook event: {webhook_event}")
        # Validate event type
        if webhook_event != 'invitee.created':
            logger.info(f"Ignoring non-invitee.created event: {webhook_event}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Event type ignored'})
            }
        # Extract scheduled event details
        scheduled_event = payload.get('scheduled_event', {})
        event_type_url = scheduled_event.get('event_type')
        # Check if event type is in our allowed list
        if event_type_url not in ALLOWED_EVENT_TYPES:
            logger.info(f"Event type not in allowed list: {event_type_url}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Event type not tracked'})
            }
        # Get marketing channel
        marketing_channel = ALLOWED_EVENT_TYPES[event_type_url]
        # Enrich the webhook data
        enriched_data = {
            'webhook_event': webhook_event,
            'webhook_received_at': datetime.utcnow().isoformat(),
            'created_at': created_at,
            'marketing_channel': marketing_channel,
            'event_type_url': event_type_url,
            'invitee_data': {
                'uri': payload.get('uri'),
                'email': payload.get('email'),
                'name': payload.get('name'),
                'first_name': payload.get('first_name'),
                'last_name': payload.get('last_name'),
                'created_at': payload.get('created_at'),
                'status': payload.get('status'),
                'timezone': payload.get('timezone'),
                'questions_and_answers': payload.get('questions_and_answers', [])
            },
            'scheduled_event_data': {
                'uri': scheduled_event.get('uri'),
                'name': scheduled_event.get('name'),
                'start_time': scheduled_event.get('start_time'),
                'end_time': scheduled_event.get('end_time'),
                'created_at': scheduled_event.get('created_at'),
                'status': scheduled_event.get('status'),
                'location': scheduled_event.get('location', {}),
                'event_memberships': scheduled_event.get('event_memberships', [])
            },
            'tracking_data': payload.get('tracking', {})
        }
        # Store raw webhook in S3 for audit/replay
        s3_key = f"webhooks/{webhook_event}/{datetime.utcnow().strftime('%Y/%m/%d')}/{context.request_id}.json"
        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=json.dumps(body, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Stored raw webhook to S3: s3://{RAW_BUCKET}/{s3_key}")
        # Send to Kinesis Data Stream
        partition_key = marketing_channel
        kinesis_response = kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(enriched_data),
            PartitionKey=partition_key
        )
        logger.info(f"Sent to Kinesis: ShardId={kinesis_response['ShardId']}, "
                   f"SequenceNumber={kinesis_response['SequenceNumber']}")
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': 'Webhook processed successfully',
                'kinesis_sequence_number': kinesis_response['SequenceNumber'],
                'marketing_channel': marketing_channel
            })
        }
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e)
            })
        }


def validate_webhook_signature(event: Dict[str, Any]) -> bool:
    """
    Validate Calendly webhook signature (if configured)
    Args:
        event: Lambda event
    Returns:
        True if valid, False otherwise
    """
    # TODO: Implement webhook signature validation
    # Calendly uses webhook signing for security
    return True
