#!/bin/bash
# Generate test data for the pipeline

set -e

ENVIRONMENT="prod"
REGION="us-west-1"
BRONZE_BUCKET="calendly-bronze-prod-635374934580"

echo "========================================="
echo "Generating Test Data for Pipeline"
echo "========================================="
echo ""

# Create sample webhook event data
cat > /tmp/test-event-1.json << 'EOF'
{
  "webhook_event": "invitee.created",
  "webhook_received_at": "2024-02-17T10:30:00Z",
  "marketing_channel": "facebook_paid_ads",
  "event_type_url": "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78",
  "payload": {
    "event": "invitee.created",
    "time": "2024-02-17T10:30:00.000000Z",
    "payload": {
      "cancel_url": "https://calendly.com/cancellations/test1",
      "created_at": "2024-02-17T10:30:00.000000Z",
      "email": "test1@example.com",
      "event": "https://api.calendly.com/scheduled_events/test1",
      "name": "Test User 1",
      "new_invitee": null,
      "old_invitee": null,
      "questions_and_answers": [],
      "reschedule_url": "https://calendly.com/reschedulings/test1",
      "rescheduled": false,
      "status": "active",
      "text_reminder_number": null,
      "timezone": "America/New_York",
      "tracking": {
        "utm_campaign": "test_campaign",
        "utm_source": "facebook",
        "utm_medium": "paid",
        "utm_content": null,
        "utm_term": null
      },
      "updated_at": "2024-02-17T10:30:00.000000Z",
      "uri": "https://api.calendly.com/scheduled_events/test1/invitees/test1",
      "canceled": false,
      "scheduled_event": {
        "event_type": "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78",
        "location": {
          "type": "zoom",
          "join_url": "https://zoom.us/j/test"
        },
        "name": "30 Minute Meeting",
        "start_time": "2024-02-17T15:00:00.000000Z",
        "end_time": "2024-02-17T15:30:00.000000Z",
        "status": "active",
        "created_at": "2024-02-17T10:30:00.000000Z",
        "updated_at": "2024-02-17T10:30:00.000000Z",
        "event_memberships": [
          {
            "user": "https://api.calendly.com/users/test-user-1"
          }
        ],
        "invitees_counter": {
          "active": 1,
          "limit": 1,
          "total": 1
        }
      }
    }
  }
}
EOF

cat > /tmp/test-event-2.json << 'EOF'
{
  "webhook_event": "invitee.created",
  "webhook_received_at": "2024-02-17T11:00:00Z",
  "marketing_channel": "youtube_paid_ads",
  "event_type_url": "https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098",
  "payload": {
    "event": "invitee.created",
    "time": "2024-02-17T11:00:00.000000Z",
    "payload": {
      "cancel_url": "https://calendly.com/cancellations/test2",
      "created_at": "2024-02-17T11:00:00.000000Z",
      "email": "test2@example.com",
      "event": "https://api.calendly.com/scheduled_events/test2",
      "name": "Test User 2",
      "status": "active",
      "timezone": "America/Los_Angeles",
      "tracking": {
        "utm_campaign": "youtube_campaign",
        "utm_source": "youtube",
        "utm_medium": "paid"
      },
      "updated_at": "2024-02-17T11:00:00.000000Z",
      "uri": "https://api.calendly.com/scheduled_events/test2/invitees/test2",
      "scheduled_event": {
        "event_type": "https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098",
        "name": "Discovery Call",
        "start_time": "2024-02-17T16:00:00.000000Z",
        "end_time": "2024-02-17T16:30:00.000000Z",
        "status": "active",
        "created_at": "2024-02-17T11:00:00.000000Z"
      }
    }
  }
}
EOF

cat > /tmp/test-event-3.json << 'EOF'
{
  "webhook_event": "invitee.created",
  "webhook_received_at": "2024-02-17T12:00:00Z",
  "marketing_channel": "tiktok_paid_ads",
  "event_type_url": "https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312",
  "payload": {
    "event": "invitee.created",
    "time": "2024-02-17T12:00:00.000000Z",
    "payload": {
      "cancel_url": "https://calendly.com/cancellations/test3",
      "created_at": "2024-02-17T12:00:00.000000Z",
      "email": "test3@example.com",
      "event": "https://api.calendly.com/scheduled_events/test3",
      "name": "Test User 3",
      "status": "active",
      "timezone": "America/Chicago",
      "tracking": {
        "utm_campaign": "tiktok_spring",
        "utm_source": "tiktok",
        "utm_medium": "paid"
      },
      "updated_at": "2024-02-17T12:00:00.000000Z",
      "uri": "https://api.calendly.com/scheduled_events/test3/invitees/test3",
      "scheduled_event": {
        "event_type": "https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312",
        "name": "Consultation",
        "start_time": "2024-02-17T17:00:00.000000Z",
        "end_time": "2024-02-17T17:45:00.000000Z",
        "status": "active",
        "created_at": "2024-02-17T12:00:00.000000Z"
      }
    }
  }
}
EOF

echo "1. Uploading test events to Bronze bucket..."

# Get current date for partitioning
CURRENT_DATE=$(date +%Y-%m-%d)

# Upload to Bronze bucket with proper partitioning
aws s3 cp /tmp/test-event-1.json \
  s3://${BRONZE_BUCKET}/calendly_events/channel=facebook_paid_ads/date=${CURRENT_DATE}/test-event-1.json \
  --region ${REGION}

aws s3 cp /tmp/test-event-2.json \
  s3://${BRONZE_BUCKET}/calendly_events/channel=youtube_paid_ads/date=${CURRENT_DATE}/test-event-2.json \
  --region ${REGION}

aws s3 cp /tmp/test-event-3.json \
  s3://${BRONZE_BUCKET}/calendly_events/channel=tiktok_paid_ads/date=${CURRENT_DATE}/test-event-3.json \
  --region ${REGION}

echo "✓ Test events uploaded"
echo ""

# Create sample spend data
echo "2. Creating sample spend data..."

cat > /tmp/spend-data.json << EOF
[
  {
    "channel": "facebook_paid_ads",
    "date": "${CURRENT_DATE}",
    "spend": 150.00,
    "impressions": 5000,
    "clicks": 250
  },
  {
    "channel": "youtube_paid_ads",
    "date": "${CURRENT_DATE}",
    "spend": 200.00,
    "impressions": 8000,
    "clicks": 400
  },
  {
    "channel": "tiktok_paid_ads",
    "date": "${CURRENT_DATE}",
    "spend": 100.00,
    "impressions": 10000,
    "clicks": 500
  }
]
EOF

# Upload spend data
aws s3 cp /tmp/spend-data.json \
  s3://${BRONZE_BUCKET}/marketing_spend/channel=facebook_paid_ads/date=${CURRENT_DATE}/spend_data.json \
  --region ${REGION}

aws s3 cp /tmp/spend-data.json \
  s3://${BRONZE_BUCKET}/marketing_spend/channel=youtube_paid_ads/date=${CURRENT_DATE}/spend_data.json \
  --region ${REGION}

aws s3 cp /tmp/spend-data.json \
  s3://${BRONZE_BUCKET}/marketing_spend/channel=tiktok_paid_ads/date=${CURRENT_DATE}/spend_data.json \
  --region ${REGION}

echo "✓ Spend data uploaded"
echo ""

# Run Glue crawler to discover the data
echo "3. Running Glue Crawler..."

# Get Glue database name
GLUE_DB=$(aws cloudformation describe-stacks \
  --stack-name calendly-main-stack-${ENVIRONMENT} \
  --region ${REGION} \
  --query "Stacks[0].Outputs[?OutputKey=='GlueDatabaseName'].OutputValue" \
  --output text)

# Create crawler if it doesn't exist
CRAWLER_NAME="calendly-bronze-crawler-${ENVIRONMENT}"

# Check if crawler exists
if ! aws glue get-crawler --name ${CRAWLER_NAME} --region ${REGION} &>/dev/null; then
  echo "Creating Bronze layer crawler..."
  
  # Get Glue role ARN
  GLUE_ROLE=$(aws cloudformation describe-stacks \
    --stack-name calendly-main-stack-${ENVIRONMENT} \
    --region ${REGION} \
    --query "Stacks[0].Outputs[?OutputKey=='GlueExecutionRoleArn'].OutputValue" \
    --output text)
  
  aws glue create-crawler \
    --name ${CRAWLER_NAME} \
    --role ${GLUE_ROLE} \
    --database-name ${GLUE_DB} \
    --targets "{\"S3Targets\":[{\"Path\":\"s3://${BRONZE_BUCKET}/calendly_events/\"},{\"Path\":\"s3://${BRONZE_BUCKET}/marketing_spend/\"}]}" \
    --region ${REGION}
  
  echo "✓ Crawler created"
fi

# Start crawler
aws glue start-crawler --name ${CRAWLER_NAME} --region ${REGION} || echo "Crawler already running"

echo "✓ Crawler started"
echo ""

echo "========================================="
echo "Test Data Generation Complete!"
echo "========================================="
echo ""
echo "Test data has been created in the Bronze bucket:"
echo "  s3://${BRONZE_BUCKET}/calendly_events/"
echo "  s3://${BRONZE_BUCKET}/marketing_spend/"
echo ""
echo "Next steps:"
echo "1. Wait for crawler to complete (~2-3 minutes)"
echo "   aws glue get-crawler --name ${CRAWLER_NAME} --region ${REGION} --query 'Crawler.State'"
echo ""
echo "2. Check Glue tables were created:"
echo "   aws glue get-tables --database-name ${GLUE_DB} --region ${REGION}"
echo ""
echo "3. Run Glue ETL jobs:"
echo "   aws glue start-job-run --job-name calendly-bronze-to-silver-${ENVIRONMENT} --region ${REGION}"
echo ""
echo "4. Check Step Functions execution:"
echo "   aws stepfunctions list-executions --state-machine-arn <your-state-machine-arn> --region ${REGION}"
echo ""

# Cleanup temp files
rm -f /tmp/test-event-*.json /tmp/spend-data.json
