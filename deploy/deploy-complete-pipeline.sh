#!/bin/bash
# Deploy Complete Calendly Pipeline with All Components

set -e

ENVIRONMENT="prod"
REGION="us-west-1"

echo "========================================="
echo "COMPLETE PIPELINE DEPLOYMENT"
echo "========================================="
echo ""
echo "Environment: $ENVIRONMENT"
echo "Region: $REGION"
echo ""
echo "This will deploy:"
echo "  1. Streaming Stack (Kinesis + Lambda processor)"
echo "  2. API Stack (API Gateway + Webhook receiver)"
echo "  3. Orchestration Stack (Step Functions + EventBridge + SNS)"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

# ==========================================
# Step 1: Deploy Streaming Stack
# ==========================================
echo ""
echo "========================================="
echo "Step 1: Deploying Streaming Stack"
echo "========================================="
echo ""

aws cloudformation deploy \
  --template-file infrastructure/streaming-stack.yaml \
  --stack-name calendly-streaming-stack-${ENVIRONMENT} \
  --parameter-overrides \
    Environment=${ENVIRONMENT} \
    MainStackName=calendly-main-stack-${ENVIRONMENT} \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ${REGION} \
  --no-fail-on-empty-changeset

echo "✓ Streaming Stack deployed"

# Get outputs
KINESIS_STREAM=$(aws cloudformation describe-stacks \
  --stack-name calendly-streaming-stack-${ENVIRONMENT} \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`KinesisStreamName`].OutputValue' \
  --output text)

echo "  Kinesis Stream: $KINESIS_STREAM"

# ==========================================
# Step 2: Deploy API Stack
# ==========================================
echo ""
echo "========================================="
echo "Step 2: Deploying API Gateway Stack"
echo "========================================="
echo ""

aws cloudformation deploy \
  --template-file infrastructure/api-stack.yaml \
  --stack-name calendly-api-stack-${ENVIRONMENT} \
  --parameter-overrides \
    Environment=${ENVIRONMENT} \
    StreamingStackName=calendly-streaming-stack-${ENVIRONMENT} \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ${REGION} \
  --no-fail-on-empty-changeset

echo "✓ API Stack deployed"

# Get webhook URL
WEBHOOK_URL=$(aws cloudformation describe-stacks \
  --stack-name calendly-api-stack-${ENVIRONMENT} \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`WebhookURL`].OutputValue' \
  --output text)

echo "  Webhook URL: $WEBHOOK_URL"

# ==========================================
# Step 3: Deploy Orchestration Stack
# ==========================================
echo ""
echo "========================================="
echo "Step 3: Deploying Orchestration Stack"
echo "========================================="
echo ""

aws cloudformation deploy \
  --template-file infrastructure/orchestration-stack.yaml \
  --stack-name calendly-orchestration-stack-${ENVIRONMENT} \
  --parameter-overrides \
    Environment=${ENVIRONMENT} \
    MainStackName=calendly-main-stack-${ENVIRONMENT} \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ${REGION} \
  --no-fail-on-empty-changeset

echo "✓ Orchestration Stack deployed"

# Get outputs
STATE_MACHINE=$(aws cloudformation describe-stacks \
  --stack-name calendly-orchestration-stack-${ENVIRONMENT} \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`StateMachineArn`].OutputValue' \
  --output text)

SNS_TOPIC=$(aws cloudformation describe-stacks \
  --stack-name calendly-orchestration-stack-${ENVIRONMENT} \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`NotificationTopicArn`].OutputValue' \
  --output text)

echo "  State Machine: $STATE_MACHINE"
echo "  SNS Topic: $SNS_TOPIC"

# ==========================================
# Summary
# ==========================================
echo ""
echo "========================================="
echo "✅ DEPLOYMENT COMPLETE!"
echo "========================================="
echo ""
echo "📊 Infrastructure Summary:"
echo ""
echo "Streaming Layer:"
echo "  • Kinesis Stream: $KINESIS_STREAM"
echo "  • Lambda Processor: calendly-kinesis-processor-${ENVIRONMENT}"
echo ""
echo "API Layer:"
echo "  • Webhook URL: $WEBHOOK_URL"
echo "  • Lambda Receiver: calendly-webhook-receiver-${ENVIRONMENT}"
echo ""
echo "Orchestration:"
echo "  • Step Functions: $STATE_MACHINE"
echo "  • SNS Topic: $SNS_TOPIC"
echo "  • Schedule: Daily at 2 AM UTC"
echo ""
echo "========================================="
echo "🚀 Next Steps"
echo "========================================="
echo ""
echo "1. Test Webhook:"
echo "   curl -X POST $WEBHOOK_URL \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"event\":\"invitee.created\",\"payload\":{\"uri\":\"test\"}}'"
echo ""
echo "2. Subscribe to SNS notifications:"
echo "   aws sns subscribe \\"
echo "     --topic-arn $SNS_TOPIC \\"
echo "     --protocol email \\"
echo "     --notification-endpoint your-email@example.com \\"
echo "     --region $REGION"
echo ""
echo "3. Manually trigger ETL pipeline:"
echo "   aws stepfunctions start-execution \\"
echo "     --state-machine-arn $STATE_MACHINE \\"
echo "     --region $REGION"
echo ""
echo "4. Configure Calendly webhook:"
echo "   - Go to Calendly Settings → Webhooks"
echo "   - Add webhook URL: $WEBHOOK_URL"
echo "   - Subscribe to: invitee.created, invitee.canceled"
echo ""
echo "========================================="
echo ""
