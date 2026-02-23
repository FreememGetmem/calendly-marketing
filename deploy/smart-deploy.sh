#!/bin/bash
# Smart Pipeline Deployment - Detects and deploys only missing components

set -e

ENVIRONMENT="prod"
REGION="us-west-1"

echo "========================================="
echo "SMART PIPELINE DEPLOYMENT"
echo "========================================="
echo ""
echo "Detecting existing infrastructure..."
echo ""

# ==========================================
# Check Existing Stacks
# ==========================================
check_stack() {
  local stack_name=$1
  if aws cloudformation describe-stacks --stack-name $stack_name --region $REGION &>/dev/null; then
    local status=$(aws cloudformation describe-stacks \
      --stack-name $stack_name \
      --region $REGION \
      --query 'Stacks[0].StackStatus' \
      --output text)
    echo "  ✓ $stack_name ($status)"
    return 0
  else
    echo "  ✗ $stack_name (not found)"
    return 1
  fi
}

echo "Existing Stacks:"
check_stack "calendly-main-stack-${ENVIRONMENT}" && MAIN_EXISTS=true || MAIN_EXISTS=false
check_stack "calendly-glue-jobs-${ENVIRONMENT}" && GLUE_EXISTS=true || GLUE_EXISTS=false
check_stack "calendly-streaming-stack-${ENVIRONMENT}" && STREAMING_EXISTS=true || STREAMING_EXISTS=false
check_stack "calendly-api-stack-${ENVIRONMENT}" && API_EXISTS=true || API_EXISTS=false
check_stack "calendly-orchestration-stack-${ENVIRONMENT}" && ORCH_EXISTS=true || ORCH_EXISTS=false

echo ""

# ==========================================
# Check Individual Resources
# ==========================================
echo "Checking key resources:"

# Kinesis Stream
if aws kinesis describe-stream --stream-name "calendly-events-stream-${ENVIRONMENT}" --region $REGION &>/dev/null; then
  KINESIS_STATUS=$(aws kinesis describe-stream \
    --stream-name "calendly-events-stream-${ENVIRONMENT}" \
    --region $REGION \
    --query 'StreamDescription.StreamStatus' \
    --output text)
  echo "  ✓ Kinesis Stream ($KINESIS_STATUS)"
  KINESIS_EXISTS=true
else
  echo "  ✗ Kinesis Stream"
  KINESIS_EXISTS=false
fi

# Lambda Processor
if aws lambda get-function --function-name "calendly-kinesis-processor-${ENVIRONMENT}" --region $REGION &>/dev/null; then
  echo "  ✓ Lambda Processor"
  LAMBDA_PROC_EXISTS=true
else
  echo "  ✗ Lambda Processor"
  LAMBDA_PROC_EXISTS=false
fi

# Lambda Webhook
if aws lambda get-function --function-name "calendly-webhook-receiver-${ENVIRONMENT}" --region $REGION &>/dev/null; then
  echo "  ✓ Lambda Webhook Receiver"
  LAMBDA_WEBHOOK_EXISTS=true
else
  echo "  ✗ Lambda Webhook Receiver"
  LAMBDA_WEBHOOK_EXISTS=false
fi

echo ""
echo "========================================="
echo "Deployment Plan"
echo "========================================="
echo ""

# ==========================================
# Deployment Strategy
# ==========================================

if [ "$STREAMING_EXISTS" = true ] && [ "$KINESIS_EXISTS" = true ] && [ "$LAMBDA_PROC_EXISTS" = true ]; then
  echo "✓ Streaming infrastructure already exists"
  echo "  Skipping streaming stack deployment"
  echo ""
  DEPLOY_STREAMING=false
else
  echo "⚠️  Streaming infrastructure incomplete or missing"
  echo "  Will deploy/update streaming stack"
  echo ""
  DEPLOY_STREAMING=true
fi

if [ "$API_EXISTS" = true ] && [ "$LAMBDA_WEBHOOK_EXISTS" = true ]; then
  echo "✓ API infrastructure already exists"
  echo "  Skipping API stack deployment"
  echo ""
  DEPLOY_API=false
else
  echo "⚠️  API infrastructure incomplete or missing"
  echo "  Will deploy/update API stack"
  echo ""
  DEPLOY_API=true
fi

if [ "$ORCH_EXISTS" = true ]; then
  echo "✓ Orchestration infrastructure already exists"
  echo "  Skipping orchestration stack deployment"
  echo ""
  DEPLOY_ORCH=false
else
  echo "⚠️  Orchestration infrastructure missing"
  echo "  Will deploy orchestration stack"
  echo ""
  DEPLOY_ORCH=true
fi

# ==========================================
# Execute Deployment
# ==========================================

if [ "$DEPLOY_STREAMING" = false ] && [ "$DEPLOY_API" = false ] && [ "$DEPLOY_ORCH" = false ]; then
  echo "========================================="
  echo "✅ All infrastructure already deployed!"
  echo "========================================="
  echo ""
  echo "Your pipeline is complete. Get the details:"
  echo ""
  echo "Webhook URL:"
  echo "  aws cloudformation describe-stacks \\"
  echo "    --stack-name calendly-api-stack-${ENVIRONMENT} \\"
  echo "    --query 'Stacks[0].Outputs[?OutputKey==\`WebhookURL\`].OutputValue' \\"
  echo "    --output text \\"
  echo "    --region ${REGION}"
  echo ""
  echo "Test webhook:"
  echo "  ./scripts/test-webhook.sh"
  echo ""
  exit 0
fi

echo "Proceeding with deployment..."
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  exit 1
fi

# Deploy Streaming Stack
if [ "$DEPLOY_STREAMING" = true ]; then
  echo ""
  echo "========================================="
  echo "Deploying Streaming Stack"
  echo "========================================="
  
  if [ "$STREAMING_EXISTS" = true ]; then
    echo "Updating existing stack..."
  else
    echo "Creating new stack..."
  fi
  
  aws cloudformation deploy \
    --template-file infrastructure/streaming-stack.yaml \
    --stack-name calendly-streaming-stack-${ENVIRONMENT} \
    --parameter-overrides \
      Environment=${ENVIRONMENT} \
      MainStackName=calendly-main-stack-${ENVIRONMENT} \
    --capabilities CAPABILITY_NAMED_IAM \
    --region ${REGION} \
    --no-fail-on-empty-changeset
  
  echo "✓ Streaming stack ready"
fi

# Deploy API Stack
if [ "$DEPLOY_API" = true ]; then
  echo ""
  echo "========================================="
  echo "Deploying API Stack"
  echo "========================================="
  
  if [ "$API_EXISTS" = true ]; then
    echo "Updating existing stack..."
  else
    echo "Creating new stack..."
  fi
  
  aws cloudformation deploy \
    --template-file infrastructure/api-stack.yaml \
    --stack-name calendly-api-stack-${ENVIRONMENT} \
    --parameter-overrides \
      Environment=${ENVIRONMENT} \
      StreamingStackName=calendly-streaming-stack-${ENVIRONMENT} \
    --capabilities CAPABILITY_NAMED_IAM \
    --region ${REGION} \
    --no-fail-on-empty-changeset
  
  echo "✓ API stack ready"
fi

# Deploy Orchestration Stack
if [ "$DEPLOY_ORCH" = true ]; then
  echo ""
  echo "========================================="
  echo "Deploying Orchestration Stack"
  echo "========================================="
  
  aws cloudformation deploy \
    --template-file infrastructure/orchestration-stack.yaml \
    --stack-name calendly-orchestration-stack-${ENVIRONMENT} \
    --parameter-overrides \
      Environment=${ENVIRONMENT} \
      MainStackName=calendly-main-stack-${ENVIRONMENT} \
    --capabilities CAPABILITY_NAMED_IAM \
    --region ${REGION} \
    --no-fail-on-empty-changeset
  
  echo "✓ Orchestration stack ready"
fi

# ==========================================
# Summary
# ==========================================
echo ""
echo "========================================="
echo "✅ DEPLOYMENT COMPLETE"
echo "========================================="
echo ""

# Get all endpoints
WEBHOOK_URL=$(aws cloudformation describe-stacks \
  --stack-name calendly-api-stack-${ENVIRONMENT} \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`WebhookURL`].OutputValue' \
  --output text 2>/dev/null || echo "Not deployed yet")

STATE_MACHINE=$(aws cloudformation describe-stacks \
  --stack-name calendly-orchestration-stack-${ENVIRONMENT} \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`StateMachineArn`].OutputValue' \
  --output text 2>/dev/null || echo "Not deployed yet")

SNS_TOPIC=$(aws cloudformation describe-stacks \
  --stack-name calendly-orchestration-stack-${ENVIRONMENT} \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`NotificationTopicArn`].OutputValue' \
  --output text 2>/dev/null || echo "Not deployed yet")

echo "Webhook URL:"
echo "  $WEBHOOK_URL"
echo ""
echo "Step Functions:"
echo "  $STATE_MACHINE"
echo ""
echo "SNS Topic:"
echo "  $SNS_TOPIC"
echo ""
echo "========================================="
echo "Next Steps"
echo "========================================="
echo ""
echo "1. Test webhook:"
echo "   ./scripts/test-webhook.sh"
echo ""
echo "2. Subscribe to notifications:"
echo "   aws sns subscribe \\"
echo "     --topic-arn $SNS_TOPIC \\"
echo "     --protocol email \\"
echo "     --notification-endpoint your@email.com \\"
echo "     --region $REGION"
echo ""
echo "3. Trigger ETL pipeline:"
echo "   aws stepfunctions start-execution \\"
echo "     --state-machine-arn $STATE_MACHINE \\"
echo "     --region $REGION"
echo ""
