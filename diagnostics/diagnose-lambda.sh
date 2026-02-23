#!/bin/bash
# Diagnose Lambda Functions

REGION="us-west-1"
ENVIRONMENT="prod"

echo "========================================="
echo "Lambda Functions Diagnostic"
echo "========================================="
echo ""

echo "1. Checking for Lambda functions..."
echo ""

# List all Lambda functions
FUNCTIONS=$(aws lambda list-functions \
  --region $REGION \
  --query 'Functions[?contains(FunctionName, `calendly`)].FunctionName' \
  --output text)

if [ -z "$FUNCTIONS" ]; then
  echo "❌ No Calendly Lambda functions found!"
  echo ""
  echo "Expected functions:"
  echo "  - calendly-webhook-receiver-${ENVIRONMENT}"
  echo "  - calendly-kinesis-processor-${ENVIRONMENT}"
  echo "  - calendly-orchestrator-${ENVIRONMENT}"
  echo ""
  echo "These functions should have been created by CloudFormation."
else
  echo "✓ Found Lambda functions:"
  for func in $FUNCTIONS; do
    echo "  - $func"
  done
fi

echo ""
echo "2. Checking CloudFormation stacks..."
echo ""

# Check for streaming stack
STREAMING_STACK="calendly-streaming-stack-${ENVIRONMENT}"
if aws cloudformation describe-stacks --stack-name $STREAMING_STACK --region $REGION &>/dev/null; then
  STATUS=$(aws cloudformation describe-stacks \
    --stack-name $STREAMING_STACK \
    --region $REGION \
    --query 'Stacks[0].StackStatus' \
    --output text)
  echo "✓ Streaming Stack: $STATUS"
  
  # Get outputs
  aws cloudformation describe-stacks \
    --stack-name $STREAMING_STACK \
    --region $REGION \
    --query 'Stacks[0].Outputs[]' \
    --output table
else
  echo "❌ Streaming Stack not found: $STREAMING_STACK"
  echo ""
  echo "This stack should contain:"
  echo "  - Kinesis Data Stream"
  echo "  - Lambda processor function"
  echo "  - IAM roles"
fi

echo ""
echo "3. Checking for Kinesis stream..."
echo ""

STREAM_NAME="calendly-events-stream-${ENVIRONMENT}"
if aws kinesis describe-stream --stream-name $STREAM_NAME --region $REGION &>/dev/null; then
  STATUS=$(aws kinesis describe-stream \
    --stream-name $STREAM_NAME \
    --region $REGION \
    --query 'StreamDescription.StreamStatus' \
    --output text)
  echo "✓ Kinesis Stream: $STREAM_NAME ($STATUS)"
else
  echo "❌ Kinesis Stream not found: $STREAM_NAME"
fi

echo ""
echo "4. Current Pipeline Status:"
echo ""

# Check main stack
MAIN_STACK="calendly-main-stack-${ENVIRONMENT}"
if aws cloudformation describe-stacks --stack-name $MAIN_STACK --region $REGION &>/dev/null; then
  echo "✓ Main Stack exists"
else
  echo "❌ Main Stack missing"
fi

# Check API stack
API_STACK="calendly-api-stack-${ENVIRONMENT}"
if aws cloudformation describe-stacks --stack-name $API_STACK --region $REGION &>/dev/null; then
  echo "✓ API Stack exists"
else
  echo "❌ API Stack missing (needed for webhook receiver)"
fi

echo ""
echo "========================================="
echo "Summary"
echo "========================================="
echo ""

if [ -z "$FUNCTIONS" ]; then
  echo "⚠️  Lambda functions not deployed"
  echo ""
  echo "To fix this, you need to:"
  echo "1. Deploy the streaming stack with Lambda functions"
  echo "2. Or check if the existing pipeline uses different names"
  echo ""
  echo "Check your current setup:"
  aws lambda list-functions --region $REGION --query 'Functions[].FunctionName' --output table
fi

echo ""
