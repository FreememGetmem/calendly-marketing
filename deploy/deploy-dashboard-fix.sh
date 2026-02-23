#!/bin/bash
# Rebuild and Deploy Fixed Dashboard

REGION="us-west-1"
ENVIRONMENT="prod"
AWS_ACCOUNT_ID="635374934580"

echo "========================================="
echo "Rebuild and Deploy Dashboard Fix"
echo "========================================="
echo ""

# Get ECR repository
ECR_REPO=$(aws cloudformation describe-stacks \
  --stack-name calendly-dashboard-stack-${ENVIRONMENT} \
  --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='ECRRepositoryUri'].OutputValue" \
  --output text)

if [ -z "$ECR_REPO" ]; then
  echo "❌ Could not find ECR repository"
  echo "Dashboard stack may not be deployed"
  exit 1
fi

echo "ECR Repository: $ECR_REPO"
echo ""

# Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region $REGION | \
  docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

echo "✓ Logged in to ECR"
echo ""

# Build Docker image
echo "Building Docker image..."
cd src/dashboard

docker build -t calendly-dashboard:latest .

if [ $? -ne 0 ]; then
  echo "❌ Docker build failed"
  exit 1
fi

echo "✓ Docker image built"
echo ""

# Tag image
echo "Tagging image..."
docker tag calendly-dashboard:latest ${ECR_REPO}:latest
docker tag calendly-dashboard:latest ${ECR_REPO}:$(date +%Y%m%d-%H%M%S)

echo "✓ Image tagged"
echo ""

# Push to ECR
echo "Pushing image to ECR..."
docker push ${ECR_REPO}:latest
docker push ${ECR_REPO}:$(date +%Y%m%d-%H%M%S)

echo "✓ Image pushed to ECR"
echo ""

# Update ECS service
echo "Updating ECS service..."

CLUSTER=$(aws cloudformation describe-stacks \
  --stack-name calendly-dashboard-stack-${ENVIRONMENT} \
  --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='ECSClusterName'].OutputValue" \
  --output text)

SERVICE=$(aws cloudformation describe-stacks \
  --stack-name calendly-dashboard-stack-${ENVIRONMENT} \
  --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='ECSServiceName'].OutputValue" \
  --output text)

if [ -n "$CLUSTER" ] && [ -n "$SERVICE" ]; then
  echo "Forcing new deployment..."
  
  aws ecs update-service \
    --cluster $CLUSTER \
    --service $SERVICE \
    --force-new-deployment \
    --region $REGION \
    --no-cli-pager
  
  echo "✓ ECS service updated"
  echo ""
  
  echo "Waiting for deployment to complete..."
  aws ecs wait services-stable \
    --cluster $CLUSTER \
    --services $SERVICE \
    --region $REGION
  
  echo "✓ Deployment complete"
else
  echo "⚠️  Could not find ECS cluster/service"
  echo "You may need to manually update the task definition"
fi

echo ""

# Get dashboard URL
DASHBOARD_URL=$(aws cloudformation describe-stacks \
  --stack-name calendly-dashboard-stack-${ENVIRONMENT} \
  --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='DashboardURL'].OutputValue" \
  --output text)

echo "========================================="
echo "✅ Dashboard Updated!"
echo "========================================="
echo ""
echo "Dashboard URL: $DASHBOARD_URL"
echo ""
echo "Changes:"
echo "  • Fixed day of week handling in booking time analysis"
echo "  • Fixed heatmap column ordering"
echo "  • Handles both string and numeric day representations"
echo ""
echo "Test the dashboard:"
echo "  Open: $DASHBOARD_URL"
echo ""

cd -
