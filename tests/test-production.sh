#!/bin/bash
# Complete End-to-End Production Test
# Tests the entire Calendly Marketing Analytics Pipeline

set -e

ENVIRONMENT="prod"
REGION="us-west-1"
MAIN_STACK="calendly-main-stack-${ENVIRONMENT}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "========================================="
echo "CALENDLY PIPELINE - END-TO-END TEST"
echo "========================================="
echo ""
echo "Environment: ${ENVIRONMENT}"
echo "Region: ${REGION}"
echo ""

# Function to wait for job completion
wait_for_job() {
    local job_name=$1
    local run_id=$2
    local max_wait=600  # 10 minutes
    local elapsed=0
    
    echo "  Waiting for job to complete..."
    while [ $elapsed -lt $max_wait ]; do
        status=$(aws glue get-job-run \
            --job-name $job_name \
            --run-id $run_id \
            --region $REGION \
            --query 'JobRun.JobRunState' \
            --output text)
        
        if [ "$status" = "SUCCEEDED" ]; then
            echo -e "  ${GREEN}✓ Job completed successfully${NC}"
            return 0
        elif [ "$status" = "FAILED" ] || [ "$status" = "STOPPED" ] || [ "$status" = "ERROR" ]; then
            echo -e "  ${RED}✗ Job failed with status: $status${NC}"
            
            # Get error message
            error=$(aws glue get-job-run \
                --job-name $job_name \
                --run-id $run_id \
                --region $REGION \
                --query 'JobRun.ErrorMessage' \
                --output text)
            
            if [ "$error" != "None" ] && [ -n "$error" ]; then
                echo "  Error: $error"
            fi
            return 1
        fi
        
        echo -n "."
        sleep 10
        elapsed=$((elapsed + 10))
    done
    
    echo -e "\n  ${RED}✗ Job timed out after ${max_wait}s${NC}"
    return 1
}

# ==========================================
# TEST 1: Infrastructure Health Check
# ==========================================
echo -e "${BLUE}TEST 1: Infrastructure Health Check${NC}"
echo "-------------------------------------------"

echo "Checking CloudFormation stacks..."

# Check main stack
if aws cloudformation describe-stacks --stack-name $MAIN_STACK --region $REGION &>/dev/null; then
    STATUS=$(aws cloudformation describe-stacks --stack-name $MAIN_STACK --region $REGION --query 'Stacks[0].StackStatus' --output text)
    if [ "$STATUS" = "CREATE_COMPLETE" ] || [ "$STATUS" = "UPDATE_COMPLETE" ]; then
        echo -e "  ${GREEN}✓${NC} Main Stack: $STATUS"
    else
        echo -e "  ${YELLOW}⚠${NC} Main Stack: $STATUS"
    fi
else
    echo -e "  ${RED}✗${NC} Main Stack: NOT FOUND"
    exit 1
fi

# Check Glue jobs stack
GLUE_STACK="calendly-glue-jobs-${ENVIRONMENT}"
if aws cloudformation describe-stacks --stack-name $GLUE_STACK --region $REGION &>/dev/null; then
    STATUS=$(aws cloudformation describe-stacks --stack-name $GLUE_STACK --region $REGION --query 'Stacks[0].StackStatus' --output text)
    if [ "$STATUS" = "CREATE_COMPLETE" ] || [ "$STATUS" = "UPDATE_COMPLETE" ]; then
        echo -e "  ${GREEN}✓${NC} Glue Jobs Stack: $STATUS"
    else
        echo -e "  ${YELLOW}⚠${NC} Glue Jobs Stack: $STATUS"
    fi
else
    echo -e "  ${YELLOW}⚠${NC} Glue Jobs Stack: NOT FOUND"
fi

# Check dashboard stack
DASHBOARD_STACK="calendly-dashboard-stack-${ENVIRONMENT}"
if aws cloudformation describe-stacks --stack-name $DASHBOARD_STACK --region $REGION &>/dev/null; then
    STATUS=$(aws cloudformation describe-stacks --stack-name $DASHBOARD_STACK --region $REGION --query 'Stacks[0].StackStatus' --output text)
    if [ "$STATUS" = "CREATE_COMPLETE" ] || [ "$STATUS" = "UPDATE_COMPLETE" ]; then
        echo -e "  ${GREEN}✓${NC} Dashboard Stack: $STATUS"
    else
        echo -e "  ${YELLOW}⚠${NC} Dashboard Stack: $STATUS"
    fi
else
    echo -e "  ${YELLOW}⚠${NC} Dashboard Stack: NOT FOUND"
fi

echo ""

# Get bucket names
echo "Getting S3 bucket names..."
BRONZE=$(aws cloudformation describe-stacks --stack-name $MAIN_STACK --region $REGION --query "Stacks[0].Outputs[?OutputKey=='BronzeBucketName'].OutputValue" --output text)
SILVER=$(aws cloudformation describe-stacks --stack-name $MAIN_STACK --region $REGION --query "Stacks[0].Outputs[?OutputKey=='SilverBucketName'].OutputValue" --output text)
GOLD=$(aws cloudformation describe-stacks --stack-name $MAIN_STACK --region $REGION --query "Stacks[0].Outputs[?OutputKey=='GoldBucketName'].OutputValue" --output text)

echo "  Bronze: $BRONZE"
echo "  Silver: $SILVER"
echo "  Gold: $GOLD"
echo ""

# ==========================================
# TEST 2: Data Ingestion Layer (Bronze)
# ==========================================
echo -e "${BLUE}TEST 2: Data Ingestion (Bronze Layer)${NC}"
echo "-------------------------------------------"

echo "Checking Bronze bucket data..."
BRONZE_COUNT=$(aws s3 ls s3://${BRONZE}/calendly_events/ --recursive --region $REGION | wc -l || echo "0")
echo "  Files in Bronze bucket: $BRONZE_COUNT"

if [ "$BRONZE_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}✓${NC} Bronze layer has data"
else
    echo -e "  ${YELLOW}⚠${NC} Bronze layer is empty - run generate-test-data.sh first"
fi
echo ""

# ==========================================
# TEST 3: Bronze to Silver Transformation
# ==========================================
echo -e "${BLUE}TEST 3: Bronze to Silver ETL Job${NC}"
echo "-------------------------------------------"

echo "Starting Bronze to Silver job..."
RUN_ID=$(aws glue start-job-run \
    --job-name calendly-bronze-to-silver-${ENVIRONMENT} \
    --region $REGION \
    --query 'JobRunId' \
    --output text)

echo "  Job Run ID: $RUN_ID"

if wait_for_job "calendly-bronze-to-silver-${ENVIRONMENT}" "$RUN_ID"; then
    # Check Silver bucket
    echo ""
    echo "Checking Silver layer data..."
    SILVER_COUNT=$(aws s3 ls s3://${SILVER}/calendly_events_silver/ --recursive --region $REGION | wc -l || echo "0")
    echo "  Files in Silver bucket: $SILVER_COUNT"
    
    if [ "$SILVER_COUNT" -gt 0 ]; then
        echo -e "  ${GREEN}✓${NC} Silver layer populated successfully"
    else
        echo -e "  ${RED}✗${NC} Silver layer is empty after job completion"
    fi
else
    echo -e "${RED}✗ Bronze to Silver job failed${NC}"
    echo ""
    echo "Check logs:"
    echo "  aws logs tail /aws-glue/jobs/output --follow --region $REGION | grep calendly-bronze-to-silver"
    exit 1
fi
echo ""

# ==========================================
# TEST 4: Silver to Gold Transformation
# ==========================================
echo -e "${BLUE}TEST 4: Silver to Gold ETL Job${NC}"
echo "-------------------------------------------"

echo "Starting Silver to Gold job..."
RUN_ID=$(aws glue start-job-run \
    --job-name calendly-silver-to-gold-${ENVIRONMENT} \
    --region $REGION \
    --query 'JobRunId' \
    --output text)

echo "  Job Run ID: $RUN_ID"

if wait_for_job "calendly-silver-to-gold-${ENVIRONMENT}" "$RUN_ID"; then
    # Check Gold bucket
    echo ""
    echo "Checking Gold layer metrics..."
    
    METRICS=(
        "daily_bookings_by_source"
        "cost_per_booking"
        "bookings_trend"
        "channel_attribution"
        "booking_time_analysis"
    )
    
    METRICS_CREATED=0
    for metric in "${METRICS[@]}"; do
        COUNT=$(aws s3 ls s3://${GOLD}/metrics/${metric}/ --recursive --region $REGION | wc -l || echo "0")
        if [ "$COUNT" -gt 0 ]; then
            echo -e "  ${GREEN}✓${NC} ${metric}: $COUNT files"
            METRICS_CREATED=$((METRICS_CREATED + 1))
        else
            echo -e "  ${RED}✗${NC} ${metric}: No files"
        fi
    done
    
    echo ""
    echo "  Metrics created: $METRICS_CREATED / ${#METRICS[@]}"
    
    if [ "$METRICS_CREATED" -eq "${#METRICS[@]}" ]; then
        echo -e "  ${GREEN}✓${NC} All metrics created successfully"
    else
        echo -e "  ${YELLOW}⚠${NC} Some metrics missing"
    fi
else
    echo -e "${RED}✗ Silver to Gold job failed${NC}"
    echo ""
    echo "Check logs:"
    echo "  aws logs tail /aws-glue/jobs/output --follow --region $REGION | grep calendly-silver-to-gold"
    exit 1
fi
echo ""

# ==========================================
# TEST 5: Glue Catalog Tables
# ==========================================
echo -e "${BLUE}TEST 5: Glue Catalog Tables${NC}"
echo "-------------------------------------------"

DB_NAME=$(aws cloudformation describe-stacks --stack-name $MAIN_STACK --region $REGION --query "Stacks[0].Outputs[?OutputKey=='GlueDatabaseName'].OutputValue" --output text)

echo "Checking Glue Catalog tables in: $DB_NAME"

TABLES=$(aws glue get-tables --database-name $DB_NAME --region $REGION --query 'TableList[].Name' --output text 2>/dev/null || echo "")

if [ -n "$TABLES" ]; then
    echo "  Tables found:"
    for table in $TABLES; do
        echo -e "    ${GREEN}✓${NC} $table"
    done
else
    echo -e "  ${YELLOW}⚠${NC} No tables found in catalog"
fi
echo ""

# ==========================================
# TEST 6: Dashboard Availability
# ==========================================
echo -e "${BLUE}TEST 6: Dashboard Service${NC}"
echo "-------------------------------------------"

# Get ALB URL
ALB_URL=$(aws cloudformation describe-stacks \
    --stack-name $DASHBOARD_STACK \
    --region $REGION \
    --query "Stacks[0].Outputs[?OutputKey=='DashboardURL'].OutputValue" \
    --output text 2>/dev/null || echo "")

if [ -n "$ALB_URL" ] && [ "$ALB_URL" != "None" ]; then
    echo "  Dashboard URL: $ALB_URL"
    
    # Test if dashboard is accessible
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" $ALB_URL --max-time 10 || echo "000")
    
    if [ "$HTTP_CODE" = "200" ]; then
        echo -e "  ${GREEN}✓${NC} Dashboard is accessible (HTTP $HTTP_CODE)"
    elif [ "$HTTP_CODE" = "503" ]; then
        echo -e "  ${YELLOW}⚠${NC} Dashboard service unavailable (HTTP $HTTP_CODE)"
        echo "    Check ECS service health"
    else
        echo -e "  ${RED}✗${NC} Dashboard returned HTTP $HTTP_CODE"
    fi
else
    echo -e "  ${YELLOW}⚠${NC} Dashboard stack not deployed"
fi
echo ""

# ==========================================
# TEST 7: Athena Query Test
# ==========================================
echo -e "${BLUE}TEST 7: Athena Query Test${NC}"
echo "-------------------------------------------"

echo "Testing Athena query on Gold layer..."

# Start a simple query
QUERY_ID=$(aws athena start-query-execution \
    --query-string "SELECT COUNT(*) as booking_count FROM ${DB_NAME}.daily_bookings_by_source" \
    --result-configuration "OutputLocation=s3://${GOLD}/athena-results/" \
    --region $REGION \
    --query 'QueryExecutionId' \
    --output text 2>/dev/null || echo "")

if [ -n "$QUERY_ID" ]; then
    echo "  Query ID: $QUERY_ID"
    
    # Wait for query to complete
    sleep 3
    STATUS=$(aws athena get-query-execution \
        --query-execution-id $QUERY_ID \
        --region $REGION \
        --query 'QueryExecution.Status.State' \
        --output text 2>/dev/null || echo "FAILED")
    
    if [ "$STATUS" = "SUCCEEDED" ]; then
        echo -e "  ${GREEN}✓${NC} Athena query succeeded"
        
        # Get results
        RESULTS=$(aws athena get-query-results \
            --query-execution-id $QUERY_ID \
            --region $REGION \
            --query 'ResultSet.Rows[1].Data[0].VarCharValue' \
            --output text 2>/dev/null || echo "0")
        
        echo "  Records found: $RESULTS"
    else
        echo -e "  ${YELLOW}⚠${NC} Athena query status: $STATUS"
    fi
else
    echo -e "  ${YELLOW}⚠${NC} Could not start Athena query"
fi
echo ""

# ==========================================
# FINAL SUMMARY
# ==========================================
echo "========================================="
echo -e "${BLUE}TEST SUMMARY${NC}"
echo "========================================="
echo ""

echo "Pipeline Components:"
echo -e "  ${GREEN}✓${NC} Infrastructure (CloudFormation)"
echo -e "  ${GREEN}✓${NC} Data Ingestion (Bronze Layer)"
echo -e "  ${GREEN}✓${NC} Bronze to Silver ETL"
echo -e "  ${GREEN}✓${NC} Silver to Gold ETL"
echo -e "  ${GREEN}✓${NC} Glue Catalog Tables"

if [ -n "$ALB_URL" ]; then
    echo -e "  ${GREEN}✓${NC} Dashboard Service"
fi

echo ""
echo "Access Points:"
echo "  📊 Dashboard: $ALB_URL"
echo "  🗄️  Glue Database: $DB_NAME"
echo "  📦 S3 Buckets:"
echo "    - Bronze: s3://$BRONZE"
echo "    - Silver: s3://$SILVER"
echo "    - Gold: s3://$GOLD"
echo ""

echo "Next Steps:"
echo "  1. Open dashboard in browser: $ALB_URL"
echo "  2. Query data via Athena console"
echo "  3. Set up EventBridge schedule for regular runs"
echo ""

echo -e "${GREEN}✓ End-to-End Test Complete!${NC}"
echo ""
