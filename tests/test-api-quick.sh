#!/bin/bash
# Quick API Webhook Test

API_URL="https://27tr2hk84i.execute-api.us-west-1.amazonaws.com/prod/webhook"

echo "========================================="
echo "Quick API Test"
echo "========================================="
echo ""
echo "Testing: $API_URL"
echo ""

# Create test payload
PAYLOAD=$(cat <<'EOF'
{
  "event": "invitee.created",
  "created_at": "2026-02-20T15:00:00Z",
  "payload": {
    "uri": "https://api.calendly.com/scheduled_events/quick-test-001",
    "email": "quicktest@example.com",
    "name": "Quick Test User",
    "first_name": "Quick",
    "last_name": "Test",
    "created_at": "2026-02-20T15:00:00Z",
    "status": "active",
    "timezone": "America/New_York",
    "tracking": {
      "utm_source": "facebook",
      "utm_medium": "paid",
      "utm_campaign": "test-campaign"
    },
    "scheduled_event": {
      "uri": "https://api.calendly.com/scheduled_events/event-quick-001",
      "name": "30 Min Meeting",
      "start_time": "2026-02-25T14:00:00Z",
      "end_time": "2026-02-25T14:30:00Z",
      "created_at": "2026-02-20T15:00:00Z",
      "status": "active",
      "event_type": "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78",
      "location": {
        "type": "zoom",
        "location": "https://zoom.us/j/123456789"
      },
      "event_memberships": [
        {
          "user_email": "sales@company.com",
          "user_name": "Sales Team"
        }
      ]
    }
  }
}
EOF
)

echo "Sending test webhook..."
echo ""

# Send request
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

HTTP_CODE=$(echo "$RESPONSE" | tail -n 1)
BODY=$(echo "$RESPONSE" | head -n -1)

echo "HTTP Status: $HTTP_CODE"
echo ""
echo "Response:"
echo "$BODY" | C:\Users\mornd\Downloads\jq '.' 2>/dev/null || echo "$BODY"
echo ""

if [ "$HTTP_CODE" = "200" ]; then
  echo "✅ API is working!"
  echo ""
  echo "Next: Wait 30 seconds and check Bronze bucket:"
  echo "  aws s3 ls s3://calendly-bronze-prod-635374934580/calendly_events/ --recursive --region us-west-1"
else
  echo "❌ API returned error status: $HTTP_CODE"
  echo ""
  echo "Check logs:"
  echo "  aws logs tail /aws/lambda/calendly-webhook-receiver-prod --follow --region us-west-1"
fi

echo ""
