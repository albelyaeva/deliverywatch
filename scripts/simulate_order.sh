#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://localhost:8080"
METRICS_URL="http://localhost:8081"

for city in Berlin Munich Hamburg Frankfurt; do
  echo "Creating orders for $city..."

  for i in {1..5}; do
    price=$((RANDOM % 40 + 15))
    promised_at="$(date -u -v+30M +'%Y-%m-%dT%H:%M:%SZ')"   # macOS/BSD date
    customer_id="$(uuidgen)"

    # Build JSON safely with jq
    payload="$(jq -n \
      --arg customerId "$customer_id" \
      --arg city "$city" \
      --arg promisedAt "$promised_at" \
      --argjson price "$price" \
      '{customerId:$customerId, city:$city, price:$price, promisedAt:$promisedAt}')"

    ORDER_JSON="$(curl -sS -X POST "$BASE_URL/orders" \
      -H "Content-Type: application/json" \
      -d "$payload")"

    ORDER_ID="$(echo "$ORDER_JSON" | jq -r '.id')"

    sleep $((RANDOM % 3 + 1))

    curl -sS -X POST "$BASE_URL/orders/$ORDER_ID/status" \
      -H "Content-Type: application/json" \
      -d '{"status":"DELIVERED"}' >/dev/null

    echo "$city order $i completed"
  done
done

echo "Waiting for metrics processing..."
sleep 10

curl -sS "$METRICS_URL/metrics/sla" | jq
curl -sS "$METRICS_URL/metrics/delivery-time" | jq

echo "Done."
