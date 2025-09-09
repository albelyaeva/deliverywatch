#!/usr/bin/env bash
set -euo pipefail

ORDER_API="${ORDER_API:-http://localhost:8080}"
CITY="${CITY:-Berlin}"
PRICE="${PRICE:-19.90}"
PROMISED_AT="$(date -u -v+3M +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || gdate -u -d '+3 min' +"%Y-%m-%dT%H:%M:%SZ")"

CUSTOMER_ID="$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)"
echo "Creating order in city=${CITY}, promisedAt=${PROMISED_AT}"

CREATE_PAYLOAD=$(cat <<JSON
{ "customerId": "${CUSTOMER_ID}", "city": "${CITY}", "price": ${PRICE}, "promisedAt": "${PROMISED_AT}" }
JSON
)

ORDER_ID=$(curl -sS -X POST "${ORDER_API}/orders" \
  -H "Content-Type: application/json" \
  -d "${CREATE_PAYLOAD}" | jq -r '.id')

echo "Order created: ${ORDER_ID}"

status() {
  curl -sS -X POST "${ORDER_API}/orders/${ORDER_ID}/status" \
    -H "Content-Type: application/json" \
    -d "{ \"status\": \"$1\" }" > /dev/null
  echo " -> $1"
}

sleep 2; status "CONFIRMED"
sleep 5; status "PREPARING"
sleep 5; status "PICKED_UP"
sleep 8; status "DELIVERED"

echo "Done."
