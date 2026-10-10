#!/usr/bin/env bash
# Checks the API against the numbers that the example events (python -m order_analytics.seed)
# are known to produce. Needs curl and jq.
#
#   ./scripts/smoke-test.sh                      # http://localhost:8090
#   BASE_URL=http://localhost:9000 ./scripts/smoke-test.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8090}"

fail() {
  echo "FAIL  $1"
  failures=$((failures + 1))
}

# expect_json <path> <expected json>: numbers are compared as numbers, so 7871.9 equals 7871.90.
expect_json() {
  local path="$1" expected="$2" actual
  actual="$(curl -fsS "$BASE_URL$path")" || { fail "GET $path did not answer 200"; return; }
  if jq -en --argjson actual "$actual" --argjson expected "$expected" '$actual == $expected' >/dev/null; then
    echo "ok    GET $path"
  else
    fail "GET $path"
    echo "      expected: $(jq -c . <<<"$expected")"
    echo "      actual:   $(jq -c . <<<"$actual")"
  fi
}

expect_status() {
  local path="$1" expected="$2" actual
  actual="$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL$path")"
  if [ "$actual" = "$expected" ]; then
    echo "ok    GET $path -> $expected"
  else
    fail "GET $path answered $actual, expected $expected"
  fi
}

check_everything() {
  failures=0

  expect_json "/health" '{"status": "UP", "database": "UP", "consumer": "UP"}'

  expect_json "/analytics/summary" '{
    "totalOrders": 16,
    "confirmed": 9,
    "cancelled": 5,
    "inProgress": 2,
    "confirmedRevenue": 7871.90,
    "approvalRate": 0.6429,
    "averageSecondsToConfirm": 6.0,
    "lastEventAt": "2026-10-09T09:31:00Z"
  }'

  expect_json "/analytics/orders-by-status" '[
    {"status": "PENDING", "count": 1},
    {"status": "STOCK_RESERVED", "count": 1},
    {"status": "CONFIRMED", "count": 9},
    {"status": "CANCELLED", "count": 5}
  ]'

  expect_json "/analytics/revenue?from=2026-10-05&to=2026-10-09&granularity=day" '[
    {"bucket": "2026-10-05", "revenue": 1018.70, "orders": 2},
    {"bucket": "2026-10-06", "revenue": 2328.90, "orders": 2},
    {"bucket": "2026-10-07", "revenue": 3158.60, "orders": 2},
    {"bucket": "2026-10-08", "revenue": 1105.90, "orders": 2},
    {"bucket": "2026-10-09", "revenue": 259.80, "orders": 1}
  ]'

  expect_json "/analytics/revenue?from=2026-10-07&to=2026-10-07&granularity=hour" '[
    {"bucket": "2026-10-07T08:00:00Z", "revenue": 609.70, "orders": 1},
    {"bucket": "2026-10-07T11:00:00Z", "revenue": 2548.90, "orders": 1}
  ]'

  expect_json "/analytics/cancellations" '[
    {"reason": "OUT_OF_STOCK", "count": 3},
    {"reason": "PAYMENT_REFUSED", "count": 2}
  ]'

  expect_json "/analytics/top-products?limit=3" '[
    {"productId": "22222222-2222-2222-2222-222222222222", "productName": "Mouse Sem Fio", "quantity": 6, "revenue": 779.40},
    {"productId": "11111111-1111-1111-1111-111111111111", "productName": "Teclado Mecânico", "quantity": 5, "revenue": 1749.50},
    {"productId": "66666666-6666-6666-6666-666666666666", "productName": "Webcam Full HD", "quantity": 5, "revenue": 945.00}
  ]'

  expect_status "/analytics/revenue?from=2026-10-09&to=2026-10-05" 422
  expect_status "/analytics/revenue?granularity=week" 422
  expect_status "/docs" 200

  csv="$(curl -fsS "$BASE_URL/analytics/reports/orders.csv" | tr -d '\r')"
  header="orderId,status,customerId,customerName,customerEmail,totalAmount,itemCount,cancellationReason,cancellationDetail,createdAt,finishedAt"
  if [ "$(head -n 1 <<<"$csv")" = "$header" ] \
    && [ "$(wc -l <<<"$csv")" -eq 17 ] \
    && grep -q ',CONFIRMED,c-1004,João Araújo,joao@exemplo.com,2199.00,1,,,2026-10-06T10:20:00Z,2026-10-06T10:20:05Z$' <<<"$csv"; then
    echo "ok    GET /analytics/reports/orders.csv"
  else
    fail "GET /analytics/reports/orders.csv"
    echo "$csv" | head -n 5
  fi

  if [ "$failures" -gt 0 ]; then
    echo "$failures check(s) failed"
    return 1
  fi
  echo "all checks passed"
}

# The consumer may still be reading what the seed has just published, and no single number
# tells that it has finished: the three topics are read in no particular order. So the whole
# comparison is repeated until it holds, and only the last attempt is shown.
echo "checking the numbers of the 16 example orders at $BASE_URL ..."
deadline=$((SECONDS + 60))
until report="$(check_everything 2>&1)"; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "$report"
    exit 1
  fi
  sleep 1
done
echo "$report"
