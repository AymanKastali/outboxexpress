#!/usr/bin/env bash
# End-to-end proof against a running stack: one order in, the same order out.
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
KEY="smoke-$(date +%s%N)"
BODY='{"customer_email":"a@b.com","item_sku":"SKU-1","quantity":2}'

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

post() {
  curl -fsS -X POST "$BASE_URL/orders" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $KEY" \
    -d "$BODY"
}

health=$(curl -fsS "$BASE_URL/healthz")
[ "$health" = '{"status":"ok"}' ] || fail "healthz returned $health"
echo "ok   healthz"

created=$(post)
replayed=$(post)
# Byte-for-byte, not field-by-field: a replay hands back the stored response, and
# comparing parsed fields would not notice if it were rebuilt.
if [ "$created" != "$replayed" ]; then
  printf 'FAIL: replay differed\n  first:  %s\n  second: %s\n' "$created" "$replayed" >&2
  exit 1
fi
echo "ok   replayed key returns the stored response byte for byte"

order_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])' <<<"$created")
fetched=$(curl -fsS "$BASE_URL/orders/$order_id")
# Both values go in through the environment: a heredoc already owns stdin here.
ORDER_ID="$order_id" FETCHED="$fetched" python3 - <<'PY' || fail "read-back mismatch"
import json, os

body = json.loads(os.environ["FETCHED"])
assert body["order_id"] == os.environ["ORDER_ID"], body
assert body["status"] == "CREATED", body
PY
echo "ok   order reads back as CREATED"

missing="00000000-0000-7000-8000-000000000000"
code=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/orders/$missing")
[ "$code" = "404" ] || fail "unknown order returned $code, not 404"
echo "ok   unknown order is 404"

echo "smoke passed"
