#!/usr/bin/env bash
# Run a naive burst (state goes backwards), then a safe burst (it does not).
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8080}"

wait_ready() {
  echo "waiting for $BASE"
  for _ in $(seq 1 90); do
    if curl -fsS "$BASE/q/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "application did not become ready" >&2
  exit 1
}

set_mode() {
  curl -fsS -X PUT "$BASE/admin/mode" \
    -H 'content-type: application/json' \
    -d "{\"mode\":\"$1\"}" >/dev/null
  echo "mode=$1"
}

burst() {
  local count="$1"
  local prefix="$2"
  curl -fsS -X POST "$BASE/demo/burst" \
    -H 'content-type: application/json' \
    -d "{\"orders\":${count},\"customerPrefix\":\"${prefix}\"}"
}

wait_ready
set_mode naive
naive_json="$(burst 6 naive-demo)"
echo "naive burst accepted"
python3 - "$BASE" "$naive_json" naive <<'PY'
import json, sys, time, urllib.request
base, payload, expected = sys.argv[1], sys.argv[2], sys.argv[3]
ids = json.loads(payload)["orderIds"]
deadline = time.time() + 60

def inspect(order_id):
    with urllib.request.urlopen(base + "/orders/" + order_id) as response:
        return json.load(response)

views = []
while time.time() < deadline:
    views = [inspect(order_id) for order_id in ids]
    if all(view["pendingOutbox"] == 0 and len(view["applied"]) == 3 for view in views):
        break
    time.sleep(0.2)
else:
    print("timed out waiting for the naive burst", file=sys.stderr)
    sys.exit(1)

regressed = [view for view in views if view["regressed"]]
print(f"naive orders={len(views)} regressed={len(regressed)}")
for view in views:
    applied = [event["version"] for event in view["applied"]]
    print(f"  {view['id']} relay={view['relayVersions']} applied={applied} status={view['projectionStatus']} regressed={view['regressed']}")
if expected == "naive" and not regressed:
    print("expected the naive burst to apply at least one order backwards", file=sys.stderr)
    sys.exit(1)
PY

set_mode safe
safe_json="$(burst 6 safe-demo)"
echo "safe burst accepted"
python3 - "$BASE" "$safe_json" <<'PY'
import json, sys, time, urllib.request
base, payload = sys.argv[1], sys.argv[2]
ids = json.loads(payload)["orderIds"]
deadline = time.time() + 60

def inspect(order_id):
    with urllib.request.urlopen(base + "/orders/" + order_id) as response:
        return json.load(response)

while time.time() < deadline:
    views = [inspect(order_id) for order_id in ids]
    if all(view["pendingOutbox"] == 0 and len(view["applied"]) == 3 for view in views):
        break
    time.sleep(0.2)
else:
    print("timed out waiting for the safe burst", file=sys.stderr)
    sys.exit(1)

failed = False
for view in views:
    applied = [event["version"] for event in view["applied"]]
    print(f"  {view['id']} relay={view['relayVersions']} applied={applied} status={view['projectionStatus']} regressed={view['regressed']}")
    if view["relayVersions"] != [1, 2, 3] or applied != [1, 2, 3] or view["projectionStatus"] != "SHIPPED" or view["regressed"]:
        failed = True
    if view["relayKeys"] != [view["id"], view["id"], view["id"]]:
        failed = True
if failed:
    print("safe burst did not keep business order", file=sys.stderr)
    sys.exit(1)
print("safe burst kept every order at SHIPPED in version order 1,2,3")
PY
