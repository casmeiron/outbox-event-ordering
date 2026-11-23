#!/usr/bin/env bash
# Concurrent creates against safe mode. Every order must end SHIPPED, version 3, not regressed.
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8080}"
COUNT="${COUNT:-24}"
PARALLEL="${PARALLEL:-8}"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "waiting for $BASE"
for _ in $(seq 1 90); do
  if curl -fsS "$BASE/q/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$BASE/q/health/ready" >/dev/null

curl -fsS -X PUT "$BASE/admin/mode" \
  -H 'content-type: application/json' \
  -d '{"mode":"safe"}' >/dev/null

echo "creating $COUNT orders with parallelism $PARALLEL"
seq 0 $((COUNT - 1)) | xargs -P "$PARALLEL" -I{} curl -fsS \
  -o "$WORKDIR/{}.json" \
  -X POST "$BASE/orders" \
  -H 'content-type: application/json' \
  -d "{\"customerRef\":\"load-{}\"}"

python3 - "$WORKDIR" "$COUNT" <<'PY'
import json, pathlib, sys
directory = pathlib.Path(sys.argv[1])
count = int(sys.argv[2])
ids = []
for index in range(count):
    body = json.loads((directory / f"{index}.json").read_text())
    ids.append(body["id"])
(directory / "ids.txt").write_text("\n".join(ids) + "\n")
PY

echo "confirming payment and shipment"
while IFS= read -r id; do
  printf '%s\n' "$id"
done < "$WORKDIR/ids.txt" | xargs -P "$PARALLEL" -I{} bash -c '
  curl -fsS -X POST "$0/orders/$1/payment" -H "content-type: application/json" -d "{\"paymentRef\":\"pay-$1\"}" >/dev/null
  curl -fsS -X POST "$0/orders/$1/shipment" -H "content-type: application/json" -d "{\"trackingNumber\":\"track-$1\"}" >/dev/null
' "$BASE" {}

python3 - "$BASE" "$WORKDIR/ids.txt" <<'PY'
import json, sys, time, urllib.request
base, id_file = sys.argv[1], sys.argv[2]
ids = [line.strip() for line in open(id_file) if line.strip()]
deadline = time.time() + 90

def inspect(order_id):
    with urllib.request.urlopen(base + "/orders/" + order_id) as response:
        return json.load(response)

views = []
while time.time() < deadline:
    views = [inspect(order_id) for order_id in ids]
    if all(view["pendingOutbox"] == 0 and len(view["applied"]) == 3 for view in views):
        break
    time.sleep(0.25)
else:
    print("timed out waiting for projections", file=sys.stderr)
    sys.exit(1)

bad = 0
for view in views:
    applied = [event["version"] for event in view["applied"]]
    ok = (
        view["relayVersions"] == [1, 2, 3]
        and applied == [1, 2, 3]
        and view["projectionStatus"] == "SHIPPED"
        and not view["regressed"]
        and view["relayKeys"] == [view["id"], view["id"], view["id"]]
    )
    if not ok:
        bad += 1
        print(f"OUT OF ORDER {view['id']} relay={view['relayVersions']} applied={applied} status={view['projectionStatus']}")
print(f"checked={len(views)} out_of_order={bad}")
if bad:
    sys.exit(1)
PY
