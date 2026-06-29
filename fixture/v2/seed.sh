#!/usr/bin/env bash
# Seed the InfluxDB 2.7 fixture with a known dataset across two buckets, all
# field types, and a pre-epoch point — mirroring the v1 fixture so we can assert
# the same reconstructed output.
set -euo pipefail

URL="${URL:-http://localhost:8087}"
TOKEN="${TOKEN:-tsm2arc-dev-token}"
ORG="${ORG:-basekick}"

write() { # write <bucket> <line-protocol...>
  local bucket="$1"; shift
  curl -sS -XPOST "${URL}/api/v2/write?org=${ORG}&bucket=${bucket}&precision=ns" \
    -H "Authorization: Token ${TOKEN}" --data-binary "$*"
}

echo "==> creating second bucket 'sensors'"
curl -sS -XPOST "${URL}/api/v2/buckets" -H "Authorization: Token ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d "{\"orgID\":\"$(curl -sS "${URL}/api/v2/orgs?org=${ORG}" -H "Authorization: Token ${TOKEN}" | grep -o '"id":"[a-f0-9]*"' | head -1 | cut -d'"' -f4)\",\"name\":\"sensors\",\"retentionRules\":[]}" >/dev/null || true

echo "==> telemetry: multi-field, all types"
write telemetry 'cpu,host=node-a,region=us-west usage_idle=98.5,cores=8i 1700000000000000000'
write telemetry 'cpu,host=node-a,region=us-west usage_idle=97.2,cores=8i 1700000001000000000'
write telemetry 'cpu,host=node-b,region=eu-central usage_idle=51.0,cores=16i 1700000000000000000'
write telemetry 'status,host=node-a healthy=true,note="all systems nominal",reboots=3u 1700000000000000000'
write telemetry 'legacy,host=mainframe value=42.0 -317001600000000000'

echo "==> sensors: tagless + tagged"
write sensors 'pressure value=1013.25 1700000000000000000'
write sensors 'pressure value=1012.80 1700000001000000000'
write sensors 'altitude,craft=falcon meters=11000.5 1700000000000000000'

echo
echo "==> seeded. Wait a few seconds for the cold-snapshot to write .tsm files."
echo "    Bucket id<->name mapping is in data/influxd.bolt"
