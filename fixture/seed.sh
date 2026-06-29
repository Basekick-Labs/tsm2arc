#!/usr/bin/env bash
# Seed the InfluxDB 1.8 fixture with a KNOWN, assertable dataset.
#
# Design goals — every tricky case tsm2arc's TSM reader must handle:
#   • multiple databases          → exercises per-DB → Arc-namespace routing
#   • multi-field points          → exercises field-rejoin by (series, ts)
#   • a tagless measurement       → exercises the no-tags path (no arc:tags dedupe)
#   • all field types             → float, integer, ufinteger, boolean, string codecs
#   • a pre-epoch (1989) point    → exercises pre-1970 timestamps end to end
#   • known exact values          → we assert extracted output against THIS file
#
# After running, do a clean restart to flush WAL → TSM:
#   docker compose restart influxdb
#
# All timestamps are explicit nanosecond Unix epoch (UTC). Values are chosen to
# be easily recognizable in extracted line protocol.
set -euo pipefail

INFLUX_URL="${INFLUX_URL:-http://localhost:8086}"

write() {  # write <db> <line-protocol...>
  local db="$1"; shift
  curl -sS -XPOST "${INFLUX_URL}/write?db=${db}&precision=ns" --data-binary "$*"
}

q() { curl -sS -XPOST "${INFLUX_URL}/query" --data-urlencode "q=$1" >/dev/null; }

echo "==> creating databases"
q 'CREATE DATABASE telemetry'
q 'CREATE DATABASE sensors'

# ---------------------------------------------------------------------------
# DB 1: telemetry
# ---------------------------------------------------------------------------

echo "==> telemetry: multi-field, multi-tag float/int points"
# cpu: two fields (float + integer) on the SAME point → must rejoin into one LP line.
# host tag present → tag-bearing series (arc:tags dedupe eligible).
# timestamp 1700000000000000000 = 2023-11-14T22:13:20Z
write telemetry 'cpu,host=node-a,region=us-west usage_idle=98.5,cores=8i 1700000000000000000'
write telemetry 'cpu,host=node-a,region=us-west usage_idle=97.2,cores=8i 1700000001000000000'
write telemetry 'cpu,host=node-b,region=eu-central usage_idle=51.0,cores=16i 1700000000000000000'

echo "==> telemetry: boolean + string + unsigned fields"
# status: boolean field; note: string field; count: unsigned integer field (1.8 supports 'u' suffix)
write telemetry 'status,host=node-a healthy=true,note="all systems nominal",reboots=3u 1700000000000000000'
write telemetry 'status,host=node-b healthy=false,note="degraded link",reboots=12u 1700000000000000000'

echo "==> telemetry: PRE-EPOCH point (1959-12-16T00:00:00Z = -317001600 s)"
# -317001600 s * 1e9 = -317001600000000000 ns (1959, before the Unix epoch).
# Tests pre-1970 negative-timestamp handling end to end.
write telemetry 'legacy,host=mainframe value=42.0 -317001600000000000'

# ---------------------------------------------------------------------------
# DB 2: sensors  (separate namespace → separate Arc database)
# ---------------------------------------------------------------------------

echo "==> sensors: TAGLESS measurement (no tags at all)"
# No tags → no arc:tags metadata in Arc → compaction will NOT dedupe these.
# Exactly the case that makes strict per-chunk resume mandatory.
write sensors 'pressure value=1013.25 1700000000000000000'
write sensors 'pressure value=1012.80 1700000001000000000'
write sensors 'pressure value=1011.95 1700000002000000000'

echo "==> sensors: a single-field tagged point for contrast"
write sensors 'altitude,craft=falcon meters=11000.5 1700000000000000000'

echo
echo "==> seeded. Now flush WAL → TSM with a clean restart:"
echo "    docker compose restart influxdb"
echo
echo "==> expected extraction (the assertion oracle):"
cat <<'EOF'
  database telemetry:
    cpu,host=node-a,region=us-west usage_idle=98.5,cores=8i 1700000000000000000
    cpu,host=node-a,region=us-west usage_idle=97.2,cores=8i 1700000001000000000
    cpu,host=node-b,region=eu-central usage_idle=51,cores=16i 1700000000000000000
    status,host=node-a healthy=true,note="all systems nominal",reboots=3u 1700000000000000000
    status,host=node-b healthy=false,note="degraded link",reboots=12u 1700000000000000000
    legacy,host=mainframe value=42 -317001600000000000   # 1959-12-16, pre-epoch
  database sensors:
    pressure value=1013.25 1700000000000000000
    pressure value=1012.8 1700000001000000000
    pressure value=1011.95 1700000002000000000
    altitude,craft=falcon meters=11000.5 1700000000000000000
  total: 10 points across 2 databases, 5 measurements
EOF
