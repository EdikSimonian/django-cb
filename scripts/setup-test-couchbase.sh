#!/usr/bin/env bash
# Initialize a local Couchbase container for running tests.
# Usage: ./scripts/setup-test-couchbase.sh
#
# Expects Couchbase to be running on localhost (ports 8091-8097, 11210-11211).
# Start it with: docker compose -f docker-compose.test.yml up -d

set -euo pipefail

CB_HOST="${CB_HOST:-localhost}"
CB_USER="${CB_USERNAME:-Administrator}"
CB_PASS="${CB_PASSWORD:-password}"
CB_BUCKET="${CB_BUCKET:-testbucket}"

echo "==> Waiting for Couchbase to be reachable..."
for i in $(seq 1 30); do
    curl -sf "http://${CB_HOST}:8091/pools" > /dev/null 2>&1 && break
    echo "    attempt $i/30..."
    sleep 2
done
curl -sf "http://${CB_HOST}:8091/pools" > /dev/null || { echo "FATAL: Couchbase not reachable"; exit 1; }

echo "==> Initializing cluster..."
curl -sf -X POST "http://${CB_HOST}:8091/clusterInit" \
    -d "hostname=127.0.0.1&username=${CB_USER}&password=${CB_PASS}&port=SAME&services=kv,n1ql,index&memoryQuota=256&indexMemoryQuota=256" \
    > /dev/null 2>&1 || true   # Ignore error if already initialized
sleep 3

echo "==> Creating bucket '${CB_BUCKET}'..."
curl -sf -X POST "http://${CB_HOST}:8091/pools/default/buckets" \
    -u "${CB_USER}:${CB_PASS}" \
    -d "name=${CB_BUCKET}&ramQuota=128&bucketType=couchbase" \
    > /dev/null 2>&1 || true   # Ignore error if bucket exists
sleep 3

echo "==> Setting index storage mode..."
curl -sf -X POST "http://${CB_HOST}:8091/settings/indexes" \
    -u "${CB_USER}:${CB_PASS}" \
    -d "storageMode=plasma" \
    > /dev/null 2>&1 || true
sleep 2

echo "==> Creating primary index..."
curl -sf -X POST "http://${CB_HOST}:8093/query/service" \
    -u "${CB_USER}:${CB_PASS}" \
    -d "statement=CREATE PRIMARY INDEX IF NOT EXISTS ON \`${CB_BUCKET}\`._default._default" \
    > /dev/null 2>&1 || true
sleep 2

echo "==> Waiting for N1QL service..."
for i in $(seq 1 20); do
    result=$(curl -sf -X POST "http://${CB_HOST}:8093/query/service" \
        -u "${CB_USER}:${CB_PASS}" \
        -d "statement=SELECT 1" 2>&1) || true
    if echo "$result" | grep -q '"status": "success"'; then
        echo "    N1QL is ready."
        break
    fi
    echo "    attempt $i/20..."
    sleep 3
done

echo "==> Couchbase test instance is ready."
echo "    Bucket: ${CB_BUCKET}"
echo "    Run tests: CB_BUCKET=${CB_BUCKET} pytest tests/ -v"
