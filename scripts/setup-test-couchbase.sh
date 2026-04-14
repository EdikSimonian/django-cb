#!/usr/bin/env bash
# Initialize a local Couchbase container for running tests.
#
# Usage:
#   ./scripts/setup-test-couchbase.sh          # Setup only (Couchbase must be running)
#   ./scripts/setup-test-couchbase.sh --start   # Start Docker + setup
#   ./scripts/setup-test-couchbase.sh --full    # Start Docker + setup + run tests
#
# Expects Couchbase to be running on localhost (ports 8091-8097, 11210-11211).
# Start manually with: docker compose -f docker-compose.test.yml up -d

set -euo pipefail

CB_HOST="${CB_HOST:-localhost}"
CB_USER="${CB_USERNAME:-Administrator}"
CB_PASS="${CB_PASSWORD:-password}"
CB_BUCKET="${CB_BUCKET:-testbucket}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# --- Parse flags ---
START_DOCKER=false
RUN_TESTS=false
for arg in "$@"; do
    case "$arg" in
        --start) START_DOCKER=true ;;
        --full)  START_DOCKER=true; RUN_TESTS=true ;;
    esac
done

# --- Start Docker if requested ---
if $START_DOCKER; then
    echo "==> Starting Couchbase via Docker..."
    docker compose -f "${PROJECT_DIR}/docker-compose.test.yml" up -d
fi

# --- Wait for Couchbase ---
echo "==> Waiting for Couchbase to be reachable..."
for i in $(seq 1 30); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://${CB_HOST}:8091/pools" 2>/dev/null)
    [ "$HTTP_CODE" != "000" ] && break
    echo "    attempt $i/30..."
    sleep 2
done
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://${CB_HOST}:8091/pools" 2>/dev/null)
[ "$HTTP_CODE" = "000" ] && { echo "FATAL: Couchbase not reachable"; exit 1; }
echo "    Couchbase responding (HTTP $HTTP_CODE)"

# --- Check if already initialized ---
ALREADY_INIT=false
curl -sf "http://${CB_HOST}:8091/pools/default" -u "${CB_USER}:${CB_PASS}" > /dev/null 2>&1 && ALREADY_INIT=true

if ! $ALREADY_INIT; then
    echo "==> Initializing cluster..."
    curl -sf -X POST "http://${CB_HOST}:8091/clusterInit" \
        -d "hostname=127.0.0.1&username=${CB_USER}&password=${CB_PASS}&port=SAME&services=kv,n1ql,index&memoryQuota=256&indexMemoryQuota=256" \
        > /dev/null 2>&1 || true
    sleep 3
else
    echo "==> Cluster already initialized."
fi

# --- Create bucket ---
echo "==> Creating bucket '${CB_BUCKET}'..."
curl -sf -X POST "http://${CB_HOST}:8091/pools/default/buckets" \
    -u "${CB_USER}:${CB_PASS}" \
    -d "name=${CB_BUCKET}&ramQuota=256&bucketType=couchbase&flushEnabled=1" \
    > /dev/null 2>&1 || true   # Ignore error if bucket exists
sleep 3

# --- Set index storage mode ---
echo "==> Setting index storage mode..."
curl -sf -X POST "http://${CB_HOST}:8091/settings/indexes" \
    -u "${CB_USER}:${CB_PASS}" \
    -d "storageMode=plasma" \
    > /dev/null 2>&1 || true
sleep 2

# --- Wait for N1QL ---
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

# --- Create primary index ---
echo "==> Creating primary index..."
curl -sf -X POST "http://${CB_HOST}:8093/query/service" \
    -u "${CB_USER}:${CB_PASS}" \
    -d "statement=CREATE PRIMARY INDEX IF NOT EXISTS ON \`${CB_BUCKET}\`" \
    > /dev/null 2>&1 || true
sleep 2

# --- Create Document API test collections ---
echo "==> Creating test collections..."
for coll in edge_test_docs exdocs int_brewers int_beers int_ratings; do
    curl -sf -X POST "http://${CB_HOST}:8091/pools/default/buckets/${CB_BUCKET}/scopes/_default/collections" \
        -u "${CB_USER}:${CB_PASS}" -d "name=$coll" 2>/dev/null || true
done
sleep 3

# --- Create primary indexes on collections ---
echo "==> Creating collection indexes..."
for coll in edge_test_docs exdocs int_brewers int_beers int_ratings; do
    curl -sf -X POST "http://${CB_HOST}:8093/query/service" \
        -u "${CB_USER}:${CB_PASS}" \
        -d "statement=CREATE PRIMARY INDEX IF NOT EXISTS ON \`${CB_BUCKET}\`.\`_default\`.\`$coll\`" \
        > /dev/null 2>&1 || true
done
sleep 2

# --- Run Django migrations ---
echo "==> Running Django migrations..."
export DJANGO_SETTINGS_MODULE=tests.django_settings
export CB_BUCKET="${CB_BUCKET}"
cd "$PROJECT_DIR"
python3 -m django migrate contenttypes 2>&1 | tail -1
python3 -m django migrate auth 2>&1 | tail -1
python3 -m django migrate admin 2>&1 | tail -1
python3 -m django migrate sessions 2>&1 | tail -1
python3 -m django migrate testapp 2>&1 | tail -1

echo ""
echo "==> Couchbase test instance is ready."
echo "    Bucket: ${CB_BUCKET}"
echo "    User:   ${CB_USER}"

# --- Run tests if --full ---
if $RUN_TESTS; then
    echo ""
    echo "==> Running tests..."
    cd "$PROJECT_DIR"
    exec python3 -m pytest tests/ -v --tb=short \
        --ignore=tests/test_wagtail_crud.py \
        --ignore=tests/test_wagtail_settings.py \
        --ignore=tests/testapp \
        --ignore=tests/wagtailapp
fi
