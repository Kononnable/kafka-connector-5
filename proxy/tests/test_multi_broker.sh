#!/usr/bin/env bash
# Test script for Kafka proxy with multi-broker cluster.
#
# Prerequisites:
#   3-node KRaft cluster running on ports 9092, 9094, 9096
#   (start via: podman, see test-cluster-setup.md)
#
# Usage:
#   ./test_multi_broker.sh            # test with 3 proxies (one per broker)
#   ./test_multi_broker.sh single     # test with 1 proxy (all ports mapped to same)

set -euo pipefail

PROXY_BIN="$(dirname "$0")/../../target/debug/proxy"
KAFKA_TOPIC="test-cluster"

if [ ! -f "$PROXY_BIN" ]; then
    echo "Building proxy..."
    (cd "$(dirname "$0")/../.." && cargo build -p proxy)
fi

cleanup() {
    echo "=== Cleaning up proxies ==="
    fuser -k 9192/tcp 9194/tcp 9196/tcp 2>/dev/null || true
    sleep 1
}
trap cleanup EXIT
cleanup

if [ "${1:-}" = "single" ]; then
    echo "=== Single proxy mode ==="
    PORT_MAP="9092:9192,9094:9192,9096:9192"
    RUST_LOG=info "$PROXY_BIN" 127.0.0.1:9192 127.0.0.1:9092 "$PORT_MAP" &
    PROXY_PID=$!
else
    echo "=== Three proxy mode ==="
    PORT_MAP="9092:9192,9094:9194,9096:9196"
    RUST_LOG=info "$PROXY_BIN" 127.0.0.1:9192 127.0.0.1:9092 "$PORT_MAP" &
    RUST_LOG=info "$PROXY_BIN" 127.0.0.1:9194 127.0.0.1:9094 "$PORT_MAP" &
    RUST_LOG=info "$PROXY_BIN" 127.0.0.1:9196 127.0.0.1:9096 "$PORT_MAP" &
fi

sleep 3

echo ""
echo "=== 1. METADATA ==="
kcat -b 127.0.0.1:9192 -L 2>&1 | grep -v "^%"

echo ""
echo "=== 2. PRODUCE ==="
echo "test-msg-$(date +%s)" | kcat -b 127.0.0.1:9192 -t "$KAFKA_TOPIC" -P 2>&1

echo ""
echo "=== 3. CONSUME ==="
kcat -b 127.0.0.1:9192 -t "$KAFKA_TOPIC" -C -o -1 -e 2>&1

echo ""
echo "=== 4. LIST TOPIC (via proxy) ==="
kcat -b 127.0.0.1:9192 -t "$KAFKA_TOPIC" -L 2>&1 | grep -v "^%"

if [ "${1:-}" != "single" ]; then
    echo ""
    echo "=== 5. LIST via different proxy ==="
    kcat -b 127.0.0.1:9194 -t "$KAFKA_TOPIC" -L 2>&1 | grep -v "^%"
fi

echo ""
echo "=== ALL TESTS PASSED ==="
