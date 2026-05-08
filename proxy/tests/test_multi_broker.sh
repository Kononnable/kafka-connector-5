#!/usr/bin/env bash
# Test script for Kafka proxy.
#
# Starts the proxy, runs metadata/produce/consume tests,
# then checks the proxy log for deserialization errors,
# undecoded messages (except ApiVersions), and port rewrites.
#
# Usage:
#   ./test_multi_broker.sh                          # 3 proxies (3-broker cluster)
#   ./test_multi_broker.sh single                   # 1 proxy (single-broker cluster)
#   ./test_multi_broker.sh --debug                  # show proxy output
#   ./test_multi_broker.sh single --debug
#
# Exit code: 0 = all checks passed, 1 = something failed

set -uo pipefail

PROXY_BIN="$(dirname "$0")/../../target/debug/proxy"
KAFKA_TOPIC="test-cluster"
PASS=0
FAIL=0
DEBUG=false

# Parse args: extract --debug flag from any position
ARGS=()
DEBUG=false
for arg in "$@"; do
    if [ "$arg" = "--debug" ]; then
        DEBUG=true
    else
        ARGS+=("$arg")
    fi
done

if [ ! -f "$PROXY_BIN" ]; then
    echo "Building proxy..."
    (cd "$(dirname "$0")/../.." && cargo build -p proxy)
fi

cleanup() {
    fuser -k 9192/tcp 9194/tcp 9196/tcp 2>/dev/null || true
    sleep 1
}
trap cleanup EXIT
cleanup

MODE="${ARGS[0]:-}"

if $DEBUG; then
    export PROXY_DECODE_MAX=0
fi

# Broker ports (cluster running on 19092/29092/39092 by default).
# e.g. BROKER_PORTS="19092" to use a single broker.
BROKER_PORTS="${BROKER_PORTS:-19092 29092 39092}"

# Proxy listening ports (one per broker).
PROXY_PORTS=(9192 9194 9196)

# Build port-map string and proxy args.
BA=($BROKER_PORTS)
PORT_MAP=""
for i in "${!BA[@]}"; do
    [ $i -ge 3 ] && break
    [ -n "$PORT_MAP" ] && PORT_MAP+=","
    PORT_MAP+="${BA[$i]}:${PROXY_PORTS[$i]}"
done

if [ "$MODE" = "single" ]; then
    # Single-proxy mode: use only the first broker.
    LOGDIR=$(mktemp -d /tmp/proxy_test_XXXX)
    RUST_LOG=info "$PROXY_BIN" 127.0.0.1:${PROXY_PORTS[0]} 127.0.0.1:${BA[0]} "$(echo "$PORT_MAP" | cut -d, -f1)" > "$LOGDIR/proxy.log" 2>&1 &
    LOG="$LOGDIR/proxy.log"
else
    # Multi-proxy mode: start one proxy per broker.
    LOGDIR=$(mktemp -d /tmp/proxy_test_XXXX)
    for i in "${!BA[@]}"; do
        [ $i -ge 3 ] && break
        RUST_LOG=info "$PROXY_BIN" 127.0.0.1:${PROXY_PORTS[$i]} 127.0.0.1:${BA[$i]} "$PORT_MAP" > "$LOGDIR/proxy$((i+1)).log" 2>&1 &
    done
    LOG="$LOGDIR/proxy1.log"
fi

echo "=== Proxy log: $LOG ==="

sleep 3

check() {
    local name="$1"
    local cmd="$2"
    echo -n "  $name ... "
    if eval "$cmd" > /dev/null 2>&1; then
        echo "ok"
        PASS=$((PASS + 1))
    else
        echo "FAILED"
        FAIL=$((FAIL + 1))
    fi
}

check "metadata list" \
    "kcat -b 127.0.0.1:9192 -L 2>&1 | grep -q 'broker 1 at'"

MSG="test-msg-$(date +%s)"
check "produce message" \
    "echo '$MSG' | kcat -b 127.0.0.1:9192 -t '$KAFKA_TOPIC' -P 2>&1"

check "consume message" \
    "kcat -b 127.0.0.1:9192 -t '$KAFKA_TOPIC' -C -o -1 -e 2>&1 | grep -qF '$MSG'"

check "list topic" \
    "kcat -b 127.0.0.1:9192 -t '$KAFKA_TOPIC' -L 2>&1 | grep -q 'broker 1 at'"

sleep 2

echo ""
echo "=== Proxy log checks ==="

DESER_ERR=$(grep -c 'deser err' < "$LOG" 2>/dev/null; true)
UNDECODED=$(grep -c '→ REQ body.*undecoded' < "$LOG" 2>/dev/null; true)
NON_API=$(grep '→ REQ body.*undecoded' < "$LOG" 2>/dev/null | grep -v '20 bytes' | wc -l; true)
REWRITES=$(grep -c 'rewriting broker' < "$LOG" 2>/dev/null; true)
PANICS=$(grep -c 'panicked' < "$LOG" 2>/dev/null; true)

echo "  deserialization errors:  $DESER_ERR (expect 0)"
echo "  undecoded (total):       $UNDECODED"
echo "  undecoded (non-Api):     $NON_API (expect 0)"
echo "  port rewrites:           $REWRITES (expect >0)"
echo "  panics:                  $PANICS (expect 0)"

# Show undecoded entries (marked as "missed")
if [ "$UNDECODED" -gt 0 ]; then
    echo ""
    echo "  --- Missed (undecoded) ---"
    # Show the REQ header line for undecoded requests
    grep '→ REQ body.*undecoded' < "$LOG" 2>/dev/null | while IFS= read -r line; do
        if echo "$line" | grep -q '→ REQ  corr'; then
            CORR=$(echo "$line" | sed 's/.*corr=//' | sed 's/ .*//')
            API=$(echo "$line" | sed 's/.*api=//' | sed 's/ .*//')
            echo "  - missed: corr=$CORR $API"
        fi
    done | head -10
fi

if [ "$DESER_ERR" -ne 0 ]; then
    echo "  FAIL: deserialization errors found"
    FAIL=$((FAIL + 1))
fi
if [ "$NON_API" -ne 0 ]; then
    echo "  FAIL: non-ApiVersions undecoded messages"
    FAIL=$((FAIL + 1))
fi
if [ "$REWRITES" -eq 0 ]; then
    echo "  FAIL: no port rewrites"
    FAIL=$((FAIL + 1))
fi
if [ "$PANICS" -ne 0 ]; then
    echo "  FAIL: panics in proxy log"
    FAIL=$((FAIL + 1))
fi

if $DEBUG; then
    echo ""
    echo "=== Proxy request/response log ==="
    grep -E '→ REQ|← RES' < "$LOG" 2>/dev/null | head -40
else
    echo "  (use --debug to show proxy log)"
fi

echo ""
if [ "$FAIL" -eq 0 ]; then
    echo "=== ALL $PASS CHECKS PASSED ==="
else
    echo "=== $FAIL CHECKS FAILED ($PASS passed) ==="
    exit 1
fi
