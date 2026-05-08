#!/usr/bin/env bash
# Start a 3-node KRaft Kafka cluster for testing the proxy.
set -euo pipefail

CLUSTER_ID="5L6g3nShT-eMCtK--X86sw"
VOTERS="1@127.0.0.1:9093,2@127.0.0.1:9095,3@127.0.0.1:9097"

echo "=== Removing old containers ==="
for i in 1 2 3; do podman rm -f kafka-$i 2>/dev/null || true; done

echo "=== Starting 3-node KRaft cluster ==="
for i in 1 2 3; do
  PLAINTEXT_PORT=$((9090 + i*2))   # 9092, 9094, 9096
  CTRL_PORT=$((PLAINTEXT_PORT + 1)) # 9093, 9095, 9097
  NAME="kafka-${i}"

  podman run -d --name "$NAME" \
    --network host \
    --add-host pi:127.0.0.1 \
    -e CLUSTER_ID="$CLUSTER_ID" \
    -e KAFKA_NODE_ID="$i" \
    -e KAFKA_PROCESS_ROLES="broker,controller" \
    -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:${PLAINTEXT_PORT},CONTROLLER://0.0.0.0:${CTRL_PORT}" \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://127.0.0.1:${PLAINTEXT_PORT}" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER" \
    -e KAFKA_INTER_BROKER_LISTENER_NAME="PLAINTEXT" \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="$VOTERS" \
    docker.io/apache/kafka-native:latest

  echo "  Started $NAME on PLAINTEXT=$PLAINTEXT_PORT CTRL=$CTRL_PORT"
done

echo "=== Waiting for cluster to be ready ==="
sleep 8
for i in 1 2 3; do
  PORT=$((9090 + i*2))
  if nc -z 127.0.0.1 $PORT; then
    echo "  kafka-$i ready on port $PORT"
  else
    echo "  kafka-$i FAILED on port $PORT"
    podman logs kafka-$i 2>&1 | tail -5
  fi
done

echo "=== Cluster ready ==="
echo "  Bootstrap: 127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096"
