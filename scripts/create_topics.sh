#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
for t in netflows scored-flows; do
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
done
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
