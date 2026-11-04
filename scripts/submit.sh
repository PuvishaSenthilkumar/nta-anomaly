#!/usr/bin/env bash
# Run a PySpark job inside the `spark` container.
#   scripts/submit.sh etl.py
#   scripts/submit.sh train.py --k 8
#   scripts/submit.sh stream_score.py
set -euo pipefail
cd "$(dirname "$0")/.."
JOB="$1"; shift || true

PKGS="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"

docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --driver-memory "${DRIVER_MEM:-3g}" \
  --packages "$PKGS" \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.cassandra.connection.host=cassandra \
  "/opt/nta/spark/${JOB}" "$@"
