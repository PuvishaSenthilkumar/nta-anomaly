#!/usr/bin/env bash
# Copy the UNSW-NB15 CSVs from ./data into HDFS (/nta/raw).
set -euo pipefail
cd "$(dirname "$0")/.."

for f in UNSW_NB15_training-set.csv UNSW_NB15_testing-set.csv; do
  [ -f "data/$f" ] || { echo "missing data/$f  (see data/README.md)"; exit 1; }
done

docker compose exec namenode bash -c 'rm -rf /tmp/nta-data && mkdir -p /tmp/nta-data'
docker compose cp data/UNSW_NB15_training-set.csv namenode:/tmp/nta-data/
docker compose cp data/UNSW_NB15_testing-set.csv  namenode:/tmp/nta-data/
docker compose exec namenode bash -c '
  hdfs dfsadmin -safemode wait &&
  hdfs dfs -mkdir -p /nta/raw &&
  hdfs dfs -put -f /tmp/nta-data/*.csv /nta/raw/ &&
  hdfs dfs -ls -h /nta/raw'
