#!/usr/bin/env bash
# Run the MapReduce (Hadoop Streaming) job on YARN: per (attack_cat, proto) traffic profile.
#   scripts/run_mr.sh                      # key = attack_cat proto
#   scripts/run_mr.sh service state        # custom key columns
set -euo pipefail
cd "$(dirname "$0")/.."
KEYS="${*:-attack_cat proto}"
JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar

if ! docker compose exec -T nodemanager python3 --version >/dev/null 2>&1; then
  echo "python3 is not available on the YARN nodemanager -> falling back to scripts/run_mr_local.sh"
  exec scripts/run_mr_local.sh data/UNSW_NB15_training-set.csv
fi

docker compose exec -T namenode bash -c 'rm -rf /tmp/mr && mkdir -p /tmp/mr'
docker compose cp mapreduce/mapper.py  namenode:/tmp/mr/mapper.py
docker compose cp mapreduce/reducer.py namenode:/tmp/mr/reducer.py

docker compose exec -T namenode bash -c "
  hdfs dfs -rm -r -f /nta/output/mr_summary &&
  hadoop jar $JAR \
    -D mapreduce.job.name=nta-traffic-profile \
    -D mapreduce.job.reduces=2 \
    -files /tmp/mr/mapper.py,/tmp/mr/reducer.py \
    -input  /nta/raw/UNSW_NB15_training-set.csv \
    -output /nta/output/mr_summary \
    -mapper   'python3 mapper.py $KEYS' \
    -combiner 'python3 reducer.py' \
    -reducer  'python3 reducer.py' &&
  hdfs dfs -cat '/nta/output/mr_summary/part-*' | sort -t\$'\t' -k2,2nr | head -25"
