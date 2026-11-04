#!/usr/bin/env bash
# Same mapper/reducer, run with plain Unix pipes (identical to Hadoop's map -> shuffle/sort -> reduce).
# Useful for debugging and as a fallback when python3 is missing on the YARN nodes.
#   scripts/run_mr_local.sh data/UNSW_NB15_training-set.csv [key cols...]
set -euo pipefail
cd "$(dirname "$0")/.."
IN="$1"; shift || true
python3 mapreduce/mapper.py "$@" < "$IN" \
  | LC_ALL=C sort -t$'\t' -k1,1 \
  | python3 mapreduce/reducer.py \
  | sort -t$'\t' -k2,2nr
