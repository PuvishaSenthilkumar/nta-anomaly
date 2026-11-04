# Network Traffic Analytics - Pipeline Execution Summary

## Execution Context
- **Date**: November 4, 2026
- **Environment**: Windows, Python 3.14.0, no Docker available
- **Data**: Synthetic UNSW-NB15 dataset generated (3000 training, 1500 testing rows)

## What Was Run

### 1. Synthetic Data Generation
Command: `python scripts/make_sample.py --rows 3000 --out data`
- Generated `data/UNSW_NB15_training-set.csv` (3000 rows)
- Generated `data/UNSW_NB15_testing-set.csv` (1500 rows)
- Column layout matches the real UNSW-NB15 dataset

### 2. Hadoop Streaming MapReduce (Local Execution)
Ran the full MapReduce pipeline locally without Docker:

**Mapper** (`mapreduce/mapper.py` with `attack_cat proto` key):
- Processed 3000 input rows
- Emitted key=attack_cat|proto, value=1\tlabel\tsbytes\tdbytes\tdur

**Sort**: LC_ALL=C sort by key field

**Reducer** (`mapreduce/reducer.py`):
- Aggregated by key (attack_cat|proto)
- Output: key\t total_flows\t attack_flows\t total_sbytes\t total_dbytes\t dur_sum

**Results**: 23 unique attack_cat|proto combinations profiled
- Normal traffic profiles dominate (Normal|tcp: 1030 flows, Normal|udp: 323 flows)
- All 9 attack categories represented: Analysis, Backdoor, DoS, Exploits, Fuzzers, Generic, Reconnaissance, Shellcode, Worms
- Each key shows flow count, attack flow count, byte totals, and duration sum

### 3. Local Machine Learning Demo
Random Forest Classifier + KMeans Anomaly Detection on synthetic data:

**Random Forest Classifier**:
- Accuracy: 97% on 600 test samples
- Precision/Recall/F1: ~0.97-0.98 for both Normal and Attack classes
- Top features: sinpkt (0.179), dur (0.153), ct_srv_src (0.132), rate (0.108), sload (0.092)

**KMeans Clustering (k=8)**:
- Fitted on normal training traffic only
- 99th percentile anomaly threshold computed
- Distance-to-centroid used as anomaly score

**Severity Classification** (classifier + outlier agreement):
- high: classifier AND outlier agree (0 samples - none in this test)
- medium: one detector fires, other doesn't (828 samples - 55.2%)
- none: neither detector fires (672 samples - 44.8%)

**Metrics saved** to `local_metrics.json`:
- RF F1 score, KMeans train/test thresholds

## Pipeline Architecture (as designed)

```
BATCH (HDFS + YARN + Spark):
  UNSW-NB15 CSV → Hadoop Streaming MapReduce → Cassandra mr_summary
  → PySpark ETL → Parquet → PySpark ML training → HDFS models
  
STREAMING:
  producer.py → Kafka [netflows] → Spark Structured Streaming
  → Cassandra (flow_predictions, alerts_by_category)
  → Kafka [scored-flows] → Logstash → Elasticsearch → Kibana/Grafana
```

## What Works Without Docker
✅ Synthetic data generation
✅ Hadoop Streaming MapReduce (mapper → sort → reducer pipeline)
✅ Random Forest classification with sklearn
✅ KMeans anomaly detection with sklearn
✅ Severity classification logic
✅ Feature importance extraction

## What Requires Docker/Cluster
❌ Full Hadoop/Spark/YARN cluster (containers)
❌ Kafka broker + topic management
❌ Cassandra database
❌ Elasticsearch/Kibana/Grafana stack
❌ Structured Streaming with model loading

## Next Steps (if Docker becomes available)
1. `docker compose up -d --build` - bring up the full stack
2. `scripts/create_topics.sh` - create Kafka topics
3. Batch layer: load HDFS, run MR, ETL, train, load to Cassandra
4. Streaming layer: start scoring, replay test data
5. Visualization: Kibana discover, Grafana dashboard

## Files Created
- `data/UNSW_NB15_training-set.csv` (1013886 bytes, 3000 rows)
- `data/UNSW_NB15_testing-set.csv` (506639 bytes, 1500 rows)
- `mapper_out_utf8.txt` (mapper output, 3000 rows)
- `local_metrics.json` (RF metrics + KMeans thresholds)
- `local_demo.py` (reproducible ML demo script)
- `run_mapper.py` / `map_generator.py` (mapper utility scripts)