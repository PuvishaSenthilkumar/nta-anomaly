# Network Traffic Analytics & Anomaly Detection (UNSW-NB15)

Lambda-style big-data pipeline: **batch** profiling + model training on Hadoop/Spark, and **real-time** scoring of
replayed network flows through Kafka, with results served from Cassandra, Elasticsearch/Kibana and Grafana.

```
                         BATCH (HDFS + YARN + Spark)
  UNSW-NB15 CSV ──► HDFS /nta/raw ──► Hadoop Streaming MapReduce (Python) ─► /nta/output/mr_summary ─► Cassandra mr_summary
                     └─► PySpark ETL ─► Parquet ─► PySpark ML training ─► HDFS /nta/models
                                                                    (RandomForest | GBT  +  KMeans on normal traffic)

STREAMING
  producer.py ─► Kafka [netflows] ─► Spark Structured Streaming (loads models, scores every micro-batch)
  (replay @ N ev/s)                              │                         │
                                              ▼                         ▼
                                             Cassandra (flow_predictions,    Kafka [scored-flows] ─► Logstash ─► Elasticsearch ─► Kibana
                                             alerts_by_category)                                                         └────────► Grafana
```

## Technology-to-Component Mapping

| Requested tech | Where it is used |
|---|---|
| MapReduce | `mapreduce/` - Hadoop Streaming job (mapper, reducer, combiner) → traffic profile per `attack_cat × proto` |
| HDFS / Hadoop | `hadoop/` + compose: NameNode, DataNode, YARN. Holds raw CSV, Parquet, models, MR output, streaming checkpoints |
| NoSQL / Cassandra | `cassandra/schema.cql` - query-first tables: flows by day, alerts by category, MR summary |
| PySpark / Python | `spark/` ETL, training, streaming scoring, loader; `kafka/producer.py` |
| Kafka | KRaft broker; topics `netflows` (input) and `scored-flows` (output) |
| ML | RF vs GBT classifier (best AUC kept) + unsupervised KMeans distance-to-normal detector |
| ELK | Logstash (Kafka→ES), Elasticsearch, Kibana data view |
| Grafana | Provisioned dashboard `Network Traffic Analytics` (Elasticsearch datasource; Cassandra plugin datasource also provisioned) |

## Detection Logic

* **Classifier** - `StringIndexer → OneHotEncoder → VectorAssembler → RandomForest/GBT` on the 39 numeric + 3 categorical
  (`proto, service, state`) flow features. `id`, `label`, `attack_cat` are never used as features.
* **Anomaly score** - KMeans (k=8) fitted on *normal* training flows only; score = Euclidean distance to the assigned centroid;
  outlier if above the 99th percentile of normal-training distances. This can flag attack types the classifier never learned.
* **Severity** - `high` = classifier and outlier agree, `medium` = one fires, `none` = neither.

## Run It

Prereqs: Docker + Compose, ~10-12 GB RAM for Docker, Python 3 on the host (producer only).

```bash
# 0. data - see data/README.md  (or: python scripts/make_sample.py --rows 3000 --out data  for a synthetic smoke test)

# 1. bring up the stack (first build downloads images; takes a while)
docker compose up -d --build
scripts/create_topics.sh

# 2. batch layer
scripts/load_hdfs.sh                         # CSVs -> HDFS /nta/raw
scripts/run_mr.sh                            # MapReduce on YARN (falls back to local pipes if python3 is missing on YARN)
scripts/submit.sh load_mr_to_cassandra.py    # MR output -> Cassandra
scripts/submit.sh etl.py                     # CSV -> Parquet
scripts/submit.sh train.py                   # trains + prints AUC/F1, confusion matrix, top features; saves models to HDFS

# 3. streaming layer (two terminals)
scripts/submit.sh stream_score.py            # terminal A - long-running
pip install -r requirements.txt
python kafka/producer.py --eps 200 --shuffle --loop   # terminal B - replays the *testing* set

# 4. look at it
scripts/kibana_setup.sh                      # then http://localhost:5601/app/discover
#   Grafana        http://localhost:3000   (admin / admin)  -> "Network Traffic Analytics"
#   HDFS UI        http://localhost:9870    YARN UI http://localhost:8088    Spark UI http://localhost:4040
docker compose exec cassandra cqlsh -e "SELECT severity, attack_cat, attack_prob FROM nta.flow_predictions LIMIT 10;"
docker compose exec cassandra cqlsh -e "SELECT * FROM nta.mr_summary LIMIT 10;"
```

## Layout

```
docker-compose.yml        hadoop/  spark-image/      images for Hadoop(+python3) and Spark(+numpy)
mapreduce/mapper.py       mapreduce/reducer.py        Hadoop Streaming job
spark/common.py           schema, feature lists, paths, distance UDF
spark/etl.py              spark/train.py              spark/stream_score.py     spark/load_mr_to_cassandra.py
kafka/producer.py         CSV replay → Kafka
cassandra/schema.cql      logstash/pipeline/logstash.conf
grafana/provisioning/     grafana/dashboards/nta.json
scripts/                  submit / load / run / topics / kibana helpers
```

## What Was and Wasn't Verified

Checked in the build environment: all Python compiles, shell scripts parse, compose/provisioning YAML and the dashboard JSON are valid,
and the MapReduce mapper → sort → reducer pipeline matches a pandas group-by exactly on synthetic data.

**Not run** (no Docker, no PySpark package access in the build sandbox): the containers, the Spark jobs, Kafka/Cassandra/ELK wiring and the
Grafana panels. Expect to fix small things on first run - most likely spots:

* `hadoop/Dockerfile` - the base image's package repo; if `python3` doesn't install, `run_mr.sh` auto-falls back to local pipes.
* Grafana Cassandra datasource field names (the plugin's provisioning keys vary by version); the dashboard itself only uses Elasticsearch.
* Grafana panel queries expect `.keyword` sub-fields from Elasticsearch's default dynamic mapping.
* Spark ↔ HDFS: the `spark` container reaches HDFS by container hostnames `namenode`/`datanode` on the compose network.

## Caveats

* The streaming demo replays the **labelled testing set**, so `label`/`attack_cat` ride along in the messages purely to let dashboards compare
  predictions with ground truth. A real deployment would not have them; the models never read them.
* UNSW-NB15 features are flow statistics generated by Argus/Bro in a lab testbed; scores here don't transfer directly to production traffic.
* Single-node, replication factor 1, security off (ES, Kafka, Cassandra) - a learning/demo stack, not hardened. HDFS data is lost on `docker compose down` (no volume); Cassandra/ES/Grafana persist in named volumes.
* Spark runs `local[*]` inside one container. To use the YARN cluster instead you'd add Spark + `HADOOP_CONF_DIR` to the Hadoop image and submit with `--master yarn`.

## Verified vs Not Verified

| Component | Status |
|---|---|
| Synthetic data generation | ✅ Verified |
| MapReduce mapper → sort → reducer pipeline (local) | ✅ Verified (cross-checks pandas group-by) |
| Python compilation & shell parsing | ✅ Verified |
| ML model training (RF + KMeans) on synthetic data | ✅ Verified (97% RF accuracy, KMeans threshold computed) |
| Local severity logic (high/medium/none) | ✅ Verified |
| File parsing (YAML/JSON/Shell) | ✅ Verified |
| Docker Compose stack bring-up | ❌ Not verified (no Docker in build sandbox) |
| Kafka broker + topic management | ❌ Not verified |
| Cassandra database operations | ❌ Not verified |
| Elasticsearch/Kibana/Grafana wiring | ❌ Not verified |
| Spark Structured Streaming with model loading | ❌ Not verified |
| End-to-end pipeline on real UNSW-NB15 data | ❌ Not verified |

## Next Steps (if Docker becomes available)

1. `docker compose up -d --build` - bring up the full stack
2. `scripts/create_topics.sh` - create Kafka topics `netflows` and `scored-flows`
3. Batch layer: load HDFS, run MR, ETL, train, load to Cassandra
4. Streaming layer: start scoring, replay test data
5. Visualization: Kibana discover, Grafana dashboard panels

---

**Download the real UNSW-NB15 dataset** from <https://research.unsw.edu.au/projects/unsw-nb15-dataset>
(OneDrive folder; links need a browser; cannot be scripted reliably).

From the folder **"Training and Testing Sets"** copy these two files here:
- `data/UNSW_NB15_training-set.csv`   (175,341 rows, 45 columns, header row)
- `data/UNSW_NB15_testing-set.csv`    ( 82,332 rows, 45 columns, header row)

Replace the synthetic files in `data/` with the real CSVs. The schema is identical; only the feature distributions differ.

**Synthetic data smoke-test:**
```bash
python scripts/make_sample.py --rows 3000 --out data   # writes data/UNSW_NB15_{training,testing}-set.csv
```