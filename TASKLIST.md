# Network Traffic Analytics & Anomaly Detection - Task List

## Priority Order (build in this order)

### 1. Synthetic Data Generation
- [ ] Run `scripts/make_sample.py` to generate UNSW-NB15 CSVs with same schema
- [ ] Verify column layout matches real dataset (45 cols, header row)

### 2. MapReduce (Hadoop Streaming)
- [ ] Create `mapreduce/mapper.py` - key=attack_cat|proto, emit flows/attack_flows/sbytes/dbytes/dur_sum
- [ ] Create `mapreduce/reducer.py` - aggregate by key, output same format (usable as combiner)
- [ ] Cross-check output against pandas group-by

### 3. Spark Module (common + ETL + Training)
- [ ] Create `spark/common.py` - ALL_COLS, FLOW_SCHEMA, CATEGORICAL, NUMERIC, get_spark(), distance_udf()
- [ ] Create `spark/etl.py` - CSV -> Parquet with StringIndexer/OneHotEncoder/VectorAssembler/StandardScaler
- [ ] Create `spark/train.py` - RF (100 trees, depth 12) vs GBT (40 iter), evaluate, save best model
- [ ] Create `spark/stream_score.py` - Structured Streaming: load models, score micro-batches, write to Cassandra, publish to Kafka

### 4. Kafka & Streaming
- [ ] Create `kafka/producer.py` - replay testing set into topic "netflows", --eps --shuffle --loop
- [ ] Create `logstash/pipeline/logstash.conf` - Kafka input (scored-flows, json), date filter, ES output

### 5. Cassandra (query-first schema)
- [ ] Create `cassandra/schema.cql` - keyspace nta, tables: flow_predictions, alerts_by_category, mr_summary

### 6. Docker Compose (sketch - not runnable without Docker)
- [ ] Create `docker-compose.yml` with all services
- [ ] Create `cassandra/init.sh` schema applier

### 7. ELK Stack
- [ ] Configure Logstash-shipped format for ES
- [ ] Create Kibana data view script

### 8. Grafana
- [ ] Create `grafana/provisioning/dashboards/nta.json` - full dashboard with stat panels, time series, barchart, table
- [ ] Create `grafana/provisioning/datasources/` - ES and Cassandra datasources

### 9. README.md
- [ ] Architecture diagram
- [ ] Technology-to-component table
- [ ] Step-by-step run commands
- [ ] UI URLs
- [ ] Known limitations / verified vs not verified

### 10. Verification
- [ ] Run MapReduce locally, cross-check with pandas
- [ ] Run local ML demo (RF + KMeans)
- [ ] Note what requires Docker

## Verification Checkpoints
- [x] MapReduce output matches pandas group-by exactly (29/29 keys match)
- [x] PySpark ETL compiles and runs (local[*])
- [x] Train.py produces AUC/F1/confusion matrix, saves models
- [x] local_demo.py runs RF + KMeans on synthetic data
- [x] All Python files compile (py_compile)
- [x] All Shell files parse (bash -n)
- [x] All YAML/JSON files parse validly
- [x] Model integration from Colab: .pkl placed at backend/models/UNSW_NB15_Model.pkl
- [x] Flask prediction API: /api/predict and /api/health endpoints verified
- [x] Batch CSV prediction: predict_batch() function verified
- [x] Model loading via joblib verified
- [x] Feature name extraction from Pipeline verified