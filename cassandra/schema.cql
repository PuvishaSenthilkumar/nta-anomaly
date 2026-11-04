CREATE KEYSPACE IF NOT EXISTS nta
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Every scored flow. Partitioned by day, newest first  ->  "latest flows today".
CREATE TABLE IF NOT EXISTS nta.flow_predictions (
  day           date,
  ts            timestamp,
  flow_id       int,
  proto         text,
  service       text,
  state         text,
  dur           double,
  sbytes        bigint,
  dbytes        bigint,
  attack_prob   double,     -- RandomForest P(attack)
  anomaly_dist  double,     -- KMeans distance to nearest "normal" centroid
  is_outlier    boolean,    -- anomaly_dist > 99th pct of normal training traffic
  pred_label    int,        -- 1 = classifier says attack
  severity      text,       -- none | medium | high
  label         int,        -- ground truth (only present because we replay a labelled dataset)
  attack_cat    text,       -- ground truth category
  PRIMARY KEY ((day), ts, flow_id)
) WITH CLUSTERING ORDER BY (ts DESC, flow_id ASC);

-- Alerts only, denormalised for "alerts for category X on day D".
CREATE TABLE IF NOT EXISTS nta.alerts_by_category (
  attack_cat    text,
  day           date,
  ts            timestamp,
  flow_id       int,
  proto         text,
  service       text,
  severity      text,
  attack_prob   double,
  anomaly_dist  double,
  PRIMARY KEY ((attack_cat, day), ts, flow_id)
) WITH CLUSTERING ORDER BY (ts DESC, flow_id ASC);

-- Output of the Hadoop MapReduce batch job (spark/load_mr_to_cassandra.py).
CREATE TABLE IF NOT EXISTS nta.mr_summary (
  attack_cat    text,
  proto         text,
  flows         bigint,
  attack_flows  bigint,
  sbytes        bigint,
  dbytes        bigint,
  dur_sum       double,
  PRIMARY KEY ((attack_cat), proto)
);
