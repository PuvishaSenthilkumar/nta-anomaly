CREATE KEYSPACE nta WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE nta;

CREATE TABLE flow_predictions (
    day date,
    ts timestamp,
    flow_id bigint,
    prediction double,
    anomaly_dist double,
    severity text,
    attack_cat text,
    label int,
    PRIMARY KEY ((day), ts, flow_id)
) WITH clustering order BY (ts DESC);

CREATE TABLE alerts_by_category (
    attack_cat text,
    day date,
    ts timestamp,
    flow_id bigint,
    prediction double,
    anomaly_dist double,
    severity text,
    label int,
    PRIMARY KEY ((attack_cat, day), ts, flow_id)
) WITH clustering order BY (ts DESC);

CREATE TABLE mr_summary (
    attack_cat text,
    proto text,
    flows bigint,
    attack_flows bigint,
    sbytes bigint,
    dbytes bigint,
    dur_sum double,
    PRIMARY KEY ((attack_cat), proto)
);