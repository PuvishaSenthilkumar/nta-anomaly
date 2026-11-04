"""Spark Structured Streaming: Kafka(netflows) -> score -> Cassandra + Kafka(scored-flows).

For each micro-batch:
  * RandomForest/GBT pipeline  -> attack_prob, pred_label
  * KMeans (normal-only)       -> anomaly_dist, is_outlier
  * severity: high   = classifier AND outlier agree
              medium = exactly one of them fires
              none   = neither
  * Cassandra: nta.flow_predictions (all flows) + nta.alerts_by_category (alerts only)
  * Kafka    : topic scored-flows (all flows) -> Logstash -> Elasticsearch -> Kibana / Grafana

    spark-submit stream_score.py [--starting-offsets latest] [--rf-threshold 0.5]
"""
import argparse

from pyspark.ml import PipelineModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from common import FLOW_SCHEMA, HDFS, KAFKA, MODELS_DIR, distance_udf, get_spark

CASS = "org.apache.spark.sql.cassandra"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-topic", default="netflows")
    ap.add_argument("--out-topic", default="scored-flows")
    ap.add_argument("--rf-threshold", type=float, default=0.5)
    ap.add_argument("--max-offsets", type=int, default=5000, help="max events per micro-batch")
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--trigger", default="5 seconds")
    ap.add_argument("--checkpoint", default=f"{HDFS}/nta/checkpoints/score")
    args = ap.parse_args()

    spark = get_spark("nta-stream-score")
    clf = PipelineModel.load(f"{MODELS_DIR}/classifier")
    km = KMeansModel.load(f"{MODELS_DIR}/kmeans")
    thr = spark.read.parquet(f"{MODELS_DIR}/kmeans_threshold").first()["threshold"]
    dist = distance_udf([c.tolist() for c in km.clusterCenters()])
    print(f"[stream] models loaded; outlier threshold = {thr:.4f}")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", args.in_topic)
           .option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_offsets)
           .option("failOnDataLoss", "false")
           .load())

    flows = (raw.select(F.from_json(F.col("value").cast("string"), FLOW_SCHEMA).alias("f"))
             .select("f.*")
             .filter(F.col("id").isNotNull())
             .withColumn("event_ts", F.current_timestamp()))

    def process(batch, batch_id):
        if batch.isEmpty():
            return
        for c in ("proto", "service", "state"):   # same normalisation as etl.py
            batch = batch.withColumn(c, F.lower(F.trim(F.coalesce(F.col(c), F.lit("-")))))
        batch = batch.fillna(0.0)

        s = km.transform(clf.transform(batch))
        s = (s.withColumn("attack_prob", vector_to_array("probability")[1])
              .withColumn("anomaly_dist", dist("cluster", "scaled"))
              .withColumn("is_outlier", F.col("anomaly_dist") > F.lit(float(thr)))
              .withColumn("pred_label", (F.col("attack_prob") >= args.rf_threshold).cast("int"))
              .withColumn("severity",
                          F.when((F.col("pred_label") == 1) & F.col("is_outlier"), "high")
                           .when((F.col("pred_label") == 1) | F.col("is_outlier"), "medium")
                           .otherwise("none")))

        out = s.select(
            F.to_date("event_ts").alias("day"), F.col("event_ts").alias("ts"),
            F.col("id").alias("flow_id"), "proto", "service", "state", "dur",
            F.col("sbytes").cast("long").alias("sbytes"), F.col("dbytes").cast("long").alias("dbytes"),
            "attack_prob", "anomaly_dist", "is_outlier", "pred_label", "severity",
            "label", "attack_cat",
        ).persist()

        # --- Cassandra (serving store) ---
        out.write.format(CASS).mode("append").options(table="flow_predictions", keyspace="nta").save()
        (out.filter("severity != 'none'")
            .select("attack_cat", "day", "ts", "flow_id", "proto", "service", "severity", "attack_prob", "anomaly_dist")
            .write.format(CASS).mode("append").options(table="alerts_by_category", keyspace="nta").save())

        # --- Kafka -> Logstash -> Elasticsearch ---
        (out.select(F.col("flow_id").cast("string").alias("key"),
                    F.to_json(F.struct(
                        F.col("ts").alias("event_ts"), "flow_id", "proto", "service", "state", "dur",
                        "sbytes", "dbytes", "attack_prob", "anomaly_dist", "is_outlier",
                        "pred_label", "severity", "label", "attack_cat")).alias("value"))
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("topic", args.out_topic)
            .save())

        stats = out.groupBy("severity").count().collect()
        print(f"[stream] batch {batch_id}: " + ", ".join(f"{r['severity']}={r['count']}" for r in stats))
        out.unpersist()

    (flows.writeStream
          .foreachBatch(process)
          .option("checkpointLocation", args.checkpoint)
          .trigger(processingTime=args.trigger)
          .start()
          .awaitTermination())


if __name__ == "__main__":
    main()
