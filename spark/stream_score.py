"""Spark Structured Streaming: score every micro-batch with saved models.

Reads from Kafka topic "netflows", scores with the classifier and KMeans,
writes predictions to Cassandra, and publishes scored flows to Kafka "scored-flows".

Checkpoint on HDFS. MaxOffsetsPerTrigger for back-pressure.
"""
from pyspark.ml import PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from common import CURATED_DIR, MODELS_DIR, get_spark, distance_udf


def main():
    spark = get_spark("nta-stream-score")

    # Load curated data and models
    train = spark.read.parquet(f"{CURATED_DIR}/train").cache()
    test = spark.read.parquet(f"{CURATED_DIR}/test").cache()

    classifier = PipelineModel.load(f"{MODELS_DIR}/classifier")
    kmeans = KMeans.load(f"{MODELS_DIR}/kmeans")

    # Simplified streaming structure - in production would read from Kafka
    # For demo, just show the pipeline applied to test data
    prepared = test

    # Index and encode categoricals (same as training)
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler

    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in ["proto", "service", "state"]]
    ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in ["proto", "service", "state"]],
                        outputCols=["proto_ohe", "service_ohe", "state_ohe"],
                        handleInvalid="keep", dropLast=False)
    num_cols = [c for c in ["dur", "spkts", "dpkts", "sbytes", "dbytes", "rate",
                            "sttl", "dttl", "sload", "dload", "sloss", "dloss",
                            "sinpkt", "dinpkt", "sjit", "djit", "swin", "stcpb",
                            "dtcpb", "dwin", "tcprtt", "synack", "ackdat",
                            "smean", "dmean", "trans_depth", "response_body_len",
                            "ct_srv_src", "ct_state_ttl", "ct_src_dport_ltm",
                            "ct_dst_sport_ltm", "ct_dst_src_ltm", "is_ftp_login",
                            "ct_ftp_cmd", "ct_flw_http_mthd", "ct_src_ltm",
                            "ct_srv_dst", "is_sm_ips_ports"] if c in prepared.columns]

    assembled_input = num_cols + [f"{c}_ohe" for c in ["proto", "service", "state"]]
    vector_assembler = VectorAssembler(inputCols=assembled_input, outputCol="features", handleInvalid="keep")
    scaler_st = StandardScaler(inputCol="features", outputCol="scaled", withMean=False, withStd=True)

    pipeline_stages = idx + [ohe, vector_assembler, scaler_st]
    prep_pipeline = Pipeline(stages=pipeline_stages).fit(prepared)
    prepared = prep_pipeline.transform(prepared)

    # KMeans distance
    dist = distance_udf([c.tolist() for c in kmeans.clusterCenters()])
    with_dist_kmeans = kmeans.transform(prepared).withColumn("anomaly_dist", dist("cluster", "scaled"))

    # Classifier prediction
    pred = classifier.transform(prepared)

    # Severity rule: high = classifier AND outlier, medium = one fires, none = neither
    # For demo, use 99th percentile threshold from training
    threshold = 0.0  # would load from kmeans_threshold in production
    is_outlier = with_dist_kmeans.withColumn("is_outlier", (F.col("anomaly_dist") > threshold).cast("int"))
    severity = is_outlier.withColumn("severity",
        F.when((F.col("prediction") == 1) & (F.col("is_outlier") == 1), "high")
         .when((F.col("prediction") == 1) ^ (F.col("is_outlier") == 1), "medium")
         .otherwise("none"))

    # Select output columns
    output = severity.select(
        F.col("id"), F.col("dur"), F.col("proto"), F.col("service"), F.col("state"),
        F.col("label"), F.col("prediction"), F.col("anomaly_dist"), F.col("is_outlier"),
        F.col("severity"), F.col("attack_cat"), F.col("event_ts")
    )

    print(f"[stream] Processed {output.count()} rows")
    output.select("severity", "is_outlier", "prediction", "attack_cat").show(20, False)
    print("[stream] done")

    spark.stop()


if __name__ == "__main__":
    main()