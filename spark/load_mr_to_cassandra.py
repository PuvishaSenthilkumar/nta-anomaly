"""Load the MapReduce summary (TSV on HDFS) into Cassandra table nta.mr_summary.

    spark-submit load_mr_to_cassandra.py [--path hdfs://.../nta/output/mr_summary]

Assumes the MapReduce job used the default key (attack_cat proto).
"""
import argparse

from pyspark.sql import functions as F

from common import OUTPUT_DIR, get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default=f"{OUTPUT_DIR}/mr_summary")
    args = ap.parse_args()

    spark = get_spark("nta-load-mr")
    df = spark.read.csv(f"{args.path}/part-*", sep="\t", schema="key string, flows long, attack_flows long, "
                                                                "sbytes long, dbytes long, dur_sum double")
    parts = F.split("key", r"\|")
    out = df.select(parts.getItem(0).alias("attack_cat"), parts.getItem(1).alias("proto"),
                    "flows", "attack_flows", "sbytes", "dbytes", "dur_sum")
    out.write.format("org.apache.spark.sql.cassandra").mode("append") \
        .options(table="mr_summary", keyspace="nta").save()
    print(f"[load_mr] wrote {out.count()} rows to nta.mr_summary")
    out.orderBy(F.desc("flows")).show(15, False)
    spark.stop()


if __name__ == "__main__":
    main()
