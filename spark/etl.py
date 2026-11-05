"""Batch ETL: raw UNSW-NB15 CSV (HDFS) -> cleaned, typed Parquet (HDFS).

    spark-submit etl.py
"""
from pyspark.sql import functions as F

from common import ALL_COLS, CURATED_DIR, FLOW_SCHEMA, RAW_DIR, get_spark


FILES = {
    "train": f"{RAW_DIR}/UNSW_NB15_training-set.csv",
    "test": f"{RAW_DIR}/UNSW_NB15_testing-set.csv",
}


def clean(df):
    df = df.dropna(subset=["label"])
    for c in ("proto", "service", "state"):
        df = df.withColumn(c, F.lower(F.trim(F.coalesce(F.col(c), F.lit("-")))))
    df = df.withColumn("attack_cat", F.trim(F.coalesce(F.col("attack_cat"), F.lit("Normal"))))
    df = df.withColumn("attack_cat", F.when(F.col("attack_cat").isin("", "-"), "Normal").otherwise(F.col("attack_cat")))
    num = [c for c, t in df.dtypes if t == "double"]
    return df.fillna(0.0, subset=num)


def main():
    spark = get_spark("nta-etl")
    for name, path in FILES.items():
        df = spark.read.csv(path, header=True, schema=FLOW_SCHEMA, mode="DROPMALFORMED")
        df = clean(df).select(*ALL_COLS)
        out = f"{CURATED_DIR}/{name}"
        df.repartition(8).write.mode("overwrite").parquet(out)
        n = df.count()
        print(f"[etl] {name}: {n} rows -> {out}")
        df.groupBy("label").count().orderBy("label").show()
        df.groupBy("attack_cat").count().orderBy(F.desc("count")).show(20, False)
    spark.stop()


if __name__ == "__main__":
    main()