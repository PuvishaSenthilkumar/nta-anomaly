"""Train two detectors on the curated UNSW-NB15 data and save them to HDFS.

  1. Supervised  : Random Forest vs Gradient-Boosted Trees (binary label) -> best by AUC-ROC
  2. Unsupervised: KMeans fitted on NORMAL traffic only; distance to the nearest centroid is the
                 anomaly score; threshold = 99th percentile of that distance on normal training flows.
                 This catches traffic that "doesn't look normal" even when the classifier has never
                 seen that attack type.

    spark-submit train.py [--k 8] [--trees 100]
"""
import argparse
import json

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import Row
from pyspark.sql import functions as F

from common import CATEGORICAL, CURATED_DIR, MODELS_DIR, NUMERIC, distance_udf, get_spark


def build_prep():
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL]
    ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL],
                        outputCols=[f"{c}_ohe" for c in CATEGORICAL],
                        handleInvalid="keep", dropLast=False)
    input_cols = [f"{c}_ohe" for c in CATEGORICAL] + NUMERIC
    asm = VectorAssembler(inputCols=input_cols, outputCol="features", handleInvalid="keep")
    # withMean=False keeps the sparse one-hot vectors sparse
    scl = StandardScaler(inputCol="features", outputCol="scaled", withMean=False, withStd=True)
    return idx + [ohe, asm, scl], input_cols


def evaluate(pred, label="label"):
    auc = BinaryClassificationEvaluator(labelCol=label, metricName="areaUnderROC").evaluate(pred)
    pr = BinaryClassificationEvaluator(labelCol=label, metricName="areaUnderPR").evaluate(pred)
    m = MulticlassClassificationEvaluator(labelCol=label, predictionCol="prediction")
    return {
        "auc_roc": auc, "auc_pr": pr,
        "accuracy": m.setMetricName("accuracy").evaluate(pred),
        "f1": m.setMetricName("f1").evaluate(pred),
        "precision_w": m.setMetricName("weightedPrecision").evaluate(pred),
        "recall_w": m.setMetricName("weightedRecall").evaluate(pred),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=int, default=8, help="KMeans clusters")
    ap.add_argument("--trees", type=int, default=100)
    ap.add_argument("--gbt-iter", type=int, default=40)
    args = ap.parse_args()

    spark = get_spark("nta-train")
    train = spark.read.parquet(f"{CURATED_DIR}/train").cache()
    test = spark.read.parquet(f"{CURATED_DIR}/test").cache()
    print(f"[train] train={train.count()} test={test.count()}")

    prep_stages, input_cols = build_prep()
    prep = Pipeline(stages=prep_stages).fit(train)
    tr, te = prep.transform(train).cache(), prep.transform(test).cache()

    # ---------------- supervised ----------------
    candidates = {
        "random_forest": RandomForestClassifier(labelCol="label", featuresCol="features",
                                                numTrees=args.trees, maxDepth=12, seed=42),
        "gbt": GBTClassifier(labelCol="label", featuresCol="features", maxIter=args.gbt_iter,
                             maxDepth=6, seed=42),
    }
    results, models = {}, {}
    for name, est in candidates.items():
        model = est.fit(tr)
        results[name] = evaluate(model.transform(te))
        models[name] = model
        print(f"[train] {name}: {json.dumps(results[name], indent=2)}")

    best = max(results, key=lambda n: results[n]["auc_roc"])
    print(f"[train] best classifier: {best}")
    best_model = models[best]

    # Probability column is needed downstream, and GBT in Spark 3.5 does provide it for binary labels.
    clf = PipelineModel(stages=prep.stages + [best_model])
    clf.write().overwrite().save(f"{MODELS_DIR}/classifier")

    # Feature names come from the VectorAssembler's column metadata (OHE blocks expand to many slots).
    attrs = te.schema["features"].metadata["ml_attr"]["attrs"]
    names = {a["idx"]: a["name"] for group in attrs.values() for a in group}
    fi = best_model.featureImportances
    top = sorted(((names.get(i, str(i)), fi[i]) for i in range(fi.size)), key=lambda x: -x[1])[:15]
    print("[train] top features:")
    for n, v in top:
        print(f"    {n:28s} {v:.4f}")

    # ---------------- confusion matrix ----------------
    pred = best_model.transform(te)
    pred.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
    pred.groupBy("attack_cat").agg(
        F.count("*").alias("n"),
        F.round(F.avg((F.col("prediction") == F.col("label")).cast("double")), 4).alias("acc"),
    ).orderBy(F.desc("n")).show(20, False)

    # ---------------- unsupervised ----------------
    normal = tr.filter(F.col("label") == 0)
    km = KMeans(k=args.k, featuresCol="scaled", predictionCol="cluster", seed=42, maxIter=30).fit(normal)
    km.write().overwrite().save(f"{MODELS_DIR}/kmeans")
    dist = distance_udf([c.tolist() for c in km.clusterCenters()])

    def with_dist(df):
        return km.transform(df).withColumn("anomaly_dist", dist("cluster", "scaled"))

    thr = with_dist(normal).approxQuantile("anomaly_dist", [0.99], 0.001)[0]
    spark.createDataFrame([Row(threshold=float(thr))]).write.mode("overwrite").parquet(f"{MODELS_DIR}/kmeans_threshold")

    scored = with_dist(te).withColumn("is_outlier", (F.col("anomaly_dist") > thr).cast("int"))
    tp = scored.filter((F.col("label") == 1) & (F.col("is_outlier") == 1)).count()
    fp = scored.filter((F.col("label") == 0) & (F.col("is_outlier") == 1)).count()
    fn = scored.filter((F.col("label") == 1) & (F.col("is_outlier") == 0)).count()
    km_metrics = {
        "threshold_p99_normal": thr,
        "precision": tp / max(tp + fp, 1),
        "recall": tp / max(tp + fn, 1),
    }
    print(f"[train] kmeans: {km_metrics}")

    spark.createDataFrame([Row(report=json.dumps({"classifiers": results, "best": best, "kmeans": km_metrics}))]) \
        .coalesce(1).write.mode("overwrite").json(f"{MODELS_DIR}/metrics")
    print(f"[train] models saved under {MODELS_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()