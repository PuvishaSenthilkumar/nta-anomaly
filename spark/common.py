"""Shared constants / helpers for the PySpark jobs."""
import math
import os

from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

HDFS = os.environ.get("HDFS_URI", "hdfs://namenode:8020")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")

RAW_DIR = f"{HDFS}/nta/raw"
CURATED_DIR = f"{HDFS}/nta/curated"
MODELS_DIR = f"{HDFS}/nta/models"
OUTPUT_DIR = f"{HDFS}/nta/output"

STRING_COLS = ["proto", "service", "state", "attack_cat"]
INT_COLS = ["id", "label"]
# Column order of UNSW_NB15_training-set.csv / UNSW_NB15_testing-set.csv
ALL_COLS = [
    "id", "dur", "proto", "service", "state", "spkts", "dpkts", "sbytes", "dbytes", "rate",
    "sttl", "dttl", "sload", "dload", "sloss", "dloss", "sinpkt", "dinpkt", "sjit", "djit",
    "swin", "stcpb", "dtcpb", "dwin", "tcprtt", "synack", "ackdat", "smean", "dmean",
    "trans_depth", "response_body_len", "ct_srv_src", "ct_state_ttl", "ct_dst_ltm",
    "ct_src_dport_ltm", "ct_dst_sport_ltm", "ct_dst_src_ltm", "is_ftp_login", "ct_ftp_cmd",
    "ct_flw_http_mthd", "ct_src_ltm", "ct_srv_dst", "is_sm_ips_ports", "attack_cat", "label",
]
CATEGORICAL = ["proto", "service", "state"]
NUMERIC = [c for c in ALL_COLS if c not in STRING_COLS and c not in INT_COLS]


def _ftype(c):
    if c in STRING_COLS:
        return T.StringType()
    if c in INT_COLS:
        return T.IntegerType()
    return T.DoubleType()


FLOW_SCHEMA = T.StructType([T.StructField(c, _ftype(c), True) for c in ALL_COLS])


def get_spark(app_name, **conf):
    b = (SparkSession.builder.appName(app_name)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.cassandra.connection.host", CASSANDRA_HOST))
    for k, v in conf.items():
        b = b.config(k.replace("__", "."), v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def distance_udf(centers):
    """Euclidean distance between a feature vector and its assigned KMeans centroid."""
    cs = [Vectors.dense(c) for c in centers]

    @F.udf(T.DoubleType())
    def dist(cluster, vec):
        if vec is None or cluster is None:
            return None
        return float(math.sqrt(Vectors.squared_distance(vec, cs[int(cluster)])))

    return dist