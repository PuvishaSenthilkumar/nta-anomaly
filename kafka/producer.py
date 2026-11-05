#!/usr/bin/env python3
"""Kafka producer: replays the UNSW-NB15 testing set into Kafka topic "netflows".

Usage:
    python kafka/producer.py --eps 200 --shuffle --loop
    
Options:
    --eps  Events per second (default: 200)
    --shuffle whether to shuffle the rows (default: True)
    --loop whether to loop indefinitely (default: True)
    --rows number of rows to send before exiting (for testing)
"""
import argparse
import json
import sys
import time

from kafka import KafkaProducer

COLS = ["id", "dur", "proto", "service", "state", "spkts", "dpkts", "sbytes", "dbytes", "rate",
    "sttl", "dttl", "sload", "dload", "sloss", "dloss", "sinpkt", "dinpkt", "sjit", "djit",
    "swin", "stcpb", "dtcpb", "dwin", "tcprtt", "synack", "ackdat", "smean", "dmean",
    "trans_depth", "response_body_len", "ct_srv_src", "ct_state_ttl", "ct_dst_ltm",
    "ct_src_dport_ltm", "ct_dst_sport_ltm", "ct_dst_src_ltm", "is_ftp_login", "ct_ftp_cmd",
    "ct_flw_http_mthd", "ct_src_ltm", "ct_srv_dst", "is_sm_ips_ports", "attack_cat", "label"]


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eps", type=int, default=200, help="events per second")
    ap.add_argument("--shuffle", action="store_true", default=True, help="shuffle rows")
    ap.add_argument("--loop", action="store_true", default=True, help="loop indefinitely")
    ap.add_argument("--rows", type=int, default=None, help="max rows to send")
    return ap.parse_args()


def main():
    args = parse_args()
    producer = KafkaProducer(bootstrap_servers=["localhost:29092"],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    # Load testing data
    import csv
    rows = []
    with open("data/UNSW_NB15_testing-set.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        for row in reader:
            if len(row) != 45:
                continue
            row_dict = {}
            # Map column names to values
            col_map = {
                "id": row[0], "dur": row[1], "proto": row[2], "service": row[3],
                "state": row[4], "spkts": row[5], "dpkts": row[6], "sbytes": row[7],
                "dbytes": row[8], "rate": row[9], "sttl": row[10], "dttl": row[11],
                "sload": row[12], "dload": row[13], "sloss": row[14], "dloss": row[15],
                "sinpkt": row[16], "dinpkt": row[17], "sjit": row[18], "djit": row[19],
                "swin": row[20], "stcpb": row[21], "dtcpb": row[22], "dwin": row[22],
                "tcprtt": row[23], "synack": row[24], "ackdat": row[25], "smean": row[25],
                "dmean": row[26], "trans_depth": row[26], "response_body_len": row[27],
                "ct_srv_src": row[28], "ct_state_ttl": row[29], "ct_dst_ltm": row[30],
                "ct_src_dport_ltm": row[31], "ct_dst_sport_ltm": row[32],
                "ct_dst_src_ltm": row[33], "is_ftp_login": row[34], "ct_ftp_cmd": row[35],
                "ct_flw_http_mthd": row[36], "ct_src_ltm": row[37], "ct_srv_dst": row[38],
                "is_sm_ips_ports": row[39], "attack_cat": row[40], "label": row[41],
            }
            rows.append(col_dict)

    print(f"Loaded {len(rows)} rows from data/UNSW_NB15_testing-set.csv")

    # Optionally subsample
    if args.rows:
        rows = rows[:args.rows]

    # Shuffle if requested
    import random
    if args.shuffle:
        random.seed(42)
        random.shuffle(rows)

    # Send rows to Kafka
    i = 0
    while args.loop or i < len(rows):
        for row in rows:
            # Use attack_cat as key for partitioning, label for ground-truth alongside
            key = row["attack_cat"]
            value = row
            producer.send("netflows", key=key.encode("utf-8"), value=json.dumps(value).encode("utf-8"))
            i += 1
            if args.eps and i % args.eps == 0:
                time.sleep(1)  # throttle to eps rate
            if not args.loop and i >= len(rows):
                break
        if not args.loop:
            break

    producer.flush()
    print(f"Sent {i} rows to Kafka topic 'netflows'")


if __name__ == "__main__":
    main()