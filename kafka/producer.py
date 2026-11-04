#!/usr/bin/env python3
"""Replay UNSW-NB15 flows into Kafka as JSON to simulate live network telemetry.

    pip install kafka-python==2.0.2
    python kafka/producer.py --csv data/UNSW_NB15_testing-set.csv --eps 200 --shuffle --loop

Run on the host: it talks to the broker's host listener (localhost:29092).
"""
import argparse
import csv
import json
import random
import time

from kafka import KafkaProducer

STR_FIELDS = {"proto", "service", "state", "attack_cat"}
INT_FIELDS = {"id", "label"}


def convert(row):
    out = {}
    for k, v in row.items():
        if k in STR_FIELDS:
            out[k] = v
        elif k in INT_FIELDS:
            out[k] = int(float(v)) if v != "" else None
        else:
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                out[k] = None
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="data/UNSW_NB15_testing-set.csv")
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="netflows")
    ap.add_argument("--eps", type=float, default=100, help="events per second (0 = as fast as possible)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    ap.add_argument("--shuffle", action="store_true")
    ap.add_argument("--loop", action="store_true", help="replay forever")
    args = ap.parse_args()

    with open(args.csv, newline="") as fh:
        rows = [convert(r) for r in csv.DictReader(fh)]
    print(f"loaded {len(rows)} flows from {args.csv}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: str(k).encode(),
        linger_ms=20, acks=1,
    )

    sent, t0, chunk = 0, time.time(), 25
    try:
        while True:
            if args.shuffle:
                random.shuffle(rows)
            for r in rows:
                producer.send(args.topic, key=r["id"], value=r)
                sent += 1
                if args.eps and sent % chunk == 0:
                    ahead = sent / args.eps - (time.time() - t0)
                    if ahead > 0:
                        time.sleep(ahead)
                if sent % 1000 == 0:
                    print(f"sent {sent}  ({sent / (time.time() - t0):.0f} ev/s)")
                if args.limit and sent >= args.limit:
                    raise StopIteration
            if not args.loop:
                break
    except (StopIteration, KeyboardInterrupt):
        pass
    finally:
        producer.flush()
        print(f"done: {sent} events in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
