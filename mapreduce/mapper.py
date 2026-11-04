#!/usr/bin/env python3
"""Hadoop Streaming mapper for UNSW-NB15 flow records (training/testing-set CSV layout).

Emits one line per flow:  <key> \t 1 \t label \t sbytes \t dbytes \t dur
where <key> is built from the columns named on the command line (default: attack_cat proto),
joined with '|'.

    mapper.py                    -> key = attack_cat|proto
    mapper.py service state      -> key = service|state
"""
import csv
import sys

COLS = [
    "id", "dur", "proto", "service", "state", "spkts", "dpkts", "sbytes", "dbytes", "rate",
    "sttl", "dttl", "sload", "dload", "sloss", "dloss", "sinpkt", "dinpkt", "sjit", "djit",
    "swin", "stcpb", "dtcpb", "dwin", "tcprtt", "synack", "ackdat", "smean", "dmean",
    "trans_depth", "response_body_len", "ct_srv_src", "ct_state_ttl", "ct_dst_ltm",
    "ct_src_dport_ltm", "ct_dst_sport_ltm", "ct_dst_src_ltm", "is_ftp_login", "ct_ftp_cmd",
    "ct_flw_http_mthd", "ct_src_ltm", "ct_srv_dst", "is_sm_ips_ports", "attack_cat", "label",
]
IDX = {c: i for i, c in enumerate(COLS)}


def num(x):
    try:
        return float(x)
    except ValueError:
        return 0.0


def main():
    keys = sys.argv[1:] or ["attack_cat", "proto"]
    bad = [k for k in keys if k not in IDX]
    if bad:
        sys.exit(f"unknown key column(s): {bad}")
    ki = [IDX[k] for k in keys]
    li, si, di, ti = IDX["label"], IDX["sbytes"], IDX["dbytes"], IDX["dur"]

    for row in csv.reader(sys.stdin):
        # skip header lines (present only in the first split) and malformed rows
        if len(row) != len(COLS) or row[0].strip().lower() == "id":
            continue
        key = "|".join((row[i].strip() or "-") for i in ki)
        print(f"{key}\t1\t{int(num(row[li]))}\t{int(num(row[si]))}\t{int(num(row[di]))}\t{num(row[ti])}")


if __name__ == "__main__":
    main()
