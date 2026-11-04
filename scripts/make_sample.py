#!/usr/bin/env python3
"""Generate a small SYNTHETIC dataset with the UNSW-NB15 training/testing-set column layout.

For smoke-testing the pipeline without downloading the real data. NOT realistic traffic.
    python scripts/make_sample.py --rows 3000 --out data/sample
"""
import argparse
import csv
import os
import random

COLS = [
    "id", "dur", "proto", "service", "state", "spkts", "dpkts", "sbytes", "dbytes", "rate",
    "sttl", "dttl", "sload", "dload", "sloss", "dloss", "sinpkt", "dinpkt", "sjit", "djit",
    "swin", "stcpb", "dtcpb", "dwin", "tcprtt", "synack", "ackdat", "smean", "dmean",
    "trans_depth", "response_body_len", "ct_srv_src", "ct_state_ttl", "ct_dst_ltm",
    "ct_src_dport_ltm", "ct_dst_sport_ltm", "ct_dst_src_ltm", "is_ftp_login", "ct_ftp_cmd",
    "ct_flw_http_mthd", "ct_src_ltm", "ct_srv_dst", "is_sm_ips_ports", "attack_cat", "label",
]
CATS = ["Generic", "Exploits", "Fuzzers", "DoS", "Reconnaissance", "Analysis", "Backdoor", "Shellcode", "Worms"]


def flow(i, attack):
    r = random.random
    proto = random.choice(["tcp", "udp", "icmp"] if attack else ["tcp", "udp", "tcp", "tcp"])
    sp = int(random.expovariate(1 / (40 if attack else 10))) + 1
    dp = int(random.expovariate(1 / (4 if attack else 12))) + 1
    sb, db = sp * random.randint(40, 1400), dp * random.randint(40, 1400)
    dur = random.expovariate(1 / (0.05 if attack else 0.8))
    row = {
        "id": i, "dur": dur, "proto": proto, "service": random.choice(["-", "http", "dns", "ftp", "smtp"]),
        "state": random.choice(["FIN", "CON", "INT"]), "spkts": sp, "dpkts": dp, "sbytes": sb, "dbytes": db,
        "rate": (sp + dp) / max(dur, 1e-6), "sttl": random.choice([254, 62, 31] if attack else [62, 254]),
        "dttl": random.choice([252, 0] if attack else [252, 29]), "sload": sb * 8 / max(dur, 1e-6),
        "dload": db * 8 / max(dur, 1e-6), "sloss": 0, "dloss": 0, "sinpkt": dur / sp, "dinpkt": dur / dp,
        "sjit": r() * 50, "djit": r() * 50, "swin": 255, "stcpb": random.randint(0, 2**31),
        "dtcpb": random.randint(0, 2**31), "dwin": 255, "tcprtt": r() * 0.1, "synack": r() * 0.05,
        "ackdat": r() * 0.05, "smean": sb // sp, "dmean": db // dp, "trans_depth": 0, "response_body_len": 0,
        "ct_srv_src": random.randint(1, 30 if attack else 10), "ct_state_ttl": random.choice([0, 1, 2, 6]),
        "ct_dst_ltm": random.randint(1, 20), "ct_src_dport_ltm": random.randint(1, 20),
        "ct_dst_sport_ltm": random.randint(1, 20), "ct_dst_src_ltm": random.randint(1, 30),
        "is_ftp_login": 0, "ct_ftp_cmd": 0, "ct_flw_http_mthd": 0, "ct_src_ltm": random.randint(1, 20),
        "ct_srv_dst": random.randint(1, 30), "is_sm_ips_ports": 0,
        "attack_cat": random.choice(CATS) if attack else "Normal", "label": int(attack),
    }
    return [row[c] for c in COLS]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=3000)
    ap.add_argument("--out", default="data/sample")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    random.seed(a.seed)
    os.makedirs(a.out, exist_ok=True)
    for name, n in (("training", a.rows), ("testing", a.rows // 2)):
        with open(f"{a.out}/UNSW_NB15_{name}-set.csv", "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(COLS)
            for i in range(1, n + 1):
                w.writerow(flow(i, random.random() < 0.55))
    print(f"wrote synthetic files to {a.out}/")


if __name__ == "__main__":
    main()
