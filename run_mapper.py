#!/usr/bin/env python3
import csv
import sys

COLS = ['id', 'dur', 'proto', 'service', 'state', 'spkts', 'dpkts', 'sbytes', 'dbytes', 'rate',
    'sttl', 'dttl', 'sload', 'dload', 'sloss', 'dloss', 'sinpkt', 'dinpkt', 'sjit', 'djit',
    'swin', 'stcpb', 'dtcpb', 'dwin', 'tcprtt', 'synack', 'ackdat', 'smean', 'dmean',
    'trans_depth', 'response_body_len', 'ct_srv_src', 'ct_state_ttl', 'ct_dst_ltm',
    'ct_src_dport_ltm', 'ct_dst_sport_ltm', 'ct_dst_src_ltm', 'is_ftp_login', 'ct_ftp_cmd',
    'ct_flw_http_mthd', 'ct_src_ltm', 'ct_srv_dst', 'is_sm_ips_ports', 'attack_cat', 'label']

IDX = {c: i for i, c in enumerate(COLS)}
li, si, di, ti = IDX['label'], IDX['sbytes'], IDX['dbytes'], IDX['dur']

with open('data/UNSW_NB15_training-set.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) != len(COLS) or row[0].strip().lower() == 'id':
            continue
        key = '|'.join((row[i].strip() or '-') for i in [IDX['attack_cat'], IDX['proto']])
        sb = int(float(row[si])) if row[si] else 0
        db = int(float(row[di])) if row[di] else 0
        dur = float(row[ti]) if row[ti] else 0
        # Force UTF-8 output to avoid Windows UTF-16 default
        line = key + '\t1\t' + str(int(float(row[li]))) + '\t' + str(sb) + '\t' + str(db) + '\t' + str(dur) + '\n'
        sys.stdout.buffer.write(line.encode('utf-8'))