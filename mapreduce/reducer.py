#!/usr/bin/env python3
"""Hadoop Streaming reducer (also usable as the combiner - the output format equals the input format).

Input : key \t flows \t attack_flows \t sbytes \t dbytes \t dur_sum   (sorted by key)
Output: same columns, one line per key.
"""
import sys
from itertools import groupby


def parse(line):
    k, *rest = line.rstrip("\n").split("\t")
    f, a, sb, db, d = rest
    return k, int(f), int(a), int(sb), int(db), float(d)


def main():
    rows = (parse(l) for l in sys.stdin if l.strip())
    for key, grp in groupby(rows, key=lambda r: r[0]):
        f = a = sb = db = 0
        d = 0.0
        for _, f1, a1, sb1, db1, d1 in grp:
            f += f1
            a += a1
            sb += sb1
            db += db1
            d += d1
        print(f"{key}\t{f}\t{a}\t{sb}\t{db}\t{d:.6f}")


if __name__ == "__main__":
    main()