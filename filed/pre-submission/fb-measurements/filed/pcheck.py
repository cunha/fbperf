#!/usr/bin/env python3

import sys

k1, k2, rtt, rtt95, weight = sys.stdin.readline().split(',')
key = k1, k2
values = set([rtt])
values2 = set([rtt95])
rows = 1

for line in sys.stdin:
    k1, k2, rtt, rtt95, weight = line.split(',')
    newk = k1, k2
    if newk != key:
        sys.stdout.write('%s %s %d %d %d %f\n' % (key[0], key[1], rows, len(values), len(values2), float(weight)))
        key = newk
        values = set()
        values2 = set()
        rows = 0
    rows += 1
    values.add(rtt)
    values2.add(rtt95)
