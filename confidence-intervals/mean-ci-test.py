#!/usr/bin/env python3

import math
import random
import sys

def mean(xs):
    return sum(xs)/len(xs)

def var(xs):
    nx = len(xs)
    sx = sum(xs)
    ssx = sum(x*x for x in xs)
    return ssx/nx - (sx/nx)**2

def mean_ci(xs, z=1.96):
    m = mean(xs)
    v = var(xs)
    interval = z * math.sqrt(v) / math.sqrt(len(xs))
    return m - interval, m + interval

def mean_diff_ci(x1, x2, z=1.96):
    n1 = len(x1)
    var1 = var(x1)
    n2 = len(x2)
    var2 = var(x2)
    interval = z * math.sqrt(var1/n1 + var2/n2)
    diff = math.fabs(mean(x1) - mean(x2))
    return diff - interval, diff + interval

def main():
    for ns in range(10, 1000, 10):
        x1, x2 = list(), list()
        for _ in range(ns):
            x1.append(random.uniform(0.40, 0.60))
            x2.append(random.uniform(0.20, 0.80))
        m1, v1 = mean(x1), var(x1)
        ci1 = mean_ci(x1)
        m2, v2, = mean(x2), var(x2)
        ci2 = mean_ci(x2)
        sys.stdout.write('%d x1 %f %f %f %f\n' % (ns, m1, v1, ci1[0], ci1[1]))
        sys.stdout.write('%d x2 %f %f %f %f\n' % (ns, m2, v2, ci2[0], ci2[1]))
        diff_ci = mean_diff_ci(x1, x2)
        sys.stdout.write('diff %f %f\n' % diff_ci)

if __name__ == '__main__':
    sys.exit(main())
