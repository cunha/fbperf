#!/usr/bin/env python3

import math
import random
import sys


def median(sortedvec):
    n = len(sortedvec)
    i50 = int(n / 2 - 0.5)
    w50 = (n / 2 - 0.5) - i50
    return (1 - w50) * sortedvec[i50] + w50 * sortedvec[i50 + 1]


def cvalue(n):
    c = (n + 1) / 2 - math.sqrt(n)
    return int(round(c))


def medianvar(sortedvec, zj):
    n = len(sortedvec)
    c = (n + 1) / 2 - math.sqrt(n)
    c = int(round(c))
    return ((sortedvec[n - c] - sortedvec[c - 1]) / (2 * zj)) ** 2


def median_diff_ci(sortedvec1, sortedvec2, z=1.96, zj=2):
    med1, med2 = median(sortedvec1), median(sortedvec2)
    var1, var2 = medianvar(sortedvec1, zj), medianvar(sortedvec2, zj)
    md = med1 - med2
    interval = z * math.sqrt(var1 + var2)
    return (md - interval, md + interval)


def paper_example():
    v1 = [
        4.1,
        7.7,
        17.5,
        31.4,
        32.7,
        40.6,
        92.4,
        115.3,
        118.3,
        119.0,
        129.6,
        198.6,
        200.7,
        242.5,
        255.0,
        274.7,
        274.7,
        302.8,
        334.1,
        430.0,
        489.1,
        703.4,
        978.0,
        1656.0,
        1697.8,
        2745.6,
    ]
    v2 = [
        1.0,
        4.9,
        4.9,
        11.5,
        17.3,
        21.7,
        24.4,
        26.1,
        26.3,
        28.6,
        29.0,
        36.6,
        41.1,
        47.3,
        68.5,
        81.2,
        87.0,
        95.0,
        147.8,
        163.0,
        244.3,
        321.2,
        345.5,
        372.4,
        830.1,
        1202.6,
    ]
    zj = 2.184
    assert len(v1) == len(v2)
    med1, med2 = median(v1), median(v2)
    var1, var2 = medianvar(v1, zj), medianvar(v2, zj)
    lo, up = median_diff_ci(v1, v2, zj=zj)

    sys.stdout.write(
        "v1 %f %f\nv2 %f %f\nci %f %f\n" % (med1, var1, med2, var2, lo, up)
    )


def main():
    for ns in range(10, 1000, 100):
        v1, v2 = list(), list()
        for _ in range(ns):
            v1.append(random.uniform(0, 50))
            v2.append(random.uniform(20, 30))
        v1.sort()
        v2.sort()
        zj = 2
        med1, med2 = median(v1), median(v2)
        var1, var2 = medianvar(v1, zj), medianvar(v2, zj)
        lo, up = median_diff_ci(v1, v2, zj=zj)

        # sys.stdout.write('v1 %f %f\nv2 %f %f\n###########\nci %f %f\n' % (
        sys.stdout.write("##########################\n")
        sys.stdout.write("%d ci %f %f\n" % (ns, lo, up))
        sys.stdout.write("  v1 %f %f %d\n" % (med1, var1, cvalue(len(v1))))
        sys.stdout.write("  v2 %f %f %d\n" % (med2, var2, cvalue(len(v2))))


if __name__ == "__main__":
    sys.exit(main())
