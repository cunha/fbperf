#!/usr/bin/env python3

import math
import sys

import matplotlib.pyplot as plt

def confidence_interval_percentiles(p, n, z=1.959964):
    if not isinstance(p, int) or p > 100:
        raise ValueError('Percentiles must be integers between 0 and 100')
    pp = p / 100.0
    diff = 100 * z * math.sqrt(pp * (1 - pp) / n)
    lower = max(math.floor(p - diff), 0)
    upper = min(math.ceil(p + diff), 100)
    return (lower, upper)

def main():
    w = sys.stdout.write
    w('# Lower and upper percentiles for confidence intervals\n')
    w('PCTILE\tSAMPLES\tLOWER\tUPPER\n')
    fig, ax1 = plt.subplots(figsize=(12,8))

    ns = range(20, 10000, 10)
    for p in [5, 10, 25, 50]:
       ys = [confidence_interval_percentiles(p, n)[0] for n in ns]
       label = 'P%s (lower)' % p
       ax1.plot(ns, ys, lw=4, label=label)

    for p in [5, 10, 25, 50, 75, 90, 95]:
       for nsamples in [50, 100, 200]:
           lower, upper = confidence_interval_percentiles(p, nsamples)
           w('%d\t%d\t%d\t%d\n' % (p, nsamples, lower, upper))

    plt.tick_params(axis='both', which='major', color='black', labelsize=20)
    ax1.set_xlabel('Sample Size', fontsize=24)
    ax1.set_ylabel('Confidence Interval Bound (Percentile)', fontsize=24)
    ax1.set_xlim(0, ns[-1])
    ax1.set_ylim(0, 50)
    fig.tight_layout()
    plt.legend(fontsize=24, loc='upper right')
    plt.savefig('min-percentiles.png', bbox_inches='tight')
    plt.close()


if __name__ == '__main__':
    sys.exit(main())
