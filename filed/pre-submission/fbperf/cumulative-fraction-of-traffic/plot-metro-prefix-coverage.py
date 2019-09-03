#!/usr/bin/env python3

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plot_cdf(fn, label):
    xs = list()
    ys = list()
    fd = open(fn, 'r')
    for line in fd:
        x, y = line.split()
        xs.append(float(x))
        ys.append(float(y))
    plt.step(xs, ys, label=label, where='post')


plt.xlabel("Number of Prefixes to Cover Percentage of Traffic", fontsize=16)
plt.ylabel("Cumulative Fraction of Metros", fontsize=16)
plt.xlim(0, 10000)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plot_cdf('per-metro/metro-num-prefixes-p90.cdf', '90% of traffic')
plot_cdf('per-metro/metro-num-prefixes-p95.cdf', '95% of traffic')
plot_cdf('per-metro/metro-num-prefixes-p99.cdf', '99% of traffic')
plt.legend(loc="best")
plt.savefig('metro-num-prefixes.pdf')

plt.clf()

plt.xlabel("Fraction of Prefixes to Cover Percentage of Traffic", fontsize=16)
plt.ylabel("Cumulative Fraction of Metros", fontsize=16)
plt.xlim(0, 1)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plot_cdf('per-metro/metro-frac-prefixes-p90.cdf', '90% of traffic')
plot_cdf('per-metro/metro-frac-prefixes-p95.cdf', '95% of traffic')
plot_cdf('per-metro/metro-frac-prefixes-p99.cdf', '99% of traffic')
plt.legend(loc="best")
plt.savefig('metro-frac-prefixes.pdf')

