#!/usr/bin/env python

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


plt.xlabel("RTT Difference (ms)", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(-100, 100)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
plot_cdf('data/private-public-smpl500-rtt_ms_p50-avg.cdf', 'Private - AvgPublic')
plot_cdf('data/peers-transit-smpl500-rtt_ms_p50-avg.cdf', 'Peer - AvgTransit')
plot_cdf('data/private-public-smpl500-rtt_ms_p50.cdf', 'Private - BestPublic')
plot_cdf('data/peers-transit-smpl500-rtt_ms_p50.cdf', 'Peer - BestTransit')
plt.legend(loc="best")
plt.savefig('rtt-p50-avg.pdf')
