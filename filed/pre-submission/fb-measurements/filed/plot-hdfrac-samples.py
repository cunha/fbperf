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


plt.xlabel("HD capable (fraction of samples)", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(-0.25, 0.25)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
plot_cdf('data/private-public-smpl500-hd_capable_frac.cdf', 'Private - BestPublic (500 samples)')
plot_cdf('data/private-public-smpl2000-hd_capable_frac.cdf', 'Private - BestPublic (2000 samples)')
plot_cdf('data/peers-transit-smpl500-hd_capable_frac.cdf', 'Peer - BestTransit (500 samples)')
plot_cdf('data/peers-transit-smpl2000-hd_capable_frac.cdf', 'Peer - BestTransit (2000 samples)')
plt.legend(loc="best")
plt.savefig('hdfrac-samples.pdf')
