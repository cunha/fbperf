#!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plot_cdf(fn, label):
    xs = list()
    ys = list()
    fd = open(fn, 'r')
    count = str(cdf2count[fn])
    if cdf2count[fn] > 10**6:
        count = '%dK' % int(cdf2count[fn]/(10**3))
    if cdf2count[fn] > 10**9:
        count = '%dM' % int(cdf2count[fn]/(10**6))
    if cdf2count[fn] > 10**12:
        count = '%dG' % int(cdf2count[fn]/(10**9))
    label = '%s [%s]' % (label, count)

    for line in fd:
        x, y = line.split()
        xs.append(float(x))
        ys.append(float(y))
    plt.step(xs, ys, label=label, where='post')


def read_counts():
    cdf2count = dict()
    fd = open('cdf-counts.txt')
    for line in fd:
        name, count = line.split()
        cdf2count[name] = int(float(count))
    fd.close()
    return cdf2count

cdf2count = read_counts()


plt.xlabel("RTT Difference (ms) [Peers - Transit]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(-80, 80)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
for cont in ['AF', 'AS', 'EU', 'NA', 'OC', 'SA']:
    plot_cdf('data/private,public,transit-private,public,transit-smpl500-rtt_ms_p50-%s.cdf' % cont, cont)
plt.legend(loc="best")
plt.savefig('rtt-p50-cont.pdf')

plt.clf()

plt.xlabel("RTT Difference (ms) [Private - BestPublic]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(-80, 80)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
for cont in ['AF', 'AS', 'EU', 'NA', 'OC', 'SA']:
    plot_cdf('data/private-public-smpl500-rtt_ms_p50-%s.cdf' % cont, cont)
plt.legend(loc="best")
plt.savefig('rtt-p50-cont-peers.pdf')

plt.clf()

plt.xlabel("RTT Difference (ms) [Peers - Transit]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(-80, 80)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
for cont in ['AF', 'AS', 'EU', 'NA', 'OC', 'SA']:
    plot_cdf('data/peers-transit-smpl500-rtt_ms_p50-%s.cdf' % cont, cont)
plt.legend(loc="best")
plt.savefig('rtt-p50-cont-transit.pdf')


# WEIGHTED


plt.xlabel("RTT Difference (ms) [Peers - Transit]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(-80, 80)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
for cont in ['AF', 'AS', 'EU', 'NA', 'OC', 'SA']:
    plot_cdf('data/private,public,transit-private,public,transit-smpl500-rtt_ms_p50-%s.cdf' % cont, cont)
plt.legend(loc="best")
plt.savefig('rtt-p50-cont-weight.pdf')

plt.clf()

plt.xlabel("RTT Difference (ms) [Private - BestPublic]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(-80, 80)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
for cont in ['AF', 'AS', 'EU', 'NA', 'OC', 'SA']:
    plot_cdf('data/private-public-smpl500-rtt_ms_p50-%s.cdf' % cont, cont)
plt.legend(loc="best")
plt.savefig('rtt-p50-cont-peers.pdf')

plt.clf()

plt.xlabel("RTT Difference (ms) [Peers - Transit]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(-80, 80)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
for cont in ['AF', 'AS', 'EU', 'NA', 'OC', 'SA']:
    plot_cdf('data/peers-transit-smpl500-rtt_ms_p50-%s.cdf' % cont, cont)
plt.legend(loc="best")
plt.savefig('rtt-p50-cont-transit.pdf')
