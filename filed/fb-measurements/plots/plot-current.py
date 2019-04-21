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

# RTT P50

plt.xlabel("Median RTT [ms]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(10, 160)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p50-best.cdf', 'Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p50-pref.cdf', 'Peer Type + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p50-preflen.cdf', 'Peer Type + AS-path Length + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p50-current.cdf', 'Current Policy')
plt.legend(loc="best")
plt.savefig('rtt-p50-current.pdf')

plt.clf()

plt.xlabel("Median RTT [ms]", fontsize=16)
plt.ylabel("Cumulative Fraction of Traffic", fontsize=16)
plt.xlim(10, 160)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p50-best-weight.cdf', 'Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p50-pref-weight.cdf', 'Peer Type + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p50-preflen-weight.cdf', 'Peer Type + AS-path Length + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p50-current-weight.cdf', 'Current Policy')
plt.legend(loc="best")
plt.savefig('rtt-p50-current-weight.pdf')

# RTT P95

plt.clf()

plt.xlabel("Tail RTT [ms]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(60, 600)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p95-best.cdf', 'Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p95-pref.cdf', 'Peer Type + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p95-preflen.cdf', 'Peer Type + AS-path Length + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p95-current.cdf', 'Current Policy')
plt.legend(loc="best")
plt.savefig('rtt-p95-current.pdf')

plt.clf()

plt.xlabel("Tail RTT [ms]", fontsize=16)
plt.ylabel("Cumulative Fraction of Traffic", fontsize=16)
plt.xlim(60, 600)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p95-best-weight.cdf', 'Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p95-pref-weight.cdf', 'Peer Type + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p95-preflen-weight.cdf', 'Peer Type + AS-path Length + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-rtt_ms_p95-current-weight.cdf', 'Current Policy')
plt.legend(loc="best")
plt.savefig('rtt-p95-current-weight.pdf')

# HD

plt.clf()

plt.xlabel("HD capable fraction [fraction of samples]", fontsize=16)
plt.ylabel("Cumulative Fraction of Prefixes", fontsize=16)
plt.xlim(0, 1)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-hd_capable_frac-best.cdf', 'Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-hd_capable_frac-pref.cdf', 'Peer Type + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-hd_capable_frac-preflen.cdf', 'Peer Type + AS-path Length + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-hd_capable_frac-current.cdf', 'Current Policy')
plt.legend(loc="best")
plt.savefig('hdfrac-current.pdf')

plt.clf()

plt.xlabel("HD capable [fraction of samples]", fontsize=16)
plt.ylabel("Cumulative Fraction of Traffic", fontsize=16)
plt.xlim(0, 1)
plt.ylim(0, 1)
# plt.xscale('log')
# plt.yscale('log')
plt.grid()
# plt.tight_layout()
# plt.locator_params(axis='x', nbins=6)
# plt.locator_params(axis='y', nticks=6)
# plt.xticks(x, xticks)
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-hd_capable_frac-best-weight.cdf', 'Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-hd_capable_frac-pref-weight.cdf', 'Peer Type + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-hd_capable_frac-preflen-weight.cdf', 'Peer Type + AS-path Length + Performance')
plot_cdf('data/current-private,public,transit-private,public,transit-smpl500-hd_capable_frac-current-weight.cdf', 'Current Policy')
plt.legend(loc="best")
plt.savefig('hdfrac-current-weight.pdf')



