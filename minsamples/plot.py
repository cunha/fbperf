#!/usr/bin/env python3

from collections import defaultdict
import argparse
import logging
import os
import sys

import matplotlib.pyplot as plt


def create_parser():
    desc = '''Plot graphs of CI sizes vs number of sammples'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--outdir',
            dest='outdir',
            action='store',
            metavar='PATH',
            type=str,
            required=True,
            help='Directory where data files are located')
    parser.add_argument('--percentiles',
            dest='percentiles',
            action='store',
            nargs='+',
            metavar='NUMBERS',
            required=False,
            default=[10, 50],
            help='Percentiles on min RTT %(default)s')
    parser.add_argument('--limits',
            dest='limits',
            action='store',
            nargs='+',
            metavar='NUMBERS',
            required=False,
            default=[0, 50, 100, 150, 200, 250, 500, 750, 1000],
            help='Limits on the number of samples %(default)s')
    return parser


# def get_file_lines(fpath):
#     with open(fpath) as f:
#         i = 0
#         for i, _l in enumerate(f):
#             pass
#         return i


def get_total_traffic(fpath):
    total = 0
    with open(fpath) as fd:
        for line in fd:
            _samples, bytes_acked, *_fields = line.split()
            try:
                total += int(bytes_acked)
            except ValueError:
                continue
    return total


def read_cdf(fpath):
    cdf = list()
    with open(fpath) as fd:
        for line in fd:
            x, y = line.split()
            cdf.append((float(x), float(y)))
    return cdf


def cdf_get_inv_percentiles(cdf, percentiles):
    invp = list()
    i = 1
    for p in percentiles:
        if p >= 1.0:
            raise ValueError('percentiles must be smaller than 1.0')
        while cdf[i][1] < p:
            i += 1
        invp.append(cdf[i-1][0])
    assert len(invp) == len(percentiles)
    return invp


def plot_ci_size_cdfs(limit2traffic, limit2cdf, xlim, xlabel, outfn):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel("Cumulative Fraction of\n(Prefix, Metro, Time) Triplets", fontsize=16)
    ax1.set_xlim(xlim[0], xlim[1])
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for limit, traffic in limit2traffic.items():
        label = '%d samples [%d%% of traffic]' % (limit, int(100*traffic))
        xs, ys = zip(*limit2cdf[limit])
        ax1.plot(xs, ys, label=label)
    plt.legend(loc='best')
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)


def plot_limits(limit2traffic, limit2cdf, pct, outfn):
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.set_xlim(0, 1000)
    ax1.set_xlabel("Minimum Number of Samples", fontsize=16)
    ax1.set_ylabel("P90 of P%d MinRTT CI Size [ms]" % pct, fontsize=16)
    ax1.set_ylim(0, 50)
    ax2.set_ylabel("Fraction of Traffic", fontsize=16)
    ax2.set_ylim(0.5, 1)
    fig.tight_layout()
    xs, cdfs = zip(*list(sorted(limit2cdf.items())))
    qtiles = list(tuple(cdf_get_inv_percentiles(cdf, [0.50, 0.90]))
            for cdf in cdfs)
    _p50s, p90s = zip(*qtiles)
    # ax1.fill_between(xs, p10s, p90s, linewidth=0, linestyle=None, color="#333333", alpha=0.25)
    ax1.plot(xs, p90s, 'ko-', linewidth=2, markersize=5, label="CI Size")
    ax1.legend(loc='lower left')
    xs, ys = zip(*list(sorted(limit2traffic.items())))
    ax2.plot(xs, ys, 'bx--', linewidth=2, markersize=5, label="Traffic")
    ax2.legend(loc='upper right')
    # plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)


def main():
    parser = create_parser()
    opts = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger('matplotlib').setLevel(logging.ERROR)

    for pct in opts.percentiles:
        limit2cdf = defaultdict(list)
        limit2relcdf = defaultdict(list)
        limit2traffic = dict()

        fpath = os.path.join(opts.outdir, 'ci_rows_0samples.txt')
        global_traffic = get_total_traffic(fpath)
        logging.info('global_traffic %d', global_traffic)

        for limit in opts.limits:
            fpath = os.path.join(opts.outdir, 'ci_rows_%dsamples.txt' % limit)
            if not os.path.exists(fpath):
                logging.warning('File %s does not exist, skipping', fpath)
                continue
            limit2traffic[limit] = get_total_traffic(fpath)
            logging.info('%dsamples traffic %d', limit, limit2traffic[limit])
            limit2traffic[limit] /= global_traffic

            fpath = os.path.join(opts.outdir,
                    'ci_size_%dsamples_rtt%d.cdf' % (limit, pct))
            if not os.path.exists(fpath):
                logging.warning('File %s does not exist, skipping', fpath)
                continue
            limit2cdf[limit] = read_cdf(fpath)

            fpath = os.path.join(opts.outdir,
                    'ci_relsize_%dsamples_rtt%d.cdf' % (limit, pct))
            if not os.path.exists(fpath):
                logging.warning('File %s does not exist, skipping', fpath)
                continue
            limit2relcdf[limit] = read_cdf(fpath)

        xlabel = "P%d MinRTT Confidence Interval Size (ms)" % pct
        outfn = os.path.join(opts.outdir, 'ci_size_rtt%d.pdf' % pct)
        plot_ci_size_cdfs(limit2traffic, limit2cdf, (0, 50), xlabel, outfn)

        xlabel = "P%d MinRTT Relative Confidence Interval Size (CI/MinRTT)" % pct
        outfn = os.path.join(opts.outdir, 'ci_relsize_rtt%d.pdf' % pct)
        plot_ci_size_cdfs(limit2traffic, limit2relcdf, (0, 1), xlabel, outfn)

        outfn = os.path.join(opts.outdir, 'ci_size_limits_rtt%d.pdf' % pct)
        plot_limits(limit2traffic, limit2cdf, pct, outfn)


if __name__ == '__main__':
    sys.exit(main())
