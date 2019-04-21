#!/usr/bin/env python3

from collections import defaultdict
import argparse
import logging
import os
import sys

import matplotlib.pyplot as plt


def create_parser():
    desc = '''Plot graphs'''
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


def countlines(fpath):
    with open(fpath) as f:
        i = 0
        for i, _l in enumerate(f):
            pass
        return i


def readcdf(fpath):
    cdf = list()
    with open(fpath) as fd:
        for line in fd:
            x, y = line.split()
            cdf.append((float(x), float(y)))
    return cdf


def plot_ci_size_cdfs(limit2count, limit2cdf, xlabel, outfn):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel("Cumulative Fraction of\n(Prefix, Metro, Time) Triplets", fontsize=16)
    ax1.set_xlim(0, 200)
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for limit, count in limit2count.items():
        label = '%d samples [%d triplets]' % (limit, count)
        xs, ys = zip(*limit2cdf[limit])
        ax1.plot(xs, ys, label=label)
    plt.legend(loc='best')
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)




def main():
    parser = create_parser()
    opts = parser.parse_args()
    logging.getLogger('matplotlib').setLevel(logging.ERROR)

    for pct in opts.percentiles:
        limit2cdf = defaultdict(list)
        limit2count = dict()
        for limit in opts.limits:
            fpath = os.path.join(opts.outdir, 'ci_stats_%dsamples.txt' % limit)
            if not os.path.exists(fpath):
                logging.warning('File %s does not exist, skipping' % fpath)
                continue
            limit2count[limit] = countlines(fpath)

            fpath = os.path.join(opts.outdir,
                    'minrtt%d_ci_size_%dsamples.cdf' % (pct, limit))
            if not os.path.exists(fpath):
                logging.warning('File %s does not exist, skipping' % fpath)
                continue
            limit2cdf[limit] = readcdf(fpath)

        xlabel = "P%d MinRTT Confidence Interval Size (ms)" % pct
        outfn = os.path.join(opts.outdir, 'minrtt%d_ci_size.pdf' % pct)
        plot_ci_size_cdfs(limit2count, limit2cdf, xlabel, outfn)


if __name__ == '__main__':
    sys.exit(main())
