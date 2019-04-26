#!/usr/bin/env python3

import argparse
import logging
import os
import sys

import matplotlib.pyplot as plt


def create_parser():
    desc = '''Plot graphs of performance spread vs prefix length'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--outdir',
            dest='outdir',
            action='store',
            metavar='PATH',
            type=str,
            required=True,
            help='Directory where data files are located')
    parser.add_argument('--aggtypes',
            dest='aggtypes',
            action='store',
            nargs='+',
            metavar='TYPES',
            type=str,
            required=False,
            default=['asn', 'bgp', 's24'],
            help='Prefix aggregation types to consider %(default)s')
    return parser


LABELS = {
        'asn': 'Aggregate by origin ASN',
        'bgp': 'No aggregation/deaggregation',
        's24': 'Deaggregate to /24 prefixes'
}


def read_cdf(fpath):
    cdf = list()
    with open(fpath) as fd:
        for line in fd:
            x, y = line.split()
            cdf.append((float(x), float(y)))
    return cdf


def plot_cdfs(label2cdf, outfn):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel("MinRTT (P25, P75) Spread [ms]", fontsize=16)
    ax1.set_ylabel("Cumulative Fraction of IPv4 Traffic", fontsize=16)
    ax1.set_xlim(0, 50)
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for label, cdf in label2cdf.items():
        xs, ys = zip(*cdf)
        ax1.plot(xs, ys, label=label)
    plt.legend(loc='best')
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)


def main():
    parser = create_parser()
    opts = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s: %(message)s")
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger('matplotlib').setLevel(logging.ERROR)

    label2cdf = dict()

    for agg in opts.aggtypes:
        logging.info('Processing aggtype %s', agg)

        fpath = os.path.join(opts.outdir, 'aggtype_%s_25spread75_v4.cdf' % agg)
        label2cdf[LABELS[agg]] = read_cdf(fpath)

    outfn = os.path.join(opts.outdir, 'aggtype_25spread75_v4.pdf')
    plot_cdfs(label2cdf, outfn)


if __name__ == '__main__':
    sys.exit(main())
