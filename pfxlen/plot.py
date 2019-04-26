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
    parser.add_argument('--spreads',
            dest='spreads',
            action='store',
            nargs='+',
            metavar='PAIRS',
            type=str,
            required=False,
            default=[(10, 90), (25, 75)],
            help='MinRTT spreads %(default)s')
    parser.add_argument('--pfx-lengths',
            dest='lengths',
            action='store',
            nargs='+',
            metavar='NUMBERS',
            type=int,
            required=False,
            default=list(range(16, 25, 2)) + list(range(32, 65, 8)),
            help='Prefix lengths to consider %(default)s')
    parser.add_argument('--pfx-classes',
            dest='classes',
            action='store',
            nargs='+',
            metavar='CLASSES',
            type=str,
            required=False,
            default=['BGP4', 'ASN4', 'S24'],
            help='Prefix classes to consider %(default)s')
    return parser


def read_traffic_ratios(fpath, opts):
    desc2ratio = dict()
    with open(fpath) as fd:
        for line in fd:
            prefixClass, _bytes, ratio = line.split()
            desc2ratio[prefixClass] = float(ratio)
    return desc2ratio


def read_spread_quantiles(fpath):
    pfxlen2spreadqtiles = dict()
    with open(fpath) as fd:
        for line in fd:
            pfxlen, p25, p50, p75 = line.split()
            pfxlen = int(pfxlen)
            p25 = int(p25)
            p50 = int(p50)
            p75 = int(p75)
            pfxlen2spreadqtiles[pfxlen] = (p25, p50, p75)
    return pfxlen2spreadqtiles


def read_cdf(fpath):
    cdf = list()
    with open(fpath) as fd:
        for line in fd:
            x, y = line.split()
            cdf.append((float(x), float(y)))
    return cdf


def plot_cdfs(label2cdf, xlim, xlabel, outfn):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel("Cumulative Fraction of\n(Prefix, Metro, Time) Triplets", fontsize=16)
    ax1.set_xlim(xlim[0], xlim[1])
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for label, cdf in label2cdf.items():
        xs, ys = zip(*cdf)
        ax1.plot(xs, ys, label=label)
    plt.legend(loc='best')
    plt.grid()
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)


def plot_spread_quantiles(pfxlen2spreadqtiles, spread, xlim, ylim, outfn):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel("Prefix Length", fontsize=16)
    ax1.set_ylabel("MinRTT (P%d, P%d) Spread [ms]" % spread, fontsize=16)
    ax1.set_xlim(xlim[0], xlim[1])
    ax1.set_ylim(ylim[0], ylim[1])
    fig.tight_layout()
    xs, qtiles = zip(*pfxlen2spreadqtiles.items())
    p25s, p50s, p75s = zip(*qtiles)
    ax1.plot(xs, p25s, '^', label="Spread P25")
    ax1.plot(xs, p50s, 'o', label="Median Spread")
    ax1.plot(xs, p75s, 'v', label="Spread P75")
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

    if isinstance(opts.spreads[0], str):
        splits = list(s.split(',') for s in opts.spreads)
        opts.streads = list((int(s[0]), int(s[1])) for s in splits)

    fpath = os.path.join(opts.outdir, 'traffic_ratios.txt')
    desc2ratio = read_traffic_ratios(fpath, opts)

    for spread in opts.spreads:
        lo, up = spread
        logging.info('Processing spread (%d, %d)', lo, up)

        fpath = os.path.join(opts.outdir,
                'pfxlen_%dspread%d_qtiles.txt' % spread)
        pfxlen2spreadqtiles = read_spread_quantiles(fpath)
        outfn = os.path.join(opts.outdir,
                'pfxlen_%dspread%d_qtiles.pdf' % spread)
        plot_spread_quantiles(pfxlen2spreadqtiles, spread, (15, 65), (0, 100),
                outfn)

        label2cdf = dict()
        for pfxlen in opts.lengths:
            fpath = os.path.join(opts.outdir,
                    'pfxlen%d_%dspread%d.cdf' % (pfxlen, lo, up))
            if not os.path.exists(fpath):
                logging.warning('File %s does not exist, skipping', fpath)
                continue
            label = '/%d prefixes [%d%% of traffic]' % (pfxlen,
                    int(100*desc2ratio[str(pfxlen)]))
            label2cdf[label] = read_cdf(fpath)
        xlabel = "MinRTT (P%d, P%d) Spread [ms]" % spread
        outfn = os.path.join(opts.outdir, 'pfxlen_%dspread%d_cdf.pdf' % spread)
        plot_cdfs(label2cdf, (0, 100), xlabel, outfn)

        label2cdf = dict()
        for cls in opts.classes:
            fpath = os.path.join(opts.outdir,
                    'pfxlen%s_%dspread%d.cdf' % (cls, lo, up))
            if not os.path.exists(fpath):
                logging.warning('File %s does not exist, skipping', fpath)
                continue
            traffic = '%s [%d%% of traffic]' % (cls, int(100*desc2ratio[cls]))
            label2cdf[label] = read_cdf(fpath)
        xlabel = "MinRTT (P%d, P%d) Spread [ms]" % spread
        outfn = os.path.join(opts.outdir, 'class_%dspread%d_cdf.pdf' % spread)
        plot_cdfs(label2cdf, (0, 100), xlabel, outfn)


if __name__ == '__main__':
    sys.exit(main())
