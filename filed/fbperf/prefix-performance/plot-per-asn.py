#!/usr/bin/env python3

import argparse
from collections import defaultdict
import glob
import logging
import os
import pickle
import sys

import matplotlib.pyplot as plt
import numpy as np


stat2xlabel_all = {'minrtt_min': 'Connection min RTT (per hour) [ms]',
                   'minrtt_p5': 'P-5 of connection min RTT (per hour) [ms]',
                   'retrans': 'Average retransmission rate (per hour)'}
stat2xlabel_best = {'minrtt_min': 'Global connection min RTT (over a day) [ms]',
                    'minrtt_p5': 'P-5 of connection min RTT (best hour of day) [ms]',
                    'retrans': 'Average retransmission rate (best hour of day)'}
stat2xlim = {'minrtt_min': 100,
             'minrtt_p5': 100,
             'retrans': 0.1}
das2name = {52782: 'Comcast',
            52419: 'AT&T',
            16822: 'T-Mobile',
            12351: 'Spectrum',
            52413: 'Verizon',
            1376: 'Sprint'}
asn_groups = {(1376, 16822),
              (52782, 12351),
              (52419, 52413),
              (16822, 52782, 12351)}


def create_parser():
    desc = '''Plot per-ASN results'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--datadir',
            dest='datadir',
            action='store',
            metavar='DIR',
            type=str,
            required=True,
            help='Directory with pickled data (asn2key2stats)')
    parser.add_argument('--graphdir',
            dest='graphdir',
            action='store',
            metavar='DIR',
            type=str,
            required=True,
            help='Output directory where to store graphs')
    parser.add_argument('--logfile',
            dest='logfile',
            action='store',
            metavar='FILE',
            type=str,
            default='log.txt',
            help='Log file location [%(default)s]')
    return parser


def plot_stat(opts, asn2key2stats, stat, prefixfn, asnfilter=set()):
    logging.info('plotting %s', stat)
    fnsuffix = '-'+'-'.join(str(a) for a in asnfilter) if asnfilter else ''

    asn2key2best = dict()
    for asn, key2stats in asn2key2stats.items():
        asn2key2best[asn] = dict((k, min(s for s in stats[stat])) for k, stats
                                 in key2stats.items())

    outfn = os.path.join(opts.graphdir, '%s%s-perhour%s.pdf' % (prefixfn, stat, fnsuffix))
    fig, ax1 = plt.subplots()
    # ax1.set_title(title, fontsize=8)
    ax1.set_xlabel("%s" % stat2xlabel_all[stat], fontsize=16)
    ax1.set_ylabel("Cum. frac. of metro:prefix:hour tuples", fontsize=16)
    ax1.set_xlim(0, stat2xlim[stat])
    ax1.set_ylim(0, 1)
    # ax1.set_yscale('log')
    fig.tight_layout()
    for asn, key2stats in asn2key2stats.items():
        if asnfilter and asn not in asnfilter: continue
        data = list()
        for stats in key2stats.values():
            data.extend(stats[stat])
        if len(data) == 1:
            data.append(data[0])
        xs = np.sort(data)
        ys = np.linspace(0, 1, len(data))
        ax1.step(xs, ys, label=('%s' % das2name[asn]), where='post', linewidth=3)
    plt.grid()
    # plt.legend(loc='best')
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)

    outfn = os.path.join(opts.graphdir, '%s%s-besthour%s.pdf' % (prefixfn, stat, fnsuffix))
    fig, ax1 = plt.subplots()
    # ax1.set_title(title, fontsize=8)
    ax1.set_xlabel("%s" % stat2xlabel_best[stat], fontsize=16)
    ax1.set_ylabel("Cum. frac. of metro:prefix pairs", fontsize=16)
    ax1.set_xlim(0, stat2xlim[stat])
    ax1.set_ylim(0, 1)
    # ax1.set_yscale('log')
    fig.tight_layout()
    for asn, key2best in asn2key2best.items():
        if asnfilter and asn not in asnfilter: continue
        data = list(v for _k, v in key2best.items())
        if len(data) == 1:
            data.append(data[0])
        xs = np.sort(data)
        ys = np.linspace(0, 1, len(data))
        ax1.step(xs, ys, label=('%s' % das2name[asn]), where='post', linewidth=3)
    plt.grid()
    # plt.legend(loc='best')
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)


def main():
    parser = create_parser()
    opts = parser.parse_args()

    logging.basicConfig(filename=opts.logfile, format='%(message)s',
                        level=logging.DEBUG)
    logging.getLogger('matplotlib').setLevel(logging.ERROR)

    os.makedirs(opts.graphdir, exist_ok=True)

    logging.info('loading pickles from %s', opts.datadir)
    global_asn2key2stats = defaultdict(dict)
    for fn in glob.glob(os.path.join(opts.datadir, '*')):
        with open(fn, 'rb') as fd:
            asn2key2stats = pickle.load(fd)
        for asn, key2stats in asn2key2stats.items():
            global_asn2key2stats[asn].update(key2stats)

    logging.info('plotting graphs')
    for stat in stat2xlabel_all:
        plot_stat(opts, global_asn2key2stats, stat, 'all-')
        for group in asn_groups:
            plot_stat(opts, global_asn2key2stats, stat, 'all-', group)

    filtered_asn2key2stats = dict()
    for asn, key2stats in global_asn2key2stats.items():
        filtered_asn2key2stats[asn] = dict()
        for key, stats in key2stats.items():
            if min(stats['25mbps']) is False: continue
            filtered_asn2key2stats[asn][key] = stats
        logging.info('asn %s had %d keys, but only %d with 25mbps', asn,
                len(key2stats), len(filtered_asn2key2stats[asn]))

    logging.info('plotting filtered graphs')
    for stat in stat2xlabel_all:
        plot_stat(opts, filtered_asn2key2stats, stat, '25mbps-')
        for group in asn_groups:
            plot_stat(opts, filtered_asn2key2stats, stat, '25mbps-', group)


if __name__ == '__main__':
    sys.exit(main())
