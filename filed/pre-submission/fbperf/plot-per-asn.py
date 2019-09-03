#!/usr/bin/env python3

import argparse
import logging
import os
import pickle
import sys

import matplotlib.pyplot as plt
import numpy as np

MIN_HOURS=48


def create_parser():
    desc = '''Convert CSV to JSON'''
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--pickle',
            dest='picklefn',
            action='store',
            metavar='FILE',
            type=str,
            required=True,
            help='Pickle file with metro data')
    parser.add_argument('--graphdir',
            dest='graphdir',
            action='store',
            metavar='FILE',
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


def plot_key(opts, mpp, key, percentile):
    keystr = '%d,%d,%s,%d' % key
    title = '%s top %.2f%% with most traffic' % (keystr, percentile)

    logging.info('plotting %s, %d timestamps, percentile %f',
                 keystr, len(mpp.key2data[key].time2perf), percentile)

    nsamples2perfs = dict()
    for nsamples in [100, 250, 500]:
        perfs = list(p for p in mpp.key2data[key].time2perf.values()
                     if p['samples'] > nsamples)
        nsamples2perfs[nsamples] = perfs
        logging.info('%d hours with %d samples', len(perfs), nsamples)
    if len(nsamples2perfs[min(nsamples2perfs.keys())]) < MIN_HOURS:
        return

    metric2label = {'minrtt_min': 'Minimum of connection minRTTs',
                    'minrtt_p5': 'P5 of connection minRTTs',
                    'minrtt_p50': 'P50 of connection minRTTs',
                    'minrtt_p95': 'P95 of connection minRTTs'}

    # minRTT
    for nsamples, perfs in nsamples2perfs.items():
        if len(perfs) < MIN_HOURS:
            continue
        outfn = os.path.join(opts.graphdir,
                             'cdf-%s-samples%d-minrtt.pdf' % (keystr, nsamples))
        fig, ax1 = plt.subplots()
        ax1.set_title(title, fontsize=8)
        ax1.set_xlabel("Connection minimum RTT [ms]", fontsize=16)
        ax1.set_ylabel("Cumulative fraction of hours", fontsize=16)
        # ax1.set_xlim(0, 50)
        ax1.set_ylim(0, 1)
        # ax1.set_yscale('log')
        fig.tight_layout()
        for metric, label in metric2label.items():
            data = list(p[metric] for p in perfs)
            xs = np.sort(data)
            ys = np.linspace(0, 1, len(data))
            ax1.step(xs, ys, label=label, where='post')
        plt.grid()
        plt.legend(loc='best')
        plt.savefig(outfn, bbox_inches='tight')
        plt.close(fig)

    # Retransmission rate
    outfn = os.path.join(opts.graphdir, 'cdf-%s-retrans.pdf' % keystr)
    fig, ax1 = plt.subplots()
    ax1.set_title(title, fontsize=8)
    ax1.set_xlabel("Retransmission rate", fontsize=16)
    ax1.set_ylabel("Cumulative fraction of hours", fontsize=16)
    # ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for nsamples, perfs in nsamples2perfs.items():
        if len(perfs) < MIN_HOURS:
            continue
        data = list(p['retrans_rate'] for p in perfs)
        xs = np.sort(data)
        ys = np.linspace(0, 1, len(data))
        ax1.step(xs, ys, label='nsamples > %d' % nsamples, where='post')
    plt.grid()
    plt.legend(loc='best')
    plt.savefig(outfn, bbox_inches='tight')
    plt.close(fig)

    perfs = nsamples2perfs[250]
    if len(perfs) > MIN_HOURS:
        outfn = os.path.join(opts.graphdir, 'scatter-%s-samples250-minrtt.pdf' % keystr)
        fig, ax1 = plt.subplots()
        ax1.set_title(title, fontsize=8)
        ax1.set_xlabel("Samples", fontsize=16)
        ax1.set_ylabel("Minimum MinRTT rate", fontsize=16)
        # ax1.set_xlim(0, 1)
        # ax1.set_ylim(0, 1)
        fig.tight_layout()
        ys = list(p['minrtt_min'] for p in perfs)
        xs = list(p['samples'] for p in perfs)
        ax1.plot(xs, ys, 'r.')
        plt.grid()
        plt.savefig(outfn, bbox_inches='tight')
        plt.close(fig)

        outfn = os.path.join(opts.graphdir, 'scatter-%s-retrans.pdf' % keystr)
        fig, ax1 = plt.subplots()
        ax1.set_title(title, fontsize=8)
        ax1.set_xlabel("Samples", fontsize=16)
        ax1.set_ylabel("Retransmission rate", fontsize=16)
        # ax1.set_xlim(0, 1)
        # ax1.set_ylim(0, 1)
        fig.tight_layout()
        ys = list(p['retrans_rate'] for p in perfs)
        xs = list(p['samples'] for p in perfs)
        ax1.plot(xs, ys, 'r.')
        plt.grid()
        plt.savefig(outfn, bbox_inches='tight')
        plt.close(fig)


def main():
    parser = create_parser()
    opts = parser.parse_args()

    logging.basicConfig(filename=opts.logfile, format='%(message)s',
                        level=logging.DEBUG)
    logging.getLogger('matplotlib').setLevel(logging.ERROR)

    os.makedirs(opts.graphdir, exist_ok=True)

    with open(opts.picklefn, 'rb') as fd:
        mpp = pickle.load(fd)

    keys = set()
    weight = 0.05
    while weight <= 1:
        i = int(weight*len(mpp.sorted_volume1prefix))
        keys.add((mpp.sorted_volume1prefix[i][1],
                  i/len(mpp.sorted_volume1prefix)))
        weight += 0.05

    for key, percentile in keys:
        plot_key(opts, mpp, key, percentile)


if __name__ == '__main__':
    sys.exit(main())
