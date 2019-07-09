#!/usr/bin/env python3

# from collections import OrderedDict
import datetime
from itertools import cycle
import logging
import os
import sys

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# import matplotlib.ticker as ticker


def read_ts(fpath, tstamp_idx, value_idx):
    ts = dict()
    with open(fpath) as fd:
        for line in fd:
            fields = line.split()
            tstamp = int(fields[tstamp_idx])
            value = int(fields[value_idx])
            dt = datetime.datetime.fromtimestamp(tstamp)
            ts[dt] = value
    ls = list(sorted(ts.items()))
    avg = list()
    for i in range(0, len(ls)-3, 4):
        datapoint = ls[i][0], (ls[i][1] + ls[i+1][1] + ls[i+2][1] + ls[i+3][1])/4
        avg.append(datapoint)
    return avg


def plot_minrtt(ts_all, ts_ca, ts_hi, outfn):
    outdir = os.path.split(outfn)[0]
    os.makedirs(outdir, exist_ok=True)

    lines = ["-", "--", "-.", ":"]
    linecycler = cycle(lines)
    plt.style.use('seaborn-colorblind')

    fmt = mdates.DateFormatter('%Y-%m-%d\n%H:%M')
    fmt = mdates.DateFormatter('%Y-%m-%d')

    fig, ax1 = plt.subplots(figsize=(8,4))
    # fig.autofmt_xdate()
    ax1.tick_params(axis="both", which="major", labelsize=12)
    ax1.xaxis_date()
    ax1.xaxis.set_major_formatter(fmt)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))

    xlim0 = min(ts_all)[0]
    xlim1 = max(ts_all)[0]
    print(xlim0)
    print(xlim1)

    ax1.set_xlim(xlim0, xlim1)

    ax1.set_ylabel("Median Minimum RTT [ms]", fontsize=16)
    ax1.set_ylim(10, 80)
    fig.tight_layout()

    times, samples = zip(*ts_hi)
    ax1.plot(times, samples, next(linecycler), label="Hawaii clients")

    times, samples = zip(*ts_all)
    ax1.plot(times, samples, next(linecycler), label="All clients")

    times, samples = zip(*ts_ca)
    ax1.plot(times, samples, next(linecycler), label="California clients")

    plt.legend(loc="upper right", fontsize=14)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def plot_numsamples(ts_all, ts_ca, ts_hi, outfn):
    outdir = os.path.split(outfn)[0]
    os.makedirs(outdir, exist_ok=True)

    lines = ["-", "--", "-.", ":"]
    linecycler = cycle(lines)
    plt.style.use('seaborn-colorblind')

    fmt = mdates.DateFormatter('%Y-%m-%d\n%H:%M')
    fmt = mdates.DateFormatter('%Y-%m-%d')

    fig, ax1 = plt.subplots(figsize=(8,4))
    # fig.autofmt_xdate()
    ax1.tick_params(axis="both", which="major", labelsize=12)
    ax1.xaxis_date()
    ax1.xaxis.set_major_formatter(fmt)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))

    xlim0 = min(ts_all)[0]
    xlim1 = max(ts_all)[0]
    print(xlim0)
    print(xlim1)

    ax1.set_xlim(xlim0, xlim1)

    ax1.set_ylabel("Number of Samples", fontsize=16)
    ax1.set_ylim(0, 1200)
    fig.tight_layout()

    times, samples = zip(*ts_all)
    ax1.plot(times, samples, next(linecycler), label="All clients")

    times, samples = zip(*ts_ca)
    ax1.plot(times, samples, next(linecycler), label="California clients")

    times, samples = zip(*ts_hi)
    ax1.plot(times, samples, next(linecycler), label="Hawaii clients")

    plt.legend(loc="best", fontsize=14)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def main():
    logging.getLogger("matplotlib").setLevel(logging.ERROR)

    ts_all = read_ts('hawaii/all-minrtt-samples.ts', 0, 2)
    ts_ca = read_ts('hawaii/ca-minrtt-samples.ts', 0, 2)
    ts_hi = read_ts('hawaii/hi-minrtt-samples.ts', 0, 2)
    outfn = 'hawaii/nsamples.pdf'
    plot_numsamples(ts_all, ts_ca, ts_hi, outfn)

    ts_all = read_ts('hawaii/all-minrtt-samples.ts', 0, 1)
    ts_ca = read_ts('hawaii/ca-minrtt-samples.ts', 0, 1)
    ts_hi = read_ts('hawaii/hi-minrtt-samples.ts', 0, 1)
    outfn = 'hawaii/minrtt.pdf'
    plot_minrtt(ts_all, ts_ca, ts_hi, outfn)



if __name__ == "__main__":
    sys.exit(main())
