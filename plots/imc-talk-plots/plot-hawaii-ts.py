#!/usr/bin/env python3

# from collections import OrderedDict
import datetime
from itertools import cycle
import logging
import os
import sys
import time

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# import matplotlib.ticker as ticker
import matplotlib
# matplotlib.rcParams['text.usetex'] = True

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

    colors = [
        "#006BA4",
        "#FF800E",
        "#ABABAB",
        "#595959",
        "#5F9ED1",
        "#C85200",
        "#898989",
        "#A2C8EC",
        "#FFBC79",
        "#CFCFCF",
    ]
    colorcycler = cycle(colors)

    lines = ["-", "--", "-.", ":"]
    linecycler = cycle(lines)

    fmt = mdates.DateFormatter('%Y-%m-%d\n%H:%M')
    fmt = mdates.DateFormatter("%b %d")

    fig, ax1 = plt.subplots(figsize=(12,4))
    # fig.autofmt_xdate()
    ax1.tick_params(axis="both", which="major", labelsize=18)
    ax1.xaxis_date()
    ax1.xaxis.set_major_formatter(fmt)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))

    xlim0 = min(ts_all)[0]
    xlim1 = max(ts_all)[0]
    print(xlim0)
    print(xlim1)

    ax1.set_xlim(xlim0, xlim1)

    ax1.set_ylabel("Median Minimum RTT [ms]", fontsize=18)
    ax1.set_ylim(0, 80)
    fig.tight_layout()

    times, samples = zip(*ts_hi)
    ax1.plot(times, samples, next(linecycler), label="Hawaii clients", lw=2)

    times, samples = zip(*ts_all)
    ax1.plot(times, samples, next(linecycler), label="All clients", lw=2)

    times, samples = zip(*ts_ca)
    ax1.plot(times, samples, next(linecycler), label="California clients", lw=2)

    plt.legend(ncol=3, loc="upper right", fontsize=18)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def plot_numsamples(ts_all, ts_ca, ts_hi, outfn):
    outdir = os.path.split(outfn)[0]
    os.makedirs(outdir, exist_ok=True)

    colors = [
        "#006BA4",
        # "#FF800E",
        "#ABABAB",
        "#595959",
        "#5F9ED1",
        "#C85200",
        "#898989",
        "#A2C8EC",
        "#FFBC79",
        "#CFCFCF",
    ]
    colorcycler = cycle(colors)

    lines = ["-", # "--",
     "-.", ":"]
    linecycler = cycle(lines)

    fmt = mdates.DateFormatter('%Y-%m-%d\n%H:%M')
    fmt = mdates.DateFormatter("%b %d")

    fig, ax1 = plt.subplots(figsize=(12,4))
    ax1.tick_params(axis="both", which="major", labelsize=18)
    ax1.xaxis_date()
    ax1.xaxis.set_major_formatter(fmt)
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    # fig.autofmt_xdate()

    xlim0 = min(ts_all)[0]
    xlim1 = max(ts_all)[0]
    # xlim0 = time.mktime(time.strptime("2019-09-10 18:00", "%Y-%m-%d %H:%M"))
    # xlim1 = time.mktime(time.strptime("2019-09-15 18:00", "%Y-%m-%d %H:%M"))
    print(xlim0)
    print(xlim1)

    ax1.set_xlim(xlim0, xlim1)

    ax1.set_ylabel("Number of Samples", fontsize=18)
    ax1.set_ylim(0, 600)
    fig.tight_layout()

    # times, samples = zip(*ts_all)
    # ax1.plot(times, samples, next(linecycler), label="All clients")

    times, samples = zip(*ts_hi)
    ax1.plot(times, samples, next(linecycler), label="Hawaii clients", lw=2)

    times, samples = zip(*ts_ca)
    ax1.plot(times, samples, next(linecycler), label="California clients", lw=2)

    # plt.legend(loc="best", fontsize=14)
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
