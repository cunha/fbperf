#!/usr/bin/env python3

from collections import OrderedDict
from itertools import cycle
import logging
import os
import sys

import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['text.usetex'] = True

def read_cdf(fpath):
    cdf = list()
    with open(fpath) as fd:
        for line in fd:
            x, y = line.split()
            cdf.append((float(x), float(y)))
    return cdf


def plot_cdfs(label2cdf, outfn, **kwargs):
    outdir = os.path.split(outfn)[0]
    os.makedirs(outdir, exist_ok=True)

    lines = ["-", "--", "-.", ":"]
    linecycler = cycle(lines)
    # plt.style.use('seaborn-colorblind')

    fig, ax1 = plt.subplots(figsize=(8,3))
    ax1.tick_params(axis="both", which="major", labelsize=14)
    ax1.set_xscale("log")
    if "xlabel" in kwargs:
        ax1.set_xlabel(kwargs["xlabel"], fontsize=18)
    else:
        ax1.set_xlabel("Metric", fontsize=18)
    if "ylabel" in kwargs:
        ax1.set_ylabel(kwargs["ylabel"], fontsize=18)
    else:
        ax1.set_ylabel("CDF", fontsize=18)
    if "xlim" in kwargs:
        ax1.set_xlim(kwargs["xlim"][0], kwargs["xlim"][1])
    else:
        ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for label, cdf in label2cdf.items():
        xs, ys = zip(*cdf)
        if "xdiv" in kwargs:
            xs = list(x/kwargs["xdiv"] for x in xs)
        ax1.plot(xs, ys, next(linecycler), label=label)
    plt.legend(loc="best", fontsize=14)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


PROTO2XLABEL = {
        "http1": "HTTP/1.1",
        "http2": "HTTP/2",
        "all": "All Requests",
}


def main():
    logging.getLogger("matplotlib").setLevel(logging.ERROR)

    base = 'conn-sz-transactions'
    metric = 'bytes'
    label2cdf = OrderedDict()
    for proto in ["http1", "all", "http2"]:
        fn = os.path.join(base, "%s-%s.cdf" % (metric, proto))
        label2cdf[PROTO2XLABEL[proto]] = read_cdf(fn)
    outfn = "%s/%s.pdf" % (base, metric)
    plot_cdfs(label2cdf, outfn, xlabel="Number of Transactions in Session",
            ylabel="Cum. Frac. of Transferred Bytes",
            xlim=(1, 1000))

    base = 'conn-sz-transactions'
    metric = 'transactions'
    label2cdf = OrderedDict()
    for proto in ["http1", "all", "http2"]:
        fn = os.path.join(base, "%s-%s.cdf" % (metric, proto))
        label2cdf[PROTO2XLABEL[proto]] = read_cdf(fn)
    outfn = "%s/%s.pdf" % (base, metric)
    plot_cdfs(label2cdf, outfn, xlabel="Number of Transactions in Session",
            ylabel="Cum. Frac. of Sessions",
            xlim=(1, 1000))


if __name__ == "__main__":
    sys.exit(main())
