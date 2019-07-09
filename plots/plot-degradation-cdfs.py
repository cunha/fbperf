#!/usr/bin/env python3

from collections import OrderedDict
from itertools import cycle
import logging
import os
import sys

import matplotlib.pyplot as plt


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
    plt.style.use('seaborn-colorblind')

    fig, ax1 = plt.subplots(figsize=(8,4))
    ax1.tick_params(axis="both", which="major", labelsize=14)
    if "xlabel" in kwargs:
        ax1.set_xlabel(kwargs["xlabel"], fontsize=16)
    else:
        ax1.set_xlabel("Metric", fontsize=16)
    if "ylabel" in kwargs:
        ax1.set_ylabel(kwargs["ylabel"], fontsize=16)
    else:
        ax1.set_ylabel("CDF", fontsize=16)
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


def main():
    logging.getLogger("matplotlib").setLevel(logging.ERROR)

    fn = os.path.join("degradation/minrtt-traffic.cdf")
    label2cdf = OrderedDict()
    label2cdf["MinRTT − Best MinRTT"] = read_cdf(fn)
    outfn = "degradation/minrtt-traffic.pdf"
    plot_cdfs(label2cdf, outfn, xlabel="MinRTT Degradation [ms]",
            ylabel="Cumulative Fraction of Traffic",
            xlim=(0, 30))

    fn = os.path.join("degradation/hdratio-traffic.cdf")
    label2cdf = OrderedDict()
    label2cdf["Best HD-Ratio − HD-Ratio"] = read_cdf(fn)
    outfn = "degradation/hdratio-traffic.pdf"
    plot_cdfs(label2cdf, outfn, xlabel="HD-Ratio Degradation",
            ylabel="Cumulative Fraction of Traffic",
            xlim=(0, 0.3))


if __name__ == "__main__":
    sys.exit(main())
