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


def plot_cdfs(label2cdf, outfn, lines, **kwargs):
    # outdir = os.path.split(outfn)[0]
    # os.makedirs(outdir, exist_ok=True)

    linecycler = cycle(lines)
    plt.style.use('seaborn-colorblind')

    if "figsize" in kwargs:
        fig, ax1 = plt.subplots(figsize=kwargs["figsize"])
    else:
        fig, ax1 = plt.subplots()
    ax1.tick_params(axis="both", which="major", labelsize=16)
    if "xlabel" in kwargs:
        ax1.set_xlabel(kwargs["xlabel"], fontsize=20)
    else:
        ax1.set_xlabel("Metric", fontsize=20)
    if "ylabel" in kwargs:
        ax1.set_ylabel(kwargs["ylabel"], fontsize=20)
    else:
        ax1.set_ylabel("CDF", fontsize=20)
    if "xlim" in kwargs:
        ax1.set_xlim(kwargs["xlim"][0], kwargs["xlim"][1])
    else:
        ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for label, cdf in label2cdf.items():
        xs, ys = zip(*cdf)
        ax1.step(xs, ys, next(linecycler), label=label, where="post")
    plt.legend(loc="best", fontsize=16)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def plot_rtt_hdr_all(rttcdf, hdrcdf, outfn):
    # outdir = os.path.split(outfn)[0]
    # os.makedirs(outdir, exist_ok=True)

    lines = ["-", "--", "-.", ":"]
    linecycler = cycle(lines)
    plt.style.use('seaborn-colorblind')

    fig, ax1 = plt.subplots()
    ax2 = ax1.twiny()

    ax2.tick_params(axis="both", which="major", labelsize=16)
    ax2.set_xlabel(METRIC2XLABEL["rtt"], fontsize=20)
    ax2.set_ylabel("Cum. Fraction of Sessions", fontsize=20)
    xlim = METRIC2XLIM["rtt"]
    ax2.set_xlim(xlim[0], xlim[1])
    ax2.set_ylim(0, 1)

    ax1.tick_params(axis="both", which="major", labelsize=16)
    ax1.set_xlabel(METRIC2XLABEL["hdr"], fontsize=20)
    ax1.set_ylabel("Cum. Fraction of Sessions", fontsize=20)
    xlim = METRIC2XLIM["hdr"]
    ax1.set_xlim(xlim[0], xlim[1])
    ax1.set_ylim(0, 1)

    fig.tight_layout()
    xs, ys = zip(*rttcdf)
    ax2.step(xs, ys, next(linecycler), label="MinRTT", where="post")

    xs, ys = zip(*hdrcdf)
    ax1.step(xs, ys, next(linecycler), label="HDratio", where="post")

    ax2.legend(framealpha=1, facecolor="white", loc="upper left", fontsize=16)
    ax1.legend(framealpha=1, facecolor="white", loc="lower right", fontsize=16)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


METRIC2XLABEL = {
        "rtt": "Minimum RTT [ms]",
        "hdr": "HDratio",
}
METRIC2XLIM = {
        "rtt": (0, 200),
        "hdr": (0, 1.0),
}


def main():
    logging.getLogger("matplotlib").setLevel(logging.ERROR)

    lines = list(["b-", "g--", "r-.", "k:", "y-", "m--"])

    metric = "rtt"
    label2cdf = OrderedDict()
    for continent in ["OC", "NA", "EU", "SA", "AS", "AF"]:
        fn = os.path.join(metric, '%s.cdf' % continent)
        label2cdf[continent] = read_cdf(fn)
    outfn = os.path.join(f"{metric}-per-continent.pdf")
    plot_cdfs(label2cdf, outfn, lines, xlabel=METRIC2XLABEL[metric],
            ylabel="Cumulative Fraction of Sessions",
            xlim=METRIC2XLIM[metric])

    metric = "hdr"
    label2cdf = OrderedDict()
    for continent in reversed(["OC", "NA", "EU", "SA", "AS", "AF"]):
        fn = os.path.join(metric, '%s.cdf' % continent)
        label2cdf[continent] = read_cdf(fn)
    outfn = os.path.join(f"{metric}-per-continent.pdf")
    plot_cdfs(label2cdf, outfn, reversed(lines), xlabel=METRIC2XLABEL[metric],
            ylabel="Cumulative Fraction of Sessions",
            xlim=METRIC2XLIM[metric])

    metric = "hdr"
    label2cdf = OrderedDict()
    for rng in reversed(["0-30", "31-50", "51-80", "81+"]):
        fn = os.path.join('%s.cdf' % rng)
        label2cdf[rng] = read_cdf(fn)
    outfn = os.path.join(f"rtt-hdr-correlation-wide.pdf")
    plot_cdfs(label2cdf, outfn, lines, xlabel=METRIC2XLABEL[metric],
            ylabel="Cum. Fraction of Sessions",
            xlim=METRIC2XLIM[metric],
            figsize=(7,3.5))

    label2cdf = OrderedDict()
    rttcdf = read_cdf("8a-minrtt.cdf")
    hdrcdf = read_cdf("8a-hdratio.cdf")
    outfn = "all.pdf"
    plot_rtt_hdr_all(rttcdf, hdrcdf, outfn)



if __name__ == "__main__":
    sys.exit(main())
