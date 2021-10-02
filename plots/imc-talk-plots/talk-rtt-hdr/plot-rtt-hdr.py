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
    shaded_labels = kwargs.get("shaded_labels", [])

    outdir = os.path.split(outfn)[0]
    os.makedirs(outdir, exist_ok=True)

    lines = ["g-", "b--", "m-.", "y:", "k-", "r--"]
    linecycler = cycle(lines)
    plt.style.use('seaborn-colorblind')

    fig, ax1 = plt.subplots()
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
        if label in shaded_labels:
            ax1.plot(xs, ys, next(linecycler), alpha=0.2)
        else:
            ax1.plot(xs, ys, next(linecycler), label=label)
    plt.legend(loc="best", fontsize=14)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def plot_rtt_hdr_all(rttcdf, hdrcdf, outfn):
    outdir = os.path.split(outfn)[0]
    os.makedirs(outdir, exist_ok=True)

    lines = ["-", "--", "-.", ":"]
    linecycler = cycle(lines)
    plt.style.use('seaborn-colorblind')

    fig, ax1 = plt.subplots()
    ax2 = ax1.twiny()

    ax2.tick_params(axis="both", which="major", labelsize=14)
    ax2.set_xlabel(METRIC2XLABEL["rtt"], fontsize=16)
    ax2.set_ylabel("Cumulative Fraction of Traffic", fontsize=16)
    xlim = METRIC2XLIM["rtt"]
    ax2.set_xlim(xlim[0], xlim[1])
    ax2.set_ylim(0, 1)

    ax1.tick_params(axis="both", which="major", labelsize=14)
    ax1.set_xlabel(METRIC2XLABEL["hdr"], fontsize=16)
    ax1.set_ylabel("Cumulative Fraction of Traffic", fontsize=16)
    xlim = METRIC2XLIM["hdr"]
    ax1.set_xlim(xlim[0], xlim[1])
    ax1.set_ylim(0, 1)

    fig.tight_layout()
    xs, ys = zip(*rttcdf)
    ax2.plot(xs, ys, next(linecycler), label="MinRTT")

    xs, ys = zip(*hdrcdf)
    ax1.plot(xs, ys, next(linecycler), label="HD-Ratio")

    ax2.legend(loc="upper left", fontsize=14)
    ax1.legend(loc="lower right", fontsize=14)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


METRIC2XLABEL = {
        "rtt": "Median Minimum RTT [ms]",
        "hdr": "HD-Ratio",
}
METRIC2XLIM = {
        "rtt": (0, 100),
        "hdr": (0.4, 1.0),
}

SHADED_SPECS = [
    ["OC", "EU", "SA", "AS", "AF"],
    ["OC", "NA", "EU", "SA", "AS"],
    ["SA", "AS", "AF"],
    ["OC", "NA", "EU"],
    [],
]


def main():
    logging.getLogger("matplotlib").setLevel(logging.ERROR)

    for shaded_labels in SHADED_SPECS:
        suffix = "-".join(shaded_labels)
        metric = "rtt"
        label2cdf = OrderedDict()
        for continent in ["OC", "NA", "EU", "SA", "AS", "AF"]:
            fn = os.path.join(metric, '%s.cdf' % continent)
            label2cdf[continent] = read_cdf(fn)
        outfn = os.path.join(metric, f"per-continent-{suffix}.pdf")
        plot_cdfs(label2cdf, outfn, xlabel=METRIC2XLABEL[metric],
                ylabel="Cumulative Fraction of Traffic",
                xlim=METRIC2XLIM[metric],
                shaded_labels=shaded_labels)
        metric = "hdr"
        label2cdf = OrderedDict()
        for continent in reversed(["OC", "NA", "EU", "SA", "AS", "AF"]):
            fn = os.path.join(metric, '%s.cdf' % continent)
            label2cdf[continent] = read_cdf(fn)
        outfn = os.path.join(metric, f"per-continent-{suffix}.pdf")
        plot_cdfs(label2cdf, outfn, xlabel=METRIC2XLABEL[metric],
                ylabel="Cumulative Fraction of Traffic",
                xlim=METRIC2XLIM[metric],
                shaded_labels=shaded_labels)

    label2cdf = OrderedDict()
    rttcdf = read_cdf("rtt/all.cdf")
    hdrcdf = read_cdf("hdr/all.cdf")
    outfn = "rtt/all.pdf"
    plot_rtt_hdr_all(rttcdf, hdrcdf, outfn)



if __name__ == "__main__":
    sys.exit(main())
