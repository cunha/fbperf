#!/usr/bin/env python3

import logging
import sys

import matplotlib.pyplot as plt


def read_cdf(fpath):
    cdf = list()
    with open(fpath) as fd:
        for line in fd:
            x, y = line.split()
            cdf.append((float(x), float(y)))
    return cdf


def plot_cdfs(label2cdf, xlabel, outfn):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel("Cumulative Fraction Prefixes", fontsize=16)
    ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for label, cdf in label2cdf.items():
        xs, ys = zip(*cdf)
        ax1.plot(xs, ys, label=label)
    plt.legend(loc="best")
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def main():
    # logging.getLogger("matplotlib").setLevel(logging.ERROR)

    label2cdf = dict()

    fn = "output/frac-improved-bins.cdf"
    cdf = read_cdf(fn)
    label2cdf["x"] = cdf
    plot_cdfs(label2cdf, "Fraction of Bins with Improvement", fn.replace(".cdf", ".pdf"))

    fn = "output/frac-improved-bytes.cdf"
    cdf = read_cdf(fn)
    label2cdf["x"] = cdf
    plot_cdfs(label2cdf, "Fraction of Bytes with Improvement", fn.replace(".cdf", ".pdf"))

    fn = "output/frac-improved-bins-in-longest-streak.cdf"
    cdf = read_cdf(fn)
    label2cdf["x"] = cdf
    plot_cdfs(label2cdf, "Fraction of Improved Bins in\nLongest Streak", fn.replace(".cdf", ".pdf"))

    fn = "output/frac-improved-bins-in-longest-streak-significant.cdf"
    cdf = read_cdf(fn)
    label2cdf["x"] = cdf
    plot_cdfs(label2cdf, "Fraction of Improved Bins in\nLongest Streak [significant]", fn.replace(".cdf", ".pdf"))

    fn = "output/ratio-shifts-to-nbins.cdf"
    cdf = read_cdf(fn)
    label2cdf["x"] = cdf
    plot_cdfs(label2cdf, "Ratio of Shifts to Number of Bins", fn.replace(".cdf", ".pdf"))

    fn = "output/ratio-shifts-to-nbins-significant.cdf"
    cdf = read_cdf(fn)
    label2cdf["x"] = cdf
    plot_cdfs(label2cdf, "Ratio of Shifts to Number of Bins [significant]", fn.replace(".cdf", ".pdf"))


if __name__ == "__main__":
    sys.exit(main())
