#!/usr/bin/env python3

import logging
import sys
import bisect

import matplotlib.pyplot as plt


def read_cdf(fpath):
    cdf = list()
    with open(fpath) as fd:
        for line in fd:
            x, y = line.split()
            cdf.append((float(x), float(y)))
    return cdf


def plot_cdfs(label2cdf, outfn):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel("MinRTT Difference [ms]", fontsize=16)
    ax1.set_ylabel("Cumulative Fraction Traffic", fontsize=16)
    ax1.tick_params(axis="both", which="major", labelsize=14)
    ax1.set_xlim(-20, +20)
    ax1.set_ylim(0, 1)
    ax1.annotate("Alternate\nis better", xy=(19, 0.85), fontsize=14,
            horizontalalignment="right", backgroundcolor="white")
    ax1.annotate("Primary\nis better", xy=(-19, 0.25), fontsize=14,
            horizontalalignment="left", backgroundcolor="white")
    fig.tight_layout()
    for label, cdfs in label2cdf.items():
        xs, ys = zip(*cdfs[0])
        ax1.step(xs, ys, label=label, where="post")
        xslo, yslo = zip(*cdfs[1])
        xsup, ysup = zip(*cdfs[2])
        xsupnorm = list()
        for y in yslo:
            i = bisect.bisect_right(ysup, y)
            i = min(i, len(xsup) - 1)
            xsupnorm.append(xsup[i])
        ax1.fill_betweenx(
            yslo, xslo, xsupnorm, color="#333333", alpha=0.4, linewidth=0
        )
    plt.legend(loc="best", fontsize=14)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def main():
    logging.getLogger("matplotlib").setLevel(logging.ERROR)
    median_diff_cdf = read_cdf(sys.argv[1])
    median_ci_lb = read_cdf(sys.argv[2])
    median_ci_ub = read_cdf(sys.argv[3])
    label2cdf = dict()
    label2cdf["Preferred − Best Alternate"] = (
        median_diff_cdf,
        median_ci_lb,
        median_ci_ub,
    )
    plot_cdfs(label2cdf, sys.argv[4])


if __name__ == "__main__":
    sys.exit(main())
