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
    ax1.set_xlabel("HD-Ratio Difference", fontsize=16)
    ax1.set_ylabel("Cumulative Fraction Traffic", fontsize=16)
    ax1.set_xlim(-0.2, +0.2)
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for label, cdfs in label2cdf.items():
        xs, ys = zip(*cdfs[0])
        ax1.plot(xs, ys, label=label)
        xslo, yslo = zip(*cdfs[1])
        xsup, ysup = zip(*cdfs[2])
        xslonorm = list()
        xsupnorm = list()
        for y in ys:
            i = bisect.bisect(yslo, y)
            i = min(i, len(xslo)-1)
            xslonorm.append(xslo[i])
            i = bisect.bisect(ysup, y)
            i = min(i, len(xsup)-1)
            xsupnorm.append(xsup[i])
        ax1.fill_betweenx(
            ys, xslonorm, xsupnorm, color="#333333", alpha=0.4, linewidth=0
        )
    plt.legend(loc="best")
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def main():
    logging.getLogger("matplotlib").setLevel(logging.ERROR)
    median_diff_cdf = read_cdf(sys.argv[1])
    median_ci_lb = read_cdf(sys.argv[2])
    median_ci_ub = read_cdf(sys.argv[3])
    label2cdf = dict()
    label2cdf["Primary - Best Alternate"] = (
        median_diff_cdf,
        median_ci_lb,
        median_ci_ub,
    )
    plot_cdfs(label2cdf, sys.argv[4])


if __name__ == "__main__":
    sys.exit(main())
