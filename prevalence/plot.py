#!/usr/bin/env python3

from collections import OrderedDict
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


def plot_cdfs(label2cdf, xlabel, outfn, **kwargs):
    outdir = os.path.split(outfn)[0]
    os.makedirs(outdir, exist_ok=True)
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.tick_params(axis="both", which="major", labelsize=14)
    if "ylabel" in kwargs:
        ax1.set_ylabel(kwargs["ylabel"], fontsize=16)
    else:
        ax1.set_ylabel("Cumulative Fraction Prefixes", fontsize=16)
    if "xlim" in kwargs:
        ax1.set_xlim(kwargs["xlim"][0], kwargs["xlim"][1])
    else:
        ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for label, cdf in label2cdf.items():
        xs, ys = zip(*cdf)
        ax1.plot(xs, ys, label=label)
    plt.legend(loc="best", fontsize=14)
    plt.grid()
    plt.savefig(outfn, bbox_inches="tight")
    plt.close(fig)


def get_label2cdf_minrtt(base, filtr, fn):
    label2cdf = OrderedDict()
    fp = "output/%s/minrtt_ci_lower_bound_10/%s/%s" % (base, filtr, fn)
    cdf = read_cdf(fp)
    label2cdf["Diff > 10ms"] = cdf
    fp = "output/%s/minrtt_ci_lower_bound_5/%s/%s" % (base, filtr, fn)
    cdf = read_cdf(fp)
    label2cdf["Diff > 5ms"] = cdf
    fp = "output/%s/minrtt_ci_lower_bound_0/%s/%s" % (base, filtr, fn)
    cdf = read_cdf(fp)
    label2cdf["Diff > 0ms"] = cdf
    return label2cdf


def get_label2cdf_hdratio(base, filtr, fn):
    label2cdf = OrderedDict()
    fp = "output/%s/hdratio_ci_lower_bound_0.10/%s/%s" % (base, filtr, fn)
    cdf = read_cdf(fp)
    label2cdf["Diff > 0.1"] = cdf
    fp = "output/%s/hdratio_ci_lower_bound_0.05/%s/%s" % (base, filtr, fn)
    cdf = read_cdf(fp)
    label2cdf["Diff > 0.05"] = cdf
    fp = "output/%s/hdratio_ci_lower_bound_0.00/%s/%s" % (base, filtr, fn)
    cdf = read_cdf(fp)
    label2cdf["Diff > 0.0"] = cdf
    return label2cdf


def main():
    # logging.getLogger("matplotlib").setLevel(logging.ERROR)

    for base in ["nonsticky", "sticky"]:
        for filtr in ["min_number_of_bins", "significant_improv_min_bins"]:
            os.makedirs(os.path.join("output", base), exist_ok=True)

            fn = "frac-improved-bins.cdf"
            label2cdf = get_label2cdf_minrtt(base, filtr, fn)
            outfn = os.path.join("output", base, "minrtt", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(label2cdf, "Fraction of Time with Improvement", outfn)
            label2cdf = get_label2cdf_hdratio(base, filtr, fn)
            outfn = os.path.join("output", base, "hdratio", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(label2cdf, "Fraction of Time with Improvement", outfn)

            fn = "frac-improved-bins-weighted.cdf"
            label2cdf = get_label2cdf_minrtt(base, filtr, fn)
            outfn = os.path.join("output", base, "minrtt", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(
                label2cdf,
                "Fraction of Time with Improvement",
                outfn,
                ylabel="Cumulative Fraction of Traffic",
            )
            label2cdf = get_label2cdf_hdratio(base, filtr, fn)
            outfn = os.path.join("output", base, "hdratio", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(
                label2cdf,
                "Fraction of Time with Improvement",
                outfn,
                ylabel="Cumulative Fraction of Traffic",
            )

            fn = "frac-improved-bytes.cdf"
            label2cdf = get_label2cdf_minrtt(base, filtr, fn)
            outfn = os.path.join("output", base, "minrtt", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(label2cdf, "Fraction of Traffic Shifted", outfn)
            label2cdf = get_label2cdf_hdratio(base, filtr, fn)
            outfn = os.path.join("output", base, "hdratio", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(label2cdf, "Fraction of Traffic Shifted", outfn)

            fn = "num-shifts-per-day.cdf"
            label2cdf = get_label2cdf_minrtt(base, filtr, fn)
            outfn = os.path.join("output", base, "minrtt", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(label2cdf, "Number of Shifts/Day", outfn, xlim=(0, 15))
            label2cdf = get_label2cdf_hdratio(base, filtr, fn)
            outfn = os.path.join("output", base, "hdratio", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(label2cdf, "Number of Shifts/Day", outfn, xlim=(0, 15))

            fn = "num-shifts-per-day-weighted.cdf"
            label2cdf = get_label2cdf_minrtt(base, filtr, fn)
            outfn = os.path.join("output", base, "minrtt", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(
                label2cdf,
                "Number of Shifts/Day",
                outfn,
                xlim=(0, 15),
                ylabel="Cumulative Fraction of Traffic",
            )
            label2cdf = get_label2cdf_hdratio(base, filtr, fn)
            outfn = os.path.join("output", base, "hdratio", filtr, fn)
            outfn = outfn.replace(".cdf", ".pdf")
            plot_cdfs(
                label2cdf,
                "Number of Shifts/Day",
                outfn,
                xlim=(0, 15),
                ylabel="Cumulative Fraction of Traffic",
            )

            # fn = "prev-per-prefix.cdf"
            # label2cdf = get_label2cdf_minrtt(base, filtr, fn)
            # outfn = os.path.join('output', base, 'minrtt', filtr, fn)
            # outfn = outfn.replace(".cdf", ".pdf")
            # plot_cdfs(label2cdf, "Prevalence of Primary Path", outfn)
            # label2cdf = get_label2cdf_hdratio(base, filtr, fn)
            # outfn = os.path.join('output', base, 'hdratio', filtr, fn)
            # outfn = outfn.replace(".cdf", ".pdf")
            # plot_cdfs(label2cdf, "Prevalence of Primary Path", outfn)

            # fn = "prev-per-prefix-weighted-traffic.cdf"
            # label2cdf = get_label2cdf_minrtt(base, filtr, fn)
            # outfn = os.path.join('output', base, 'minrtt', filtr, fn)
            # outfn = outfn.replace(".cdf", ".pdf")
            # plot_cdfs(label2cdf, "Prevalence of Primary Path", outfn, ylabel="Cumulative Fraction of Traffic")
            # label2cdf = get_label2cdf_hdratio(base, filtr, fn)
            # outfn = os.path.join('output', base, 'hdratio', filtr, fn)
            # outfn = outfn.replace(".cdf", ".pdf")
            # plot_cdfs(label2cdf, "Prevalence of Primary Path", outfn, ylabel="Cumulative Fraction of Traffic")


if __name__ == "__main__":
    sys.exit(main())
