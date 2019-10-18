#!/usr/bin/env python3

import argparse
import logging
import pathlib
import resource
import sys

import fbperf
import matplotlib
matplotlib.rcParams['text.usetex'] = True

def create_parser():
    desc = """Plot CDF of performance differences including CIs"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--basepath",
        dest="basepath",
        action="store",
        metavar="PATH",
        type=pathlib.Path,
        required=True,
        help="Directory containing data (as exported by the Rust code)",
    )
    return parser


METRICS = ["minrtt50", "hdratio50"]

SPECS = {
    (7, 8): "Peering vs Transit",
    (8, 8): "Transit vs Transit",
    (5, 2): "Private vs Public",
}

METRIC_TO_SUMSTR_TAIL = {
    "hdratio50": "bound-true--diff-thresh-0.05--diff-ci-0.20",
    "minrtt50": "bound-true--diff-thresh-5.00--diff-ci-10.00",
}


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 32, 1 << 32))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.getLogger("matplotlib").setLevel(logging.ERROR)
    logging.basicConfig(format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    for tempdir in opts.basepath.glob("tempconfig*"):
        for metric in METRICS:
            tail = METRIC_TO_SUMSTR_TAIL[metric]
            name2cdf = dict()
            for prialt, name in SPECS.items():
                pri, alt = prialt
                sumstr = f"{metric}--relationships-{pri}-{alt}--{tail}"
                fpath = tempdir / sumstr / "main_diff_weighted_true_table_true.cdf"
                name2cdf[name] = fbperf.Plots.readcdf(fpath)

            xlabel = fbperf.Plots.METRIC_SUMMARY_XLABEL[metric]["relationships"]
            ylabel = "Cum. Fraction of Traffic"
            xlim = fbperf.Plots.METRIC_SUMMARY_XLIM[metric]["relationships"]
            labels = fbperf.Plots.METRIC_SUMMARY_LABELS[metric]["relationships"]
            outfn = tempdir / f"{metric}--relationships-{pri}-{alt}.pdf"
            fbperf.Plots.plot_multiline(name2cdf, xlabel, ylabel, xlim, labels, outfn)


if __name__ == "__main__":
    sys.exit(main())
