#!/usr/bin/env python3

import argparse
from collections import Counter
import logging
import pathlib
import resource
import sys

import fbperf


def create_parser():
    desc = """Dump TeX table"""
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
    parser.add_argument(
        "--hdratio-total-bytes",
        dest="hdratio_total_bytes",
        action="store",
        metavar="INT",
        type=int,
        required=False,
        default=6.35198e15,  # daiquery-5410
        help="Total valid bytes in dataset [%(default), from daiquery-5410]",
    )
    parser.add_argument(
        "--minrtt-total-bytes",
        dest="minrtt_total_bytes",
        action="store",
        metavar="INT",
        type=int,
        required=False,
        default=7.71296e15,  # daiquery-5410
        help="Total bytes in dataset [%(default), from daiquery-5410]",
    )
    return parser


minrtt_sumstr = (
    "minrtt50--opp--bound-true--diff-thresh-5.00--diff-ci-10.00--hdratio-diff-ci-0.10"
)
hdratio_sumstr = "hdratio50--opp--bound-true--diff-thresh-0.05--diff-ci-0.10"


HEADERS = [
    "\\begin{tabular}{l||r|rrr||r|rrr}",
    " & ".join(
        [
            "\\textsc{Pref.} $\\rightarrow$ \\textsc{Alt.}",
            "\\multicolumn{4}{c||}{\\minrttpct (\\cref{sec:aggregating-measurements})}",
            "\\multicolumn{4}{c||}{\\hdratiopct (\\cref{sec:aggregating-measurements})}",
        ]
    ),
    " & ".join(
        [
            "\\textsc{relationship}",
            "Absolute",
            "Relative",
            "Longer",
            "Prepended",
            "Absolute",
            "Relative",
            "Longer",
            "Prepended",
        ]
    ),
]

LINE_SPECS = {
    ("Private", "Private"): "bglblue",
    ("Private", "Transit"): "bgloran",
    ("Public", "Public"): "bglblue",
    ("Public", "Transit"): "bgloran",
    ("Transit", "Transit"): "bglblue",
    ("Others", "Others"): "bgwhite",
}

OTHERS = [("Public", "Private"), ("Private", "Public"), ("Transit", "Private")]


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 31, 1 << 31))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 31, 1 << 31))
    logging.basicConfig(format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    for tempdir in opts.basepath.glob("tempconfig*"):
        outfn = tempdir / "opp-vs-relationship.tex"
        outfd = open(outfn, "w")
        outfd.write(" \\\\\n".join(HEADERS))
        outfd.write(" \\\\\n\\hline\n")

        minrtt_data = fbperf.RelationshipData(tempdir / minrtt_sumstr)
        hdratio_data = fbperf.RelationshipData(tempdir / hdratio_sumstr)

        hkey = ("Others", "Others")
        minrttc = Counter(total=0, longer=0, prepended_more=0)
        hdratioc = Counter(total=0, longer=0, prepended_more=0)
        minrtt_data.prialt2counters[hkey] = minrttc
        hdratio_data.prialt2counters[hkey] = hdratioc
        for prialt in OTHERS:
            minrttc["total"] += minrtt_data.prialt2counters[prialt]["total"]
            minrttc["longer"] += minrtt_data.prialt2counters[prialt]["longer"]
            minrttc["prepended_more"] += minrtt_data.prialt2counters[prialt][
                "prepended_more"
            ]
            hdratioc3["total"] += hdratio_data.prialt2counters[prialt]["total"]
            hdratioc3["longer"] += hdratio_data.prialt2counters[prialt]["longer"]
            hdratioc["prepended_more"] += hdratio_data.prialt2counters[prialt]3[
                "prepended_more"
            ]

        for prialt, color in LINE_SPECS.items():
            outfd.write("\\rowcolor{%s}\n" % color)
            outfd.write("%s $\\rightarrow$ %s " % prialt)

            counters = minrtt_data.prialt2counters[prialt]
            absolute = counters["total"] / opts.minrtt_total_bytes
            absolute = f"{absolute:.4f}".lstrip("0")
            relative = counters["total"] / minrtt_data.total_bytes
            relative = f"{relative:.3f}".lstrip("0")
            longer = counters["longer"] / minrtt_data.total_bytes
            longer = f"{longer:.3f}".lstrip("0")
            prepended_more = counters["prepended_more"] / minrtt_data.total_bytes
            prepended_more = f"{prepended_more:.3f}".lstrip("0")
            outfd.write(f"& {absolute} & {relative} & {longer} & {prepended_more}")

            counters = hdratio_data.prialt2counters[prialt]
            absolute = counters["total"] / opts.hdratio_total_bytes
            absolute = f"{absolute:.4f}".lstrip("0")
            relative = counters["total"] / hdratio_data.total_bytes
            relative = f"{relative:.3f}".lstrip("0")
            longer = counters["longer"] / hdratio_data.total_bytes
            longer = f"{longer:.3f}".lstrip("0")
            prepended_more = counters["prepended_more"] / hdratio_data.total_bytes
            prepended_more = f"{prepended_more:.3f}".lstrip("0")
            outfd.write(f"& {absolute} & {relative} & {longer} & {prepended_more}")

            outfd.write("\\\\\n")


if __name__ == "__main__":
    sys.exit(main())
