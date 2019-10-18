#!/usr/bin/env python3

import argparse
import logging
import pathlib
import resource
import sys

import fbperf
import matplotlib
matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
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

XLABEL = {"minrtt": "Median MinRTT Difference (ms) [Preferred $-$ Alternate]",
"hdrati": "HDratio Difference [Alternate $-$ Preferred]"}


def plot_summarizer_diff_ci(subdir, sumstr):
    logging.info("plotting %s", str(sumstr))
    xlabel = XLABEL[sumstr[0:6]]
    ylabel = "Cum. Fraction of Traffic"
    xlim = fbperf.Plots.get_xlim(sumstr)
    labels = fbperf.Plots.get_labels(sumstr)
    for weighted in ["true", "false"]:
        for table in ["true", "false"]:
            fname = f"main_diff_weighted_{weighted}_table_{table}.cdf"
            cdf = fbperf.Plots.readcdf(subdir / pathlib.Path(fname))
            fname = f"main_lb_weighted_{weighted}_table_{table}.cdf"
            lbcdf = fbperf.Plots.readcdf(subdir / pathlib.Path(fname))
            fname = f"main_ub_weighted_{weighted}_table_{table}.cdf"
            ubcdf = fbperf.Plots.readcdf(subdir / pathlib.Path(fname))
            fname = f"main_plot_weighted_{weighted}_table_{table}.pdf"
            outfile = subdir / fname
            fbperf.Plots.plot_diff_ci(
                cdf, lbcdf, ubcdf, xlabel, ylabel, xlim, labels, outfile
            )


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 32, 1 << 32))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.getLogger("matplotlib").setLevel(logging.ERROR)
    logging.basicConfig(format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    for tempdir in opts.basepath.glob("tempconfig*"):
        logging.info("entering %s", tempdir.name)
        for sumdir in tempdir.glob("*"):
            if not sumdir.is_dir():
                continue
            logging.info("entering %s", sumdir.name)
            plot_summarizer_diff_ci(sumdir, sumdir.name)


if __name__ == "__main__":
    sys.exit(main())
