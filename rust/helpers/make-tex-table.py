#!/usr/bin/env python3

import argparse
import bisect
import logging
import pathlib
import re
import resource
import sys
from typing import List, Mapping, Tuple

import matplotlib.pyplot as plt


def readcdf(fpath):
    logging.info("reading cdf from %s", str(fpath))
    cdf = list()
    with open(fpath) as fd:
        for line in fd:
            x, y = line.split()
            cdf.append((float(x), float(y)))
    logging.info("cdf has %d points", len(cdf))
    return cdf


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
    parser.add_argument(
        "--minrtt-thresh",
        dest="minrtt_thresh",
        action="store",
        metavar="MS",
        type=int,
        required=False,
        default=0,
        help="One of the RTT thresholds in dumps (int) [%(default)s]",
    )
    parser.add_argument(
        "--hdratio-thresh",
        dest="hdratio_thresh",
        action="store",
        metavar="MS",
        type=str,
        required=False,
        default="0.00",
        help="One of the HD-ratio thresholds in dumps (exact string) [%(default)s]",
    )
    return parser


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 30, 1 << 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 30, 1 << 30))
    # logging.getLogger("matplotlib").setLevel(logging.ERROR)
    logging.basicConfig(filename="log.txt", format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    for tempdir in opts.basepath.glob("tempconfig*"):
        logging.info("entering %s", tempdir.name)
        for subdir in tempdir.glob("minrtt50--opp*"):
            if subdir.is_dir():
                # Second dash is a minus sign
                xlabel = "MinRTT Difference [Preferred - Alternate]"
                xlim = (-20, 20)
                labels = [
                    ((xlim[0] + 1, 0.25), "left", "Preferred\nis better"),
                    ((xlim[1] - 1, 0.75), "right", "Alternate\nis better"),
                ]
                logging.info("plotting diff CIs for minrtt50--opp")
                plot_ci_cdfs(subdir, xlabel, xlim, labels)
        for subdir in tempdir.glob("minrtt50--deg*"):
            if subdir.is_dir():
                xlabel = "MinRTT Degradation [Current − Best]"
                xlim = (0, 30)
                labels = []
                logging.info("plotting diff CIs for minrtt50--deg")
                plot_ci_cdfs(subdir, xlabel, xlim, labels)
        for subdir in tempdir.glob("hdratio--opp*"):
            if subdir.is_dir():
                xlabel = "HD-Ratio Difference [Alternate − Preferred]"
                xlim = (-0.20, 0.20)
                labels = [
                    ((xlim[0] + 0.01, 0.25), "left", "Preferred\nis better"),
                    ((xlim[1] - 0.01, 0.75), "right", "Alternate\nis better"),
                ]
                logging.info("plotting diff CIs for hdratio--opp")
                plot_ci_cdfs(subdir, xlabel, xlim, labels)
        for subdir in tempdir.glob("hdratio--deg*"):
            if subdir.is_dir():
                xlabel = "HD-Ratio Degradation [Best − Current]"
                xlim = (0, 0.3)
                labels = []
                logging.info("plotting diff CIs for hdratio-deg")
                plot_ci_cdfs(subdir, xlabel, xlim, labels)
        plot_path_cdfs(
            tempdir,
            "minrtt50--deg",
            "Degradation > {}ms",
            "min-deg",
            opts.minrtt_thresh,
        )
        plot_path_cdfs(
            tempdir,
            "minrtt50--opp",
            "Improvement > {}ms",
            "min-improv",
            opts.minrtt_thresh,
        )
        plot_path_cdfs(
            tempdir, "hdratio--deg", "Degradation > {}", "min-deg", opts.hdratio_thresh
        )
        plot_path_cdfs(
            tempdir,
            "hdratio--opp",
            "Improvement > {}",
            "min-improv",
            opts.hdratio_thresh,
        )


def plot_ci_cdfs(path, xlabel, xlim, labels):
    # Plot difference (bins)
    cdf = readcdf(path / pathlib.Path("diff_ci_bins.cdf"))
    lbcdf = readcdf(path / pathlib.Path("diff_ci_lb_bins.cdf"))
    ubcdf = readcdf(path / pathlib.Path("diff_ci_ub_bins.cdf"))
    ylabel = "Cum. Frac. of Time Bins"
    outfile = path / "diff_ci.pdf"
    plot_ci(cdf, lbcdf, ubcdf, xlabel, ylabel, xlim, labels, outfile)

    # Plot difference (traffic)
    cdf = readcdf(path / pathlib.Path("diff_ci_bins_weighted.cdf"))
    lbcdf = readcdf(path / pathlib.Path("diff_ci_lb_bins_weighted.cdf"))
    ubcdf = readcdf(path / pathlib.Path("diff_ci_ub_bins_weighted.cdf"))
    ylabel = "Cum. Frac. of Time Bins"
    outfile = path / "diff_ci_weighted.pdf"
    plot_ci(cdf, lbcdf, ubcdf, xlabel, ylabel, xlim, labels, outfile)


def plot_ci(
    diff_cdf: List[Tuple[float, float]],
    lower_bound_cdf: List[Tuple[float, float]],
    upper_bound_cdf: List[Tuple[float, float]],
    xlabel: str,
    ylabel: str,
    xlim: Tuple[float, float],
    labels: List[Tuple[Tuple[float, float], str, str]],
    outfile: pathlib.Path,
):
    logging.info("plotting %s", str(outfile))
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel(ylabel, fontsize=16)
    ax1.tick_params(axis="both", which="major", labelsize=14)
    ax1.set_xlim(xlim[0], xlim[1])
    ax1.set_ylim(0, 1)
    for pos, alignment, text in labels:
        ax1.annotate(
            text,
            xy=pos,
            fontsize=14,
            horizontalalignment=alignment,
            backgroundcolor="white",
        )
    fig.tight_layout()
    xs, ys = zip(*diff_cdf)
    ax1.plot(xs, ys)

    xslo, yslo = zip(*lower_bound_cdf)
    xsup, ysup = zip(*upper_bound_cdf)
    xslonorm = list()
    xsupnorm = list()
    for y in ys:
        i = bisect.bisect(yslo, y)
        i = min(i, len(xslo) - 1)
        xslonorm.append(xslo[i])
        i = bisect.bisect(ysup, y)
        i = min(i, len(xsup) - 1)
        xsupnorm.append(xsup[i])
    ax1.fill_betweenx(ys, xslonorm, xsupnorm, color="#333333", alpha=0.4, linewidth=0)

    plt.legend(loc="best", fontsize=14)
    plt.grid()
    plt.savefig(outfile, bbox_inches="tight")
    plt.close(fig)


def plot_path_cdfs(path, config_prefix, legend_format, thresh_string, thresh):
    re_extract_thresh = re.compile(f"--{thresh_string}-" + r"([.\d]+)")
    config_suffix = f"--{thresh_string}-{thresh}"
    for subdir in path.glob(f"{config_prefix}*{config_suffix}"):
        if not subdir.is_dir():
            continue
        assert subdir.name.endswith(config_suffix)
        configstr = subdir.name[: -len(config_suffix)]
        logging.info("processing %s", configstr)
        shifts_per_day_label2cdf = dict()
        frac_shifted_bins_label2cdf = dict()
        for cfgdir in path.glob(f"{configstr}--{thresh_string}-*"):
            logging.info("processing %s", cfgdir)
            cthresh = re_extract_thresh.search(cfgdir.name).group(1)
            fpath = cfgdir / "average_shifts_per_day_paths.cdf"
            label = legend_format.format(cthresh)
            shifts_per_day_label2cdf[label] = readcdf(fpath)
            fpath = cfgdir / "frac_shifted_bins_paths.cdf"
            label = legend_format.format(cthresh)
            frac_shifted_bins_label2cdf[label] = readcdf(fpath)
        plot_multiline(
            shifts_per_day_label2cdf,
            "Average Shifts per Day",
            (0, 16),
            path / f"{configstr}.shifts-per-day.pdf",
        )
        plot_multiline(
            frac_shifted_bins_label2cdf,
            "Fraction of Time with Improvement",
            (0.0, 1.0),
            path / f"{configstr}.frac-shifted-bins.pdf",
        )


def plot_multiline(
    label2cdf: Mapping[str, List[Tuple[float, float]]],
    xlabel: str,
    xlim: Tuple[float, float],
    outfile: pathlib.Path,
):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel("Cum. Frac. of Paths", fontsize=16)
    ax1.tick_params(axis="both", which="major", labelsize=14)
    ax1.set_xlim(xlim[0], xlim[1])
    ax1.set_ylim(0, 1)
    fig.tight_layout()
    for label, cdf in label2cdf.items():
        xs, ys = zip(*cdf)
        ax1.plot(xs, ys, label=label)
    plt.legend(loc="best", fontsize=14)
    plt.grid()
    plt.savefig(outfile, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    sys.exit(main())
