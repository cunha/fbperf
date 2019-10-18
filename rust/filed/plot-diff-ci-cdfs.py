#!/usr/bin/env python3

import argparse
import bisect
import logging
import pathlib
import resource
import sys
from typing import List, Mapping, Tuple

import matplotlib.pyplot as plt

from fbperf import Summarizer


class Plot:
    METRIC_SUMMARY_XLABEL = {
        "minrtt50": {
            "opp": "Median MinRTT Difference [Preferred - Alternate]",
            "deg": "Median MinRTT Degradation [Current − Best]",
            "relationships": "Median MinRtt Difference",
        },
        "hdratio50": {
            "opp": "Median HDratio Difference [Alternate − Preferred]",
            "deg": "Median HDratio Degradation [Best − Current]",
            "relationships": "Median HDratio Difference",
        },
        "hdratioboot": {
            "opp": "Median HDratio Difference [Alternate − Preferred]",
            "deg": "Median HDratio Degradation [Best − Current]",
            "relationships": "Median HDratio Difference",
        },
    }
    METRIC_SUMMARY_XLIM = {
        "minrtt50": {"opp": (-10, 10), "deg": (0, 20), "relationships": (-10, 10)},
        "hdratio50": {
            "opp": (-0.2, 0.2),
            "deg": (0, 0.4),
            "relationships": (-0.2, 0.2),
        },
        "hdratioboot": {
            "opp": (-0.2, 0.2),
            "deg": (0, 0.4),
            "relationships": (-0.2, 0.2),
        },
    }
    METRIC_SUMMARY_LABELS = {
        "minrtt50": {
            "opp": [
                (
                    (METRIC_SUMMARY_XLIM["minrtt50"]["opp"][0] + 1, 0.25),
                    "left",
                    "Preferred\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["minrtt50"]["opp"][1] - 1, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
            "deg": [],
            "relationships": [],
        },
        "hdratio50": {
            "opp": [
                (
                    (METRIC_SUMMARY_XLIM["hdratio50"]["opp"][0] + 0.01, 0.25),
                    "left",
                    "Preferred\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["hdratio50"]["opp"][1] - 0.01, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
            "deg": [],
            "relationships": [],
        },
        "hdratioboot": {
            "opp": [
                (
                    (METRIC_SUMMARY_XLIM["hdratioboot"]["opp"][0] + 0.01, 0.25),
                    "left",
                    "Preferred\nis better",
                ),
                (
                    (METRIC_SUMMARY_XLIM["hdratioboot"]["opp"][1] - 0.01, 0.75),
                    "right",
                    "Alternate\nis better",
                ),
            ],
            "deg": [],
            "relationships": [],
        },
    }
    WEIGHTED_SHIFTED_YLABEL = {
        "true": {
            "true": "Cum. Fract. of Shifted Traffic",
            "false": "Cum. Fraction of Traffic",
        },
        "false": {"true": "Cum. Frac. of Shifted Bins", "false": "Cum. Frac. of Bins"},
    }

    @staticmethod
    def get_xlabel_xlim_labels(summarizer):
        m = summarizer.metric
        s = summarizer.summary
        logging.info("%s %s", m, s)
        return (
            Plot.METRIC_SUMMARY_XLABEL[m][s],
            Plot.METRIC_SUMMARY_XLIM[m][s],
            Plot.METRIC_SUMMARY_LABELS[m][s],
        )


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
    return parser


METRICS = ["minrtt50", "hdratio50"]


def plot_summarizer_diff_ci(subdir, summarizer):
    logging.info("plotting %s", str(summarizer))
    xlabel, xlim, labels = Plot.get_xlabel_xlim_labels(summarizer)
    for weighted in ["true", "false"]:
        for shifted in ["true", "false"]:
            fname = f"main_diff_weighted_{weighted}_table_{shifted}.cdf"
            cdf = readcdf(subdir / pathlib.Path(fname))
            fname = f"main_lb_weighted_{weighted}_table_{shifted}.cdf"
            lbcdf = readcdf(subdir / pathlib.Path(fname))
            fname = f"main_ub_weighted_{weighted}_table_{shifted}.cdf"
            ubcdf = readcdf(subdir / pathlib.Path(fname))
            ylabel = Plot.WEIGHTED_SHIFTED_YLABEL[weighted]["false"]
            fname = f"main_plot_weighted_{weighted}_table_{shifted}.pdf"
            outfile = subdir / fname
            plot_ci(cdf, lbcdf, ubcdf, xlabel, ylabel, xlim, labels, outfile)

            #     fname = f"ci_diff_{metric}_weighted_{weighted}_shifted_{shifted}.cdf"
            #     cdf = readcdf(subdir / pathlib.Path(fname))
            #     fname = f"ci_lb_{metric}_weighted_{weighted}_shifted_{shifted}.cdf"
            #     lbcdf = readcdf(subdir / pathlib.Path(fname))
            #     fname = f"ci_ub_{metric}_weighted_{weighted}_shifted_{shifted}.cdf"
            #     ubcdf = readcdf(subdir / pathlib.Path(fname))
            #     ylabel = Plot.WEIGHTED_SHIFTED_YLABEL[weighted][shifted]
            #     fname = f"plot_{metric}_weighted_{weighted}_shifted_{shifted}.pdf"
            #     outfile = subdir / fname
            #     plot_ci(cdf, lbcdf, ubcdf, xlabel, ylabel, xlim, labels, outfile)



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
    ax1.step(xs, ys, where="post")

    xslo, yslo = zip(*lower_bound_cdf)
    # ax1.step(xslo, yslo, where="post", linewidth=1, color="#333333")
    xsup, ysup = zip(*upper_bound_cdf)
    # ax1.step(xsup, ysup, where="post", linewidth=1, color="#333333")

    xslonorm = list()
    xsupnorm = list()
    joinys = list(sorted(yslo + ysup))
    for y in joinys:
        i = bisect.bisect_right(yslo, y)
        i = min(i, len(xslo) - 1)
        xslonorm.append(xslo[i])
        i = bisect.bisect_right(ysup, y)
        i = min(i, len(xsup) - 1)
        xsupnorm.append(xsup[i])
    ax1.fill_betweenx(joinys, xslonorm, xsupnorm, step="post", color="#333333", alpha=0.4, linewidth=0)

    # plt.legend(loc="best", fontsize=14)
    plt.grid()
    plt.savefig(outfile, bbox_inches="tight")
    plt.close(fig)


RELATIONSHIPS_METRIC_THRESH_HALFWIDTH = {
    "minrtt50": (5.0, 10.0),
    "hdratio50": (0.05, 0.20),
}


def plot_tempconfig_graphs(tempdir):
    for metric in METRICS:
        label2cdf = dict()
        for prialt, name in Summarizer.RELATIONSHIP_PAIR_NAMES.items():
            pri, alt = prialt
            thresh, diff_ci = RELATIONSHIPS_METRIC_THRESH_HALFWIDTH[metric]
            summarizer = Summarizer.make_key(
                metric, "relationships", thresh, diff_ci, pri, alt
            )
            fname = f"main_diff_weighted_true.cdf"
            fpath = tempdir / str(summarizer) / fname
            label2cdf[name] = readcdf(fpath)

        sum_default = Summarizer.make_key(
            metric, "relationships", thresh, diff_ci, 0, 0
        )
        xlabel, xlim, labels = Plot.get_xlabel_xlim_labels(sum_default)
        plot_multiline(
            label2cdf,
            xlabel,
            "Cum. Frac. of Traffic",
            xlim,
            labels,
            tempdir / f"plot--{metric}--relationships--weighted.pdf",
        )


def plot_multiline(
    label2cdf: Mapping[str, List[Tuple[float, float]]],
    xlabel: str,
    ylabel: str,
    xlim: Tuple[float, float],
    _labels: List[Tuple[Tuple, str]],
    outfile: pathlib.Path,
):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel(xlabel, fontsize=16)
    ax1.set_ylabel(ylabel, fontsize=16)
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


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 32, 1 << 32))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 35, 1 << 35))
    logging.getLogger("matplotlib").setLevel(logging.ERROR)
    logging.basicConfig(format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    for tempdir in opts.basepath.glob("tempconfig*"):
        for sumdir in tempdir.glob("*"):
            if not sumdir.is_dir():
                continue
            logging.info("loading %s", sumdir)
            logging.info("processing %s", sumdir.name)
            summarizer = Summarizer.parse(sumdir.name)
            plot_summarizer_diff_ci(sumdir, summarizer)
        # plot_tempconfig_graphs(tempdir)


if __name__ == "__main__":
    sys.exit(main())

    # plot_path_cdfs(
    #     tempdir,
    #     "minrtt50--deg",
    #     "Degradation > {}ms",
    #     "min-deg",
    #     opts.minrtt_thresh,
    # )
    # plot_path_cdfs(
    #     tempdir,
    #     "minrtt50--opp",
    #     "Improvement > {}ms",
    #     "min-improv",
    #     opts.minrtt_thresh,
    # )
    # plot_path_cdfs(
    #     tempdir, "hdratio--deg", "Degradation > {}", "min-deg", opts.hdratio_thresh
    # )
    # plot_path_cdfs(
    #     tempdir,
    #     "hdratio--opp",
    #     "Improvement > {}",
    #     "min-improv",
    #     opts.hdratio_thresh,
    # )

# def plot_path_cdfs(tempdir, config_prefix, legend_format, thresh_string, thresh):
#     re_extract_thresh = re.compile(f"--{thresh_string}-" + r"([.\d]+)")
#     config_suffix = f"--{thresh_string}-{thresh}"
#     for subdir in tempdir.glob(f"{config_prefix}*{config_suffix}"):
#         if not subdir.is_dir():
#             continue
#         assert subdir.name.endswith(config_suffix)
#         configstr = subdir.name[: -len(config_suffix)]
#         logging.info("processing %s", configstr)
#         shifts_per_day_label2cdf = dict()
#         frac_shifted_bins_label2cdf = dict()
#         for cfgdir in tempdir.glob(f"{configstr}--{thresh_string}-*"):
#             logging.info("processing %s", cfgdir)
#             cthresh = re_extract_thresh.search(cfgdir.name).group(1)
#             fpath = cfgdir / "average_shifts_per_day_paths.cdf"
#             label = legend_format.format(cthresh)
#             shifts_per_day_label2cdf[label] = readcdf(fpath)
#             fpath = cfgdir / "frac_shifted_bins_paths.cdf"
#             label = legend_format.format(cthresh)
#             frac_shifted_bins_label2cdf[label] = readcdf(fpath)
#         plot_multiline(
#             shifts_per_day_label2cdf,
#             "Average Shifts per Day",
#             "Cum. Frac. of Paths",
#             (0, 16),
#             tempdir / f"{configstr}.shifts-per-day.pdf",
#         )
#         plot_multiline(
#             frac_shifted_bins_label2cdf,
#             "Fraction of Time with Improvement",
#             "Cum. Frac. of Paths",
#             (0.0, 1.0),
#             tempdir / f"{configstr}.frac-shifted-bins.pdf",
#         )
