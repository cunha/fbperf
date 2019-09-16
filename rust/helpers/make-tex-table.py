#!/usr/bin/env python3

import argparse
from collections import defaultdict
import logging
import pathlib
import pickle
import re
import resource
import sys


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


METRIC_NAME = {"minrtt50": "\\minrtt", "hdratio": "\\hdratio"}
SUMMARIZER_NAME = {"deg": "\\textsc{Degradation}", "opp": "\\textsc{Opportunity}"}

TEMPORAL_BEHAVIORS = [
    "MissingBins",
    "NoRoute",
    "Undersampled",
    "Uneventful",
    "Continuous",
    "Diurnal",
    "Episodic",
]

TEMPORAL_IGNORE_SHIFTS = ["MissingBins", "NoRoute", "Undersampled", "Uneventful"]

CLIENT_CONTINENTS = ["AF", "AS", "EU", "NA", "OC", "SA"]

SUMMARIZER_METRIC_THRESH = {
    "opp": {"minrtt50": [5.0], "hdratio": [0.02]},
    "deg": {"minrtt50": [5.0, 10.0, 20.0], "hdratio": [0.02, 0.05, 0.10]},
}

TEMPORAL_VALID = ["Uneventful", "Continuous", "Diurnal", "Episodic"]
TEMPORAL_HAS_SHIFTS = ["Continuous", "Diurnal", "Episodic"]


def make_thresh2cont2valid(thresh2name2tuple):
    thresh2cont2valid = defaultdict(dict)
    for t, name2tuple in thresh2name2tuple.items():
        for cont in CLIENT_CONTINENTS:
            getname = lambda temp: f"{temp}+{cont}"
            print(name2tuple)
            print(getname("Uneventful"))
            print(name2tuple[getname("Uneventful")])
            valid = sum(name2tuple[getname(temp)][1] for temp in TEMPORAL_VALID)
            thresh2cont2valid[t][cont] = valid
    return thresh2cont2valid


def make_thresh2valid(thresh2name2tuple):
    thresh2valid = dict()
    for t, name2tuple in thresh2name2tuple.items():
        valid = sum(name2tuple[temp][1] for temp in TEMPORAL_VALID)
        thresh2valid[t] = valid
    return thresh2valid


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 30, 1 << 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 30, 1 << 30))
    # logging.getLogger("matplotlib").setLevel(logging.ERROR)
    logging.basicConfig(filename="log.txt", format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    metric2thresh = {"minrtt50": opts.minrtt_thresh, "hdratio": opts.hdratio_thresh}
    summarizer2threshstr = {"deg": "min-deg", "opp": "min-improv"}

    w = sys.stdout.write
    for tempdir in opts.basepath.glob("tempconfig*"):
        sum2metric2thresh2name2tuple = defaultdict(dict)
        sum2metric2thresh2cont2valid = defaultdict(dict)
        sum2metric2thresh2valid = defaultdict(dict)
        w(f"{tempdir.name}\n")
        for m in ["minrtt50", "hdratio"]:
            for s in ["deg", "opp"]:
                thresh_string = summarizer2threshstr[s]
                thresh = metric2thresh[m]
                sum2metric2thresh2name2tuple[s][m] = load_thresh2name2tuple(
                    tempdir, m, s, thresh_string, thresh
                )
                sum2metric2thresh2cont2valid[s][m] = make_thresh2cont2valid(
                    sum2metric2thresh2name2tuple[s][m]
                )
                sum2metric2thresh2valid[s][m] = make_thresh2valid(
                    sum2metric2thresh2name2tuple[s][m]
                )

        build_header(sum2metric2thresh2name2tuple, w)
        for temp in TEMPORAL_BEHAVIORS:
            templine = build_temp_line(
                sum2metric2thresh2name2tuple, sum2metric2thresh2valid, temp
            )
            if temp not in TEMPORAL_VALID:
                w(f"% {temp: <12} & {templine} \\\\\n")
            else:
                w(f"\\hline\n")
                w(f"\\rowcolor{{bggray}}\n")
                w(f"{temp: <12} & {templine} \\\\\n")
                for cont in CLIENT_CONTINENTS:
                    line = build_cont_line(
                        sum2metric2thresh2name2tuple,
                        sum2metric2thresh2cont2valid,
                        temp,
                        cont,
                    )
                    name = f"\\qquad {cont}"
                    w(f"{name: <12} & {line} \\\\\n")


METRIC_FMT_THRESH = {"minrtt50": lambda x: f"{int(x)}ms", "hdratio": lambda x: x}


def build_header(sum2metric2thresh2name2tuple, w):
    summarizers = list()
    metrics = list()
    thresholds = list()
    colstr = ""
    cols = 0
    for sumr, metric2thresh2name2tuple in sum2metric2thresh2name2tuple.items():
        scols = 0
        colstr += "|"
        for metric, thresh2name2tuple in metric2thresh2name2tuple.items():
            mcols = 2 * len(thresh2name2tuple)
            scols += mcols
            string = "\\multicolumn{%d}{c|}{%s}" % (mcols, METRIC_NAME[metric])
            metrics.append(string)
            for thresh, _name2tuple in sorted(thresh2name2tuple.items()):
                string = "\\multicolumn{2}{c|}{%s}" % METRIC_FMT_THRESH[metric](thresh)
                thresholds.append(string)
                colstr += "|rr"
        string = "\\multicolumn{%d}{c||}{%s}" % (scols, SUMMARIZER_NAME[sumr])
        summarizers.append(string)
        cols += scols
    w("\\begin{tabular}{r%s}\n" % colstr)
    w("& %s \\\\\n" % " & ".join(summarizers))
    w("\\multicolumn{1}{l||}{\\textsc{Class$/$}} & %s \\\\\n" % " & ".join(metrics))
    w("\\textsc{Continent} & %s \\\\\n" % " & ".join(thresholds))
    return cols


def build_temp_line(sum2metric2thresh2name2tuple, sum2metric2thresh2valid, temp):
    numbers = list()
    for s, metric2thresh2name2tuple in sum2metric2thresh2name2tuple.items():
        for m, thresh2name2tuple in metric2thresh2name2tuple.items():
            for t, name2tuple in sorted(thresh2name2tuple.items()):
                valid = name2tuple[temp][1]
                shifted = name2tuple[temp][0]
                total_valid = sum2metric2thresh2valid[s][m][t]
                frac_valid = valid / total_valid
                frac_shifted = shifted / total_valid
                numbers.append(f"{frac_valid:.3f}".lstrip("0"))
                if temp not in TEMPORAL_HAS_SHIFTS:
                    numbers.append("    ")
                else:
                    numbers.append(f"{frac_shifted:.3f}".lstrip("0"))
    return " & ".join(numbers)


def build_cont_line(
    sum2metric2thresh2name2tuple, sum2metric2thresh2cont2valid, temp, cont
):
    numbers = list()
    name = f"{temp}+{cont}"
    for s, metric2thresh2name2tuple in sum2metric2thresh2name2tuple.items():
        for m, thresh2name2tuple in metric2thresh2name2tuple.items():
            for t, name2tuple in sorted(thresh2name2tuple.items()):
                valid = name2tuple[name][1]
                shifted = name2tuple[name][0]
                total_valid = sum2metric2thresh2cont2valid[s][m][t][cont]
                frac_valid = valid / total_valid
                frac_shifted = shifted / total_valid
                numbers.append(f"{frac_valid:.3f}".lstrip("0"))
                if temp not in TEMPORAL_HAS_SHIFTS:
                    numbers.append("    ")
                else:
                    numbers.append(f"{frac_shifted:.3f}".lstrip("0"))
    return " & ".join(numbers)


def load_thresh2name2tuple(path, metric, summarizer, thresh_string, thresh):
    config_prefix = f"{metric}--{summarizer}"
    config_suffix = f"--{thresh_string}-{thresh}"

    re_extract_thresh = re.compile(f"--{thresh_string}-" + r"([.\d]+)")

    thresh2name2tuple = dict()
    for subdir in path.glob(f"{config_prefix}*{config_suffix}"):
        if not subdir.is_dir():
            continue
        assert subdir.name.endswith(config_suffix)
        configstr = subdir.name[: -len(config_suffix)]
        logging.info("processing %s", configstr)

        for cfgdir in path.glob(f"{configstr}--{thresh_string}-*"):
            logging.info("processing %s", cfgdir)
            cthresh = float(re_extract_thresh.search(cfgdir.name).group(1))
            if cthresh not in SUMMARIZER_METRIC_THRESH[summarizer][metric]:
                continue

            with open(cfgdir / "temporal-behavior.pickle", "rb") as fd:
                name2tuple = pickle.load(fd)
                assert isinstance(name2tuple, dict)
                thresh2name2tuple[cthresh] = name2tuple

    return thresh2name2tuple


if __name__ == "__main__":
    sys.exit(main())
