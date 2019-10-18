#!/usr/bin/env python3

import argparse
import logging
import pathlib
import resource
import sys

import fbperf


DIFF_CI_SPECS = [
    {"minrtt50": 10, "hdratio50": 0.1, "hdratioboot": 0.2},
    # {"minrtt50": 20, "hdratio50": 0.2, "hdratioboot": 0.2},
]

summary2metric2thresholds = {
    "deg": {"minrtt50": [5, 10, 20, 50], "hdratio50": [0.05, 0.10, 0.20, 0.50]},
    "opp": {"minrtt50": [5, 10], "hdratio50": [0.05]},
}


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
    return parser


class LaTeX:
    FORMAT_METRIC = {
        "minrtt50": "\\minrttpct (\\cref{sec:units-of-aggregation})",
        "hdratio50": "\\hdratiopct (\\cref{sec:units-of-aggregation})",
    }
    FORMAT_SUMMARY = {
        "deg": "Periods of degraded performance (\\cref{sec:overtime})",
        "opp": "Opportunity for performance-aware routing (\\cref{sec:opportunity})",
    }
    FORMAT_THRESH = {
        "deg": {
            "minrtt50": lambda x: f"$+${int(x)}ms",
            "hdratio50": lambda x: f"$-${x}",
            "hdratioboot": lambda x: f"$-${x}",
        },
        "opp": {
            "minrtt50": lambda x: f"$-${int(x)}ms",
            "hdratio50": lambda x: f"$+${x}",
            "hdratioboot": lambda x: f"$+${x}",
        },
    }

    @staticmethod
    def headers():
        summarizer_headers = list()
        metric_headers = list()
        thresh_headers = list()
        tabular_spec = ""
        columns = 0
        for summary, metric2thresholds in summary2metric2thresholds.items():
            tabular_spec += "|"
            summary_tex = LaTeX.FORMAT_SUMMARY[summary]
            summarizer_columns = 0
            for metric, thresholds in metric2thresholds.items():
                metric_tex = LaTeX.FORMAT_METRIC[metric]
                tabular_spec += "|rr" * len(thresholds)
                for thresh in thresholds:
                    thresh_latex = LaTeX.FORMAT_THRESH[summary][metric](thresh)
                    header = "\\multicolumn{2}{c|}{%s}" % thresh_latex
                    thresh_headers.append(header)
                columns = 2 * len(thresholds)
                summarizer_columns += columns
                header = "\\multicolumn{%d}{c|}{%s}" % (columns, metric_tex)
                metric_headers.append(header)
            header = "\\multicolumn{%d}{c|}{%s}" % (summarizer_columns, summary_tex)
            summarizer_headers.append(header)
        tabular_line = "\\begin{tabular}{r%s}" % tabular_spec
        summarizers_line = "& %s \\\\" % " & ".join(summarizer_headers)
        metrics_line = "\\multicolumn{1}{l|}{\\textsc{Class$/$}} & "
        metrics_line += "%s \\\\" % " & ".join(metric_headers)
        thresh_line = "\\textsc{Continent} & %s \\\\" % " & ".join(thresh_headers)
        return "\n".join([tabular_line, summarizers_line, metrics_line, thresh_line])

    @staticmethod
    def build_pattern_line(sumstr2data, pattern, metric2diffci):
        numbers = list()
        for summary, metric2thresholds in summary2metric2thresholds.items():
            for metric, thresholds in metric2thresholds.items():
                diffci = metric2diffci[metric]
                for thresh in thresholds:
                    sumstr = fbperf.Summarizer.filter_sumstr(
                        sumstr2data, metric, summary, thresh, diffci
                    )
                    data = sumstr2data[sumstr]
                    valid = data.name2data[pattern].valid
                    shifted = data.name2data[pattern].shifted
                    total_valid = data.get_valid()
                    frac_valid = valid / total_valid
                    frac_shifted = shifted / total_valid
                    frac_valid = f"{frac_valid:.3f}".lstrip("0")
                    frac_shifted = f"{frac_shifted:.3f}".lstrip("0")
                    numbers.append("\\cellcolor{bgdblue}" + frac_valid)
                    if pattern in fbperf.TemporalPattern.WITH_SHIFTS:
                        numbers.append("\\cellcolor{bgdoran}" + frac_shifted)
                    else:
                        numbers.append("\\cellcolor{bgdoran}" + "    ")
        return " & ".join(numbers)

    @staticmethod
    def build_continent_line(sumstr2data, pattern, continent, metric2diffci):
        numbers = list()
        name = f"{pattern}+{continent}"
        for summary, metric2thresholds in summary2metric2thresholds.items():
            for metric, thresholds in metric2thresholds.items():
                diffci = metric2diffci[metric]
                for thresh in thresholds:
                    sumstr = fbperf.Summarizer.filter_sumstr(
                        sumstr2data, metric, summary, thresh, diffci
                    )
                    data = sumstr2data[sumstr]
                    valid = data.name2data[name].valid
                    shifted = data.name2data[name].shifted
                    total_valid = data.get_continent_valid(continent)
                    frac_valid = valid / total_valid
                    frac_shifted = shifted / total_valid
                    frac_valid = f"{frac_valid:.3f}".lstrip("0")
                    frac_shifted = f"{frac_shifted:.3f}".lstrip("0")
                    numbers.append("\\cellcolor{bglblue}" + frac_valid)
                    if pattern in fbperf.TemporalPattern.WITH_SHIFTS:
                        numbers.append("\\cellcolor{bgloran}" + frac_shifted)
                    else:
                        numbers.append("\\cellcolor{bgloran}" + "    ")
        return " & ".join(numbers)

    @staticmethod
    def dump_tempconfig_tables(basedir, summarizer2data):
        for metric2diffci in DIFF_CI_SPECS:
            diffci = metric2diffci["minrtt50"]
            fd = open(basedir / f"table--diff-ci-{diffci:0.2f}.tex", "w")
            fd.write(LaTeX.headers())
            fd.write("\n")
            for pattern in fbperf.TemporalPattern.ALL:
                pattern_line = LaTeX.build_pattern_line(
                    summarizer2data, pattern, metric2diffci
                )
                if pattern not in fbperf.TemporalPattern.VALID:
                    fd.write(f"% {pattern: <12} & {pattern_line} \\\\\n")
                else:
                    fd.write("\\hline\n\\cellcolor{bggray}")
                    fd.write(f"{pattern: <12} & {pattern_line} \\\\\n")
                    for continent in fbperf.CLIENT_CONTINENTS:
                        line = LaTeX.build_continent_line(
                            summarizer2data, pattern, continent, metric2diffci
                        )
                        name = f"\\qquad {continent}"
                        fd.write(f"{name: <12} & {line} \\\\\n")
            fd.close()


def main():
    resource.setrlimit(resource.RLIMIT_AS, (1 << 31, 1 << 31))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 31, 1 << 31))
    logging.basicConfig(format="%(message)s", level=logging.NOTSET)

    parser = create_parser()
    opts = parser.parse_args()

    for tempdir in opts.basepath.glob("tempconfig*"):
        sumstr2data = dict()
        for sumdir in tempdir.glob("*"):
            if not sumdir.is_dir():
                continue
            logging.info("loading %s", sumdir)
            sumstr2data[sumdir.name] = fbperf.RunData(sumdir)

        LaTeX.dump_tempconfig_tables(tempdir, sumstr2data)


if __name__ == "__main__":
    sys.exit(main())
